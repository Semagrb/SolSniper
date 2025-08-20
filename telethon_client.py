import os
import logging
import re
import json
import time
import hashlib
from datetime import datetime, timezone
from telethon import TelegramClient, events, Button
from dotenv import load_dotenv

load_dotenv()

# Accept multiple .env key variants for compatibility
API_ID = os.getenv('TELETHON_API_ID') or os.getenv('API_ID')
API_HASH = os.getenv('TELETHON_API_HASH') or os.getenv('API_HASH')
SESSION_NAME = 'solana_bot_session'
BOT_TOKEN = (
    os.getenv('TELEGRAM_BOT_TOKEN')
    or os.getenv('TELEGRAM_TOKEN')
    or os.getenv('BOT_TOKEN')
)
POST_ORDER_LIMIT_IN_GROUPS = (os.getenv('POST_ORDER_LIMIT_IN_GROUPS', 'true').strip().lower() in ('1','true','yes','on'))
_ALLOWED_IDS_ENV = (
    os.getenv('TELEGRAM_BOT_ALLOWED_IDS')
    or os.getenv('ALLOWED_USER_IDS')
    or ''
).strip()
ALLOWED_USER_IDS: set[int] = set(
    int(x) for x in re.split(r'[;,\s]+', _ALLOWED_IDS_ENV) if x.isdigit()
)

client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
bot_client: TelegramClient | None = None

PERSIST_PATH = 'strategies.json'
DEDUP_TTL_SECONDS = 6 * 60 * 60  # 6 hours
_processed_cache: dict[str, float] = {}
PROCESSING_PAUSED: bool = False
SELF_ID: int | None = None
CONV_STATE: dict[int, dict] = {}
PASS_CTX: dict[str, dict] = {}
BOT_CONTROL_CHAT_ID: int | None = None

def _belongs_to(strategy: dict, owner_id: int | None) -> bool:
    """Strategy belongs to this owner if IDs match or if legacy has no owner."""
    try:
        sid = strategy.get('owner_id')
        return (sid is None) or (owner_id is not None and int(sid) == int(owner_id))
    except Exception:
        return True

def extract_token_address(message_text):
    """Extract Solana token address from message text"""
    # Look for typical Solana address patterns (base58, 32-44 chars)
    patterns = [
        r'[A-Za-z0-9]{32,44}',  # General Solana address pattern
        r'CA:\s*([A-Za-z0-9]{32,44})',  # Contract Address format
        r'Token:\s*([A-Za-z0-9]{32,44})',  # Token format
    ]
    
    for pattern in patterns:
        match = re.search(pattern, message_text)
        if match:
            return match.group(1) if '(' in pattern else match.group(0)
    return None


def _parse_number(s: str):
    """Parse a number string that may use comma as decimal separator."""
    if s is None:
        return None
    try:
        # Normalize to string and trim
        s = str(s).strip()
        if not s:
            return None
        # Remove spaces (e.g., '1 000,50')
        s = s.replace(' ', '')
        # If there are commas but no dots, treat comma as decimal separator
        if s.count(',') > 0 and s.count('.') == 0:
            s = s.replace(',', '.')
        # Remove any non numeric/decimal/sign characters (keeps + - . and digits)
        s = re.sub(r"[^0-9.+-]", "", s)
        # Prevent strings that are only signs or dots
        if s in ("", "+", "-", ".", "+.", "-."):
            return None
        return float(s)
    except Exception:
        return None


def _parse_duration_to_minutes(text: str):
    """Parse durations like '45s', '30m', '2h', '1d', or combos '1h30m', '1d 2h 5m 10s'.
    Bare numbers default to minutes. Returns float minutes or None if invalid.
    """
    if text is None:
        return None
    t = text.strip().lower()
    if not t:
        return None
    # If purely numeric, default to minutes
    if re.fullmatch(r"\d+(?:\.\d+)?", t):
        try:
            return float(t)
        except Exception:
            return None
    total = 0.0
    matched = False
    for m in re.finditer(r"(\d+(?:\.\d+)?)\s*([smhd])", t):
        matched = True
        v = float(m.group(1))
        u = m.group(2)
        if u == 's':
            total += v / 60.0
        elif u == 'm':
            total += v
        elif u == 'h':
            total += v * 60.0
        elif u == 'd':
            total += v * 1440.0
    if matched and total > 0:
        return total
    return None


def _fmt_minutes_human(minutes_val) -> str:
    """Format minutes (float) into a short human string like '1h 30m', '45s', '2d 3h'."""
    try:
        total_seconds = int(round(float(minutes_val) * 60))
    except Exception:
        return 'Not set'
    if total_seconds <= 0:
        return 'Not set'
    d, rem = divmod(total_seconds, 86400)
    h, rem = divmod(rem, 3600)
    m, s = divmod(rem, 60)
    parts = []
    if d:
        parts.append(f"{d}d")
    if h:
        parts.append(f"{h}h")
    if m:
        parts.append(f"{m}m")
    if s and not parts:
        parts.append(f"{s}s")
    return ' '.join(parts) if parts else '0m'


def parse_message_fields(text: str):
    """Extract relevant fields from the message text.
    Returns dict with keys: first_buy_pct (float|None), balance_sol (float|None), tx_count (int|None), label (str|None)
    """
    if not text:
        return {
            'first_buy_pct': None,
            'balance_sol': None,
            'tx_count': None,
            'label': None,
        }

    t = text
    lower = t.lower()

    # Label detection (exact phrases as per GUI)
    label = None
    if 'dev has enough money' in lower:
        label = 'Dev Has Enough Money'
    elif 'dev wallet empty' in lower:
        label = 'Dev Wallet Empty'

    # First Buy %
    first_buy_pct = None
    patterns_pct = [
        r'first\s*buy[^\n%\d]*([\d.,]+)\s*%',
        r'first\s*buy\s*%[^\d]*([\d.,]+)',
        r'first\s*purchase[^\n%\d]*([\d.,]+)\s*%',
        r'first\s*buyers?[^\n%\d]*([\d.,]+)\s*%'
    ]
    for pat in patterns_pct:
        m = re.search(pat, t, flags=re.IGNORECASE)
        if m:
            first_buy_pct = _parse_number(m.group(1))
            break
    if first_buy_pct is None:
        # generic: any number followed by % on the line containing 'first'
        for line in t.splitlines():
            if 'first' in line.lower():
                m2 = re.search(r'([\d.,]+)\s*%', line)
                if m2:
                    first_buy_pct = _parse_number(m2.group(1))
                    break

    # Balance (SOL)
    balance_sol = None
    patterns_bal = [
        r'(?:balance|sol\s*balance|wallet\s*balance)[^\n\d]*([\d.,]+)\s*sol',
        r'([\d.,]+)\s*sol\b'
    ]
    for pat in patterns_bal:
        m = re.search(pat, t, flags=re.IGNORECASE)
        if m:
            balance_sol = _parse_number(m.group(1))
            break

    # Transactions (count)
    tx_count = None
    patterns_tx = [
        r'(?:transactions|txs?|tx\b)[^\n\d]*([\d,\.]+)\b'
    ]
    for pat in patterns_tx:
        m = re.search(pat, t, flags=re.IGNORECASE)
        if m:
            num = _parse_number(m.group(1))
            if num is not None:
                try:
                    tx_count = int(round(num))
                except Exception:
                    tx_count = None
            break

    return {
        'first_buy_pct': first_buy_pct,
        'balance_sol': balance_sol,
        'tx_count': tx_count,
        'label': label,
    }

def load_strategies():
    try:
        if not os.path.exists(PERSIST_PATH):
            return []
        with open(PERSIST_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except Exception as e:
        logging.error(f"Failed to load strategies: {e}")
        return []


def save_strategies(strategies: list) -> bool:
    """Persist strategies back to JSON. Returns True on success."""
    try:
        with open(PERSIST_PATH, 'w', encoding='utf-8') as f:
            json.dump(strategies, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        logging.error(f"Failed to save strategies: {e}")
        return False

def get_age_minutes(message_date):
    """Compute age in minutes from Telegram message timestamp to now (UTC)."""
    now = datetime.now(timezone.utc)
    # Telethon's message_date is usually aware UTC; ensure timezone-aware
    msg_dt = message_date if message_date.tzinfo else message_date.replace(tzinfo=timezone.utc)
    return max(0.0, (now - msg_dt).total_seconds() / 60.0)

def token_age_passes(filters_dict, age_minutes):
    """Return True if Token Age filter passes; if absent, treat as pass (only check enabled filters)."""
    ta = filters_dict.get('Token Age (minutes)')
    if isinstance(ta, dict) and 'from' in ta and 'to' in ta:
        low = float(ta['from'])
        high = float(ta['to'])
        return (low <= age_minutes <= high)
    return True


def range_filter_passes(rng: dict, value: float | int | None) -> bool:
    """Generic range check. If value is None, fail. Expects {'from': x, 'to': y}."""
    if not isinstance(rng, dict) or 'from' not in rng or 'to' not in rng:
        return True
    if value is None:
        return False
    try:
        low = float(rng['from'])
        high = float(rng['to'])
        return low <= float(value) <= high
    except Exception:
        return False


def label_filter_passes(expected: str | None, detected: str | None) -> bool:
    if not expected or expected == 'Any':
        return True
    return (expected == detected)


def _safe_format(template: str, ctx: dict) -> str:
    class _D(dict):
        def __missing__(self, k):
            return ''
    return template.format_map(_D(ctx or {}))


async def _send_action_message(target: str, message: str):
    # Accept '@username' or 'username'
    chat = target.strip()
    if chat.startswith('@'):
        chat = chat[1:]
    try:
        await client.send_message(chat, message)
        logging.info(f"📤 Sent to @{chat}: {message}")
    except Exception as e:
        logging.error(f"Failed to send to {target}: {e}")

async def _send_limit_to_trojan(token: str, amount: float, expiry: float, slippage: float, trigger: float):
    """Send a LIMIT order command to @solana_trojanbot. Assumes you've started it before."""
    target = 'solana_trojanbot'
    cmd = f"/limit token={token} amount={amount} slippage={slippage} trigger={trigger} expiry={int(expiry)}"
    try:
        if bot_client:
            await bot_client.send_message(target, cmd)
        else:
            await client.send_message(target, cmd)
        logging.info(f"📤 LIMIT sent to @{target}: {cmd}")
    except Exception as e:
        logging.error(f"Failed to send LIMIT to @{target}: {e}")


def set_paused(value: bool):
    global PROCESSING_PAUSED
    PROCESSING_PAUSED = value


def is_paused() -> bool:
    return PROCESSING_PAUSED


def _authorized(sender_id: int | None) -> bool:
    if sender_id is None:
        return False
    if not ALLOWED_USER_IDS:
        # If not configured, allow private users (caller must keep bot private)
        return True
    return int(sender_id) in ALLOWED_USER_IDS

def _get_control_chat_id() -> int | None:
    if ALLOWED_USER_IDS:
        try:
            return next(iter(ALLOWED_USER_IDS))
        except StopIteration:
            return None
    return BOT_CONTROL_CHAT_ID

def _store_pass_ctx(data: dict) -> str:
    import secrets
    cid = secrets.token_hex(4)
    PASS_CTX[cid] = data
    return cid

async def process_token_message(event, group_username):
    """Process incoming message: parse fields and evaluate all enabled filters."""
    global PROCESSING_PAUSED
    if PROCESSING_PAUSED:
        logging.debug("Processing paused; skipping group message")
        return
    msg_text = event.message.message or ''

    # Compute message age first
    age = get_age_minutes(event.message.date)
    logging.info(f"🕒 Message age in @{group_username}: {age:.2f} min")

    # Parse message for fields
    parsed = parse_message_fields(msg_text)
    logging.debug(f"Parsed fields: {parsed}")

    # Load strategies created via GUI
    strategies = load_strategies()

    # Map username to GUI group format (with leading @)
    gui_group = f"@{group_username}"

    # Evaluate strategies for this group that are enabled
    for idx, strat in enumerate(strategies, start=1):
        try:
            if not strat.get('enabled', True):
                continue
            if strat.get('group') != gui_group:
                continue
            filters_dict = strat.get('filters', {})
            # Normalize legacy keys to current schema
            if 'First Buy (%)' not in filters_dict and 'First Buy (count)' in filters_dict:
                filters_dict['First Buy (%)'] = filters_dict.get('First Buy (count)')
            if 'Label' not in filters_dict and 'Mention' in filters_dict:
                filters_dict['Label'] = filters_dict.get('Mention')

            # Check all filters present in this strategy
            if not token_age_passes(filters_dict, age):
                logging.debug(f"❌ {strat.get('name')} fail: Token Age")
                continue

            fb_rng = filters_dict.get('First Buy (%)')
            if not range_filter_passes(fb_rng, parsed['first_buy_pct']):
                logging.debug(f"❌ {strat.get('name')} fail: First Buy %")
                continue

            bal_rng = filters_dict.get('Balance (SOL)')
            if not range_filter_passes(bal_rng, parsed['balance_sol']):
                logging.debug(f"❌ {strat.get('name')} fail: Balance (SOL)")
                continue

            tx_rng = filters_dict.get('Transactions (count)')
            if not range_filter_passes(tx_rng, parsed['tx_count']):
                logging.debug(f"❌ {strat.get('name')} fail: Transactions")
                continue

            label_expect = filters_dict.get('Label')
            if not label_filter_passes(label_expect, parsed['label']):
                logging.debug(f"❌ {strat.get('name')} fail: Label")
                continue

            # Optional: extract token address (for future actions)
            token_address = extract_token_address(msg_text)
            logging.info(
                f"✅ Strategy '{strat.get('name')}' PASSED (age={age:.2f} min, fb%={parsed['first_buy_pct']}, bal={parsed['balance_sol']}, tx={parsed['tx_count']}, label={parsed['label']})"
                + (f" | Token: {token_address}" if token_address else "")
            )
            # Notify strategy owner with inline actions
            if bot_client and token_address:
                ctx = {
                    'token': token_address,
                    'age': age,
                    'parsed': parsed,
                    'strat_index': idx,
                    'strat_name': strat.get('name',''),
                }
                cid = _store_pass_ctx(ctx)
                chat_id = strat.get('owner_id') or _get_control_chat_id()
                if chat_id:
                    # Include a Dexscreener link to render a rich preview card with token image
                    ds_link = f"https://dexscreener.com/solana/{token_address}"
                    text = (
                        f"🎯 Match: {strat.get('name')}\n"
                        f"Token: {token_address}\n"
                        f"Age: {age:.2f}m | FB%: {parsed['first_buy_pct']} | Bal: {parsed['balance_sol']} | Tx: {parsed['tx_count']} | Label: {parsed['label']}\n\n"
                        f"🔗 {ds_link}"
                    )
                    buttons = [
                        [Button.inline('📈 Order LIMIT', data=f'limit:{cid}')],
                    ]
                    try:
                        await bot_client.send_message(chat_id, text, buttons=buttons)
                    except Exception as _e:
                        logging.warning(f"Notify failed: {_e}")
                    # Optionally also post into the source group so the button is available there
                    if POST_ORDER_LIMIT_IN_GROUPS:
                        try:
                            await bot_client.send_message(event.chat_id, text, buttons=buttons)
                        except Exception as _e2:
                            logging.debug(f"Group notify skipped: {_e2}")
            # Execute action if configured
            action = strat.get('action') or {}
            target = action.get('target')
            template = action.get('template')
            if target and template:
                ctx = {
                    'token': token_address or '',
                    'age': f"{age:.2f}",
                    'first_buy_pct': parsed['first_buy_pct'],
                    'balance_sol': parsed['balance_sol'],
                    'tx_count': parsed['tx_count'],
                    'label': parsed['label'] or '',
                    'name': strat.get('name', ''),
                }
                msg = _safe_format(template, ctx)
                await _send_action_message(target, msg)
        except Exception as e:
            logging.error(f"Error evaluating strategy '{strat.get('name')}': {e}")

async def start_telethon():
    if not API_ID or not API_HASH:
        raise RuntimeError("Missing TELETHON_API_ID or TELETHON_API_HASH in .env")
    await client.start()
    global SELF_ID, bot_client
    me = await client.get_me()
    SELF_ID = getattr(me, 'id', None)
    logging.info('🚀 Telethon client started - monitoring groups...')

    # Optionally start Bot API client (via Telethon) if token provided
    if BOT_TOKEN:
        bot_client = TelegramClient('solsniper_bot_session', API_ID, API_HASH)
        await bot_client.start(bot_token=BOT_TOKEN)
        logging.info('🤖 Bot client started')

        # ---------- Helpers ----------
        def _status_text():
            strategies = load_strategies()
            enabled = sum(1 for s in strategies if s.get('enabled', True))
            total = len(strategies)
            cache_size = len(_processed_cache)
            return (
                "🚀 SolSniper Dashboard\n"
                f"• Paused: {PROCESSING_PAUSED}\n"
                f"• Self ID: {SELF_ID}\n"
                f"• Strategies: {enabled}/{total} enabled\n"
                f"• Dedup cache: {cache_size} entries"
            )

        def _strategies_page(owner_id: int, page: int = 1, page_size: int = 6):
            items = [s for s in load_strategies() if _belongs_to(s, owner_id)]
            total = len(items)
            start = (page - 1) * page_size
            end = start + page_size
            page_items = items[start:end]
            rows = []
            for i, s in enumerate(page_items, start=start + 1):
                flag = '✅' if s.get('enabled', True) else '⏸️'
                rows.append([Button.inline(f"#{i} {flag} {s.get('name','(unnamed)')}", data=f"view:{i}")])
            nav = []
            if start > 0:
                nav.append(Button.inline('⬅️ Prev', data=f"page:{page-1}"))
            if end < total:
                nav.append(Button.inline('Next ➡️', data=f"page:{page+1}"))
            if nav:
                rows.append(nav)
            # Controls within My Strategies
            if is_paused():
                rows.append([Button.inline('▶ Resume', data='resume'), Button.inline('🔁 Reload', data='reload')])
            else:
                rows.append([Button.inline('⏸ Pause', data='pause'), Button.inline('🔁 Reload', data='reload')])
            rows.append([
                Button.inline('🔄 Refresh', data='strats'), Button.inline('⬅️ Back', data='dash'),
            ])
            text = f"📁 My Strategies – Page {page}"
            return text, rows

        async def _send_dashboard(chat_id):
            await bot_client.send_message(
                chat_id,
                _status_text(),
                buttons=[
                    [Button.inline('📊 STATUS', data='stat')],
                    [Button.inline('🟢 ON', data='resume')],
                    [Button.inline('🔴 OFF', data='pause')],
                    [Button.inline('📁 MY STRATEGIES', data='strats')],
                    [Button.inline('➕ NEW STRATEGY', data='new')],
                ],
            )

        def _fmt_filters_human(filters: dict) -> str:
            if not isinstance(filters, dict):
                return 'None'
            def rng(k):
                v = filters.get(k)
                if isinstance(v, dict) and 'from' in v and 'to' in v:
                    return f"{v['from']} → {v['to']}"
                return 'Not set'
            lines = []
            lines.append(f"- Token Age (minutes): {rng('Token Age (minutes)')}")
            lines.append(f"- First Buy (%): {rng('First Buy (%)')}")
            # Backward compat
            if 'First Buy (count)' in filters and 'First Buy (%)' not in filters:
                v = filters.get('First Buy (count)')
                if isinstance(v, dict) and 'from' in v and 'to' in v:
                    lines[-1] = f"- First Buy (%): {v['from']} → {v['to']}"
            lines.append(f"- Balance (SOL): {rng('Balance (SOL)')}")
            lines.append(f"- Transactions (count): {rng('Transactions (count)')}")
            label = filters.get('Label') or filters.get('Mention') or 'Any'
            lines.append(f"- Label: {label}")
            # Include any extra keys not covered above
            known = {'Token Age (minutes)','First Buy (%)','First Buy (count)','Balance (SOL)','Transactions (count)','Label','Mention'}
            for k, v in filters.items():
                if k in known:
                    continue
                if isinstance(v, dict) and 'from' in v and 'to' in v:
                    lines.append(f"- {k}: {v['from']} → {v['to']}")
                else:
                    lines.append(f"- {k}: {v}")
            return "\n".join(lines)

        def _count_filled_filters(filters: dict) -> int:
            """Count how many filter options are meaningfully set (ranges or label not 'Any')."""
            if not isinstance(filters, dict):
                return 0
            cnt = 0
            for k, v in filters.items():
                if isinstance(v, dict) and 'from' in v and 'to' in v and v['from'] is not None and v['to'] is not None:
                    cnt += 1
                elif k in ('Label', 'Mention'):
                    if v and str(v).strip() and str(v).strip() != 'Any':
                        cnt += 1
                else:
                    if v not in (None, '', 'Any'):
                        cnt += 1
            return cnt

        def _fmt_strategy_human(idx: int, s: dict) -> str:
            name = s.get('name','(unnamed)')
            group = s.get('group','?')
            enabled = '✅ Enabled' if s.get('enabled', True) else '⏸️ Disabled'
            filters = _fmt_filters_human(s.get('filters', {}))
            action = s.get('action') or {}
            trojan = s.get('trojan') or {}
            action_text = (
                f"- Target: {action.get('target','Not set')}\n"
                f"- Template: {action.get('template','Not set')}"
            ) if action else 'None'
            trojan_text = (
                f"- Amount (SOL): {trojan.get('amount','Not set')}\n"
                f"- Expiry: {_fmt_minutes_human(trojan.get('expiry_minutes'))}\n"
                f"- Slippage (%): {trojan.get('slippage_pct','Not set')}\n"
                f"- Trigger (SOL): {trojan.get('trigger_price','Not set')}"
            ) if trojan else 'None'
            return (
                f"Strategy #{idx}\n"
                f"Name: {name}\n"
                f"Group: {group}\n"
                f"Status: {enabled}\n\n"
                f"Filters:\n{filters}\n\n"
                f"Action:\n{action_text}\n\n"
                f"Trojan (for @solana_trojanbot):\n{trojan_text}"
            )

        def _get_owned_strategy(owner_id: int, index: int):
            all_items = load_strategies()
            owned = [s for s in all_items if _belongs_to(s, owner_id)]
            if 1 <= index <= len(owned):
                return all_items, owned[index - 1]
            return all_items, None

        def _parse_range(text: str):
            t = (text or '').strip().lower()
            if t in ('', 'skip', 'none'):
                return None
            # Accept single value or min,max with flexible separators (comma, dash, whitespace)
            # Normalize en-dash/em-dash to hyphen
            t = t.replace('–', '-').replace('—', '-')
            # Try min,max first (comma or dash or 'to')
            # 1) Comma separated
            parts = re.split(r'\s*,\s*', t)
            if len(parts) == 2:
                a = _parse_number(parts[0])
                b = _parse_number(parts[1])
            else:
                # 2) Hyphen or 'to' separated
                parts2 = re.split(r'\s*(?:-|to)\s*', t)
                if len(parts2) == 2:
                    a = _parse_number(parts2[0])
                    b = _parse_number(parts2[1])
                else:
                    # 3) Whitespace separated two numbers
                    parts3 = re.findall(r'[+-]?[\d.,]+', t)
                    if len(parts3) == 2:
                        a = _parse_number(parts3[0])
                        b = _parse_number(parts3[1])
                    else:
                        # 4) Single value -> use as both min and max
                        a = _parse_number(t)
                        b = a
            if a is None or b is None:
                return None
            lo, hi = (a, b) if a <= b else (b, a)
            return {'from': lo, 'to': hi}

        def _fmt_range(r):
            if not isinstance(r, dict) or 'from' not in r or 'to' not in r:
                return 'Not set'
            return f"{r['from']} to {r['to']}"

        def _builder_text(st):
            # Small header only; details are shown on buttons
            return "Strategy Builder"

        def _label_menu_text(st):
            d = st.get('data', {})
            f = d.get('filters', {})
            cur = f.get('Label', 'Any')
            return (
                "🏷️ Label\n"
                f"Current: {cur}\n\n"
                "Choose:")

        def _label_menu_buttons():
            return [
                [Button.inline('Any', data='label:Any')],
                [Button.inline('Has Enough Money', data='label:Dev Has Enough Money')],
                [Button.inline('Wallet Empty', data='label:Dev Wallet Empty')],
                [Button.inline('⬅️ Back', data='builder')],
            ]

        def _order_menu_text(st):
            d = st.get('data', {})
            t = d.get('trojan', {})
            return (
                "⚙️ Order LIMIT\n"
                f"Amount (SOL): {t.get('amount','Not set')}\n"
                f"Expiry: {_fmt_minutes_human(t.get('expiry_minutes'))}\n"
                f"Slippage (%): {t.get('slippage_pct','Not set')}\n"
                f"Trigger (SOL): {t.get('trigger_price','Not set')}\n\n"
                "Quick picks or edit:")

        def _order_menu_buttons():
            return [
                [Button.inline('Amount 0.5', data='set:amount:0.5'), Button.inline('1', data='set:amount:1'), Button.inline('2', data='set:amount:2')],
                [Button.inline('Expiry 10m', data='set:expiry:10m'), Button.inline('30m', data='set:expiry:30m'), Button.inline('1h', data='set:expiry:1h')],
                [Button.inline('Slippage 1%', data='set:slippage:1'), Button.inline('2%', data='set:slippage:2'), Button.inline('5%', data='set:slippage:5')],
                [Button.inline('Trigger 0.1', data='set:trigger:0.1'), Button.inline('0.5', data='set:trigger:0.5'), Button.inline('1', data='set:trigger:1')],
                [Button.inline('✏️ Amount', data='edit:amount'), Button.inline('✏️ Expiry', data='edit:expiry')],
                [Button.inline('✏️ Slippage', data='edit:slippage'), Button.inline('✏️ Trigger', data='edit:trigger')],
                [Button.inline('⬅️ Back', data='builder')],
            ]

        def _builder_buttons(st):
            d = st.get('data', {})
            group = d.get('group')
            rows = []
            # Top: Strategy name and group
            name = d.get('name') or '(set name)'
            rows.append([Button.inline(f"📛 Strategy: {name}", data='edit:name')])
            group_label = group or '(choose group)'
            rows.append([Button.inline(f"👥 Group: {group_label}", data='change_group')])
            if group == '@solana_trojanbot':
                t = d.get('trojan', {})
                rows += [[Button.inline('⚙️ LIMIT – Quick Picks', data='menu:order')]]
                rows += [[Button.inline(f"Amt: {t.get('amount','Not set')}", data='edit:amount')]]
                rows += [[Button.inline(f"Exp: {_fmt_minutes_human(t.get('expiry_minutes'))}", data='edit:expiry')]]
                rows += [[Button.inline(f"Slip: {t.get('slippage_pct','Not set')}%", data='edit:slippage')]]
                rows += [[Button.inline(f"Trig: {t.get('trigger_price','Not set')}", data='edit:trigger')]]
            else:
                f = d.setdefault('filters', {})
                rows += [
                    [Button.inline(f"Token Age (min): {_fmt_range(f.get('Token Age (minutes)'))}", data='edit:token_age')],
                    [Button.inline(f"First Buy (%): {_fmt_range(f.get('First Buy (%)'))}", data='edit:first_buy')],
                    [Button.inline(f"Balance (SOL): {_fmt_range(f.get('Balance (SOL)'))}", data='edit:balance')],
                    [Button.inline(f"Transactions: {_fmt_range(f.get('Transactions (count)'))}", data='edit:tx')],
                ]
                label = f.get('Label', 'Any')
                rows.append([Button.inline(f"🏷️ Label: {label}", data='menu:label')])
                # LIMIT summary controls (compact)
                t = d.get('trojan', {})
                rows += [[Button.inline('⚙️ LIMIT – Quick Picks', data='menu:order')]]
                rows += [[Button.inline(f"Amt: {t.get('amount','Not set')}", data='edit:amount')]]
                rows += [[Button.inline(f"Exp: {_fmt_minutes_human(t.get('expiry_minutes'))}", data='edit:expiry')]]
                rows += [[Button.inline(f"Slip: {t.get('slippage_pct','Not set')}%", data='edit:slippage')]]
                rows += [[Button.inline(f"Trig: {t.get('trigger_price','Not set')}", data='edit:trigger')]]
            rows.append([Button.inline('✅ Save', data='save'), Button.inline('❌ Cancel', data='cancel')])
            rows.append([Button.inline('⬅️ Back', data='dash')])
            return rows

        # ---------- Commands ----------

        @bot_client.on(events.NewMessage(pattern=r'^/start$'))
        async def bot_start(event):
            if not event.is_private or not _authorized(event.sender_id):
                return
            global BOT_CONTROL_CHAT_ID
            BOT_CONTROL_CHAT_ID = event.chat_id
            # Clear any stale conversation state so /start shows only the dashboard
            CONV_STATE.pop(event.chat_id, None)
            await _send_dashboard(event.chat_id)

        @bot_client.on(events.NewMessage(pattern=r'^/(help|h)$'))
        async def bot_help(event):
            if not event.is_private or not _authorized(event.sender_id):
                return
            await event.respond(
                "Commands:\n"
                "/help – this help\n"
                "/status – show bot status\n"
                "/strategies – list strategies with index\n"
                "/enable <#idx|name> – enable a strategy\n"
                "/disable <#idx|name> – disable a strategy\n"
                "/pause – pause processing group messages\n"
                "/resume – resume processing\n"
                "/reload – reload strategies.json (ack)\n"
                "/ping – health check\n"
                "/whoami – show your Telegram ID\n"
                "/dashboard – open inline dashboard\n"
                "/new – start new strategy wizard"
            )

        @bot_client.on(events.NewMessage(pattern=r'^/ping'))
        async def bot_ping(event):
            if not event.is_private or not _authorized(event.sender_id):
                return
            await event.respond('pong')

        @bot_client.on(events.NewMessage(pattern=r'^/(menu|dashboard)$'))
        async def bot_dashboard(event):
            if not event.is_private or not _authorized(event.sender_id):
                return
            global BOT_CONTROL_CHAT_ID
            BOT_CONTROL_CHAT_ID = event.chat_id
            await _send_dashboard(event.chat_id)

        @bot_client.on(events.NewMessage(pattern=r'^/whoami'))
        async def bot_whoami(event):
            if not event.is_private or not _authorized(event.sender_id):
                return
            await event.respond(f"Your ID: {event.sender_id}")

        @bot_client.on(events.NewMessage(pattern=r'^/status'))
        async def bot_status(event):
            if not event.is_private or not _authorized(event.sender_id):
                return
            strategies = load_strategies()
            enabled = sum(1 for s in strategies if s.get('enabled', True))
            total = len(strategies)
            cache_size = len(_processed_cache)
            await event.respond(
                f"Status:\nPaused: {PROCESSING_PAUSED}\n"
                f"Self ID: {SELF_ID}\n"
                f"Strategies: {enabled}/{total} enabled\n"
                f"Dedup cache size: {cache_size}"
            )

        @bot_client.on(events.NewMessage(pattern=r'^/strategies'))
        async def bot_strategies(event):
            if not event.is_private or not _authorized(event.sender_id):
                return
            strategies = [s for s in load_strategies() if _belongs_to(s, event.sender_id)]
            if not strategies:
                await event.respond('No strategies found.')
                return
            lines = []
            for i, s in enumerate(strategies, start=1):
                flag = '✅' if s.get('enabled', True) else '⏸️'
                lines.append(f"#{i} {flag} {s.get('name','(unnamed)')} — {s.get('group','?')}")
            await event.respond("\n".join(lines))

        @bot_client.on(events.NewMessage(pattern=r'^/new$'))
        async def bot_new(event):
            if not event.is_private or not _authorized(event.sender_id):
                return
            CONV_STATE[event.chat_id] = {'mode': 'create', 'step': 'group', 'data': {}}
            await event.respond(
                'Choose group for the new strategy:',
                buttons=[
                    [Button.inline('@SolanaNewPumpfun', data='new_group:@SolanaNewPumpfun')],
                    [Button.inline('@solana_trojanbot', data='new_group:@solana_trojanbot')],
                    [Button.inline('❌ Cancel', data='cancel')],
                ]
            )

        async def _toggle_enable(event, enable: bool, arg: str):
            all_items = load_strategies()
            strategies = [s for s in all_items if _belongs_to(s, event.sender_id)]
            # Match by index (#n) or name
            if not arg:
                await event.respond('Provide a name or #index')
                return
            s = None
            if arg.startswith('#') and arg[1:].isdigit():
                idx = int(arg[1:])
                if 1 <= idx <= len(strategies):
                    s = strategies[idx-1]
            else:
                low = arg.lower()
                for it in strategies:
                    if str(it.get('name','')).lower() == low:
                        s = it
                        break
            if not s:
                await event.respond('Strategy not found')
                return
            s['enabled'] = enable
            if save_strategies(all_items):
                await event.respond(('Enabled' if enable else 'Disabled') + f": {s.get('name')}")
            else:
                await event.respond('Failed to save strategies.json')

        @bot_client.on(events.NewMessage(pattern=r'^/enable(?:\s+(.+))?'))
        async def bot_enable(event):
            if not event.is_private or not _authorized(event.sender_id):
                return
            arg = (event.pattern_match.group(1) or '').strip()
            await _toggle_enable(event, True, arg)

        @bot_client.on(events.NewMessage(pattern=r'^/disable(?:\s+(.+))?'))
        async def bot_disable(event):
            if not event.is_private or not _authorized(event.sender_id):
                return
            arg = (event.pattern_match.group(1) or '').strip()
            await _toggle_enable(event, False, arg)

        @bot_client.on(events.NewMessage(pattern=r'^/pause'))
        async def bot_pause(event):
            if not event.is_private or not _authorized(event.sender_id):
                return
            set_paused(True)
            await event.respond('Processing paused')

        @bot_client.on(events.NewMessage(pattern=r'^/resume'))
        async def bot_resume(event):
            if not event.is_private or not _authorized(event.sender_id):
                return
            set_paused(False)
            await event.respond('Processing resumed')

        @bot_client.on(events.NewMessage(pattern=r'^/reload'))
        async def bot_reload(event):
            if not event.is_private or not _authorized(event.sender_id):
                return
            _ = load_strategies()
            await event.respond('Reloaded strategies.json')

        # ---------- Inline button callbacks ----------
        @bot_client.on(events.CallbackQuery)
        async def on_button(event):
            # Only allow in private chat and for authorized users
            if not event.is_private or not _authorized(event.sender_id):
                try:
                    await event.answer('Not allowed', alert=False)
                except Exception:
                    pass
                return
            data = event.data.decode('utf-8') if isinstance(event.data, (bytes, bytearray)) else str(event.data)
            chat_id = event.chat_id
            try:
                # Ignore spacer clicks
                if data == 'noop':
                    await event.answer()
                    return
                # Dashboard
                if data in ('dash', 'menu'):
                    await event.edit(_status_text(), buttons=[
                        [Button.inline('📊 STATUS', data='stat')],
                        [Button.inline('🟢 ON', data='resume')],
                        [Button.inline('🔴 OFF', data='pause')],
                        [Button.inline('📁 MY STRATEGIES', data='strats')],
                        [Button.inline('➕ NEW STRATEGY', data='new')],
                    ])
                    return
                if data == 'stat':
                    await event.edit(_status_text(), buttons=[
                        [Button.inline('🟢 ON', data='resume')],
                        [Button.inline('🔴 OFF', data='pause')],
                        [Button.inline('🔄 Refresh', data='stat')],
                        [Button.inline('⬅️ Back', data='dash')]
                    ])
                    return
                if data == 'change_group':
                    st = CONV_STATE.get(chat_id) or {'mode': 'create', 'step': 'group', 'data': {}}
                    CONV_STATE[chat_id] = st
                    # Edit the current builder message to show group choices
                    await event.edit(
                        'Choose group:',
                        buttons=[
                            [Button.inline('@SolanaNewPumpfun', data='new_group:@SolanaNewPumpfun')],
                            [Button.inline('@solana_trojanbot', data='new_group:@solana_trojanbot')],
                            [Button.inline('⬅️ Back', data='builder')],
                        ]
                    )
                    return
                if data == 'edit:name':
                    st = CONV_STATE.get(chat_id)
                    if not st:
                        st = {'mode': 'create', 'step': 'edit_name', 'data': {}}
                        CONV_STATE[chat_id] = st
                    st['step'] = 'edit_name'
                    await event.edit('Enter strategy name:', buttons=[[Button.inline('⬅️ Back', data='builder')]])
                    return
                if data.startswith('strats') or data.startswith('page:'):
                    page = 1
                    if data.startswith('page:'):
                        try:
                            page = int(data.split(':', 1)[1])
                        except Exception:
                            page = 1
                    text, rows = _strategies_page(event.sender_id, page=page)
                    await event.edit(text, buttons=rows)
                    return
                if data.startswith('view:'):
                    # Show human-readable details for owned strategy index
                    try:
                        idx = int(data.split(':', 1)[1])
                    except Exception:
                        idx = 1
                    all_items, s = _get_owned_strategy(event.sender_id, idx)
                    if not s:
                        await event.answer('Not found')
                        return
                    enabled = s.get('enabled', True)
                    detail = _fmt_strategy_human(idx, s)
                    # One control per row to keep buttons large and always visible
                    rows = [
                        [Button.inline('✏️ Edit', data=f'edit_strategy:{idx}')],
                        [Button.inline('✅ Enable' if not enabled else '⏸ Disable', data=f'toggle:{idx}')],
                        [Button.inline('🗑 Delete', data=f'delete:{idx}')],
                        [Button.inline('⬅️ Back', data='strats')],
                    ]
                    await event.edit(detail, buttons=rows)
                    return
                if data.startswith('edit_strategy:'):
                    # Open builder prefilled with selected strategy for editing
                    try:
                        idx = int(data.split(':', 1)[1])
                    except Exception:
                        await event.answer('Invalid')
                        return
                    all_items = load_strategies()
                    owned = [s for s in all_items if _belongs_to(s, event.sender_id)]
                    if not (1 <= idx <= len(owned)):
                        await event.answer('Out of range')
                        return
                    s = owned[idx - 1]
                    # Prefill state with existing strategy data
                    try:
                        # Deep copy via JSON to avoid mutating original while editing
                        data_copy = json.loads(json.dumps({
                            'name': s.get('name'),
                            'group': s.get('group'),
                            'filters': s.get('filters') or {},
                            'action': s.get('action') or {},
                            'trojan': s.get('trojan') or {},
                        }))
                    except Exception:
                        data_copy = {
                            'name': s.get('name'),
                            'group': s.get('group'),
                            'filters': s.get('filters') or {},
                            'action': s.get('action') or {},
                            'trojan': s.get('trojan') or {},
                        }
                    st = {'mode': 'edit', 'step': 'builder', 'data': data_copy, 'edit_index': idx}
                    CONV_STATE[chat_id] = st
                    await event.edit(_builder_text(st), buttons=_builder_buttons(st))
                    return
                if data.startswith('delete:'):
                    # Ask for confirmation before deleting
                    try:
                        idx = int(data.split(':', 1)[1])
                    except Exception:
                        await event.answer('Invalid')
                        return
                    _all, s = _get_owned_strategy(event.sender_id, idx)
                    if not s:
                        await event.answer('Not found')
                        return
                    name = s.get('name', '(unnamed)')
                    await event.edit(
                        f"Delete strategy #{idx} '{name}'?",
                        buttons=[[Button.inline('❌ Cancel', data=f'view:{idx}'), Button.inline('✅ Confirm', data=f'confirm_delete:{idx}')]]
                    )
                    return
                if data.startswith('confirm_delete:'):
                    try:
                        idx = int(data.split(':', 1)[1])
                    except Exception:
                        await event.answer('Invalid')
                        return
                    items = load_strategies()
                    owned = [s for s in items if _belongs_to(s, event.sender_id)]
                    if not (1 <= idx <= len(owned)):
                        await event.answer('Out of range')
                        return
                    target = owned[idx - 1]
                    # Remove the exact object from items list
                    try:
                        items.remove(target)
                        ok = save_strategies(items)
                    except Exception:
                        ok = False
                    if ok:
                        await event.answer('Deleted')
                        text, rows = _strategies_page(event.sender_id, page=1)
                        await event.edit(text, buttons=rows)
                    else:
                        await event.answer('Delete failed')
                    return
                if data.startswith('toggle:'):
                    try:
                        idx = int(data.split(':', 1)[1])
                    except Exception:
                        await event.answer('Invalid')
                        return
                    items = load_strategies()
                    owned = [s for s in items if _belongs_to(s, event.sender_id)]
                    if not (1 <= idx <= len(owned)):
                        await event.answer('Out of range')
                        return
                    s = owned[idx - 1]
                    s['enabled'] = not s.get('enabled', True)
                    if save_strategies(items):
                        await event.answer('Saved')
                    else:
                        await event.answer('Save failed')
                    # Refresh strategy detail view with updated buttons
                    enabled = s.get('enabled', True)
                    detail = _fmt_strategy_human(idx, s)
                    rows = [
                        [Button.inline('✏️ Edit', data=f'edit_strategy:{idx}')],
                        [Button.inline('✅ Enable' if not enabled else '⏸ Disable', data=f'toggle:{idx}')],
                        [Button.inline('🗑 Delete', data=f'delete:{idx}')],
                        [Button.inline('⬅️ Back', data='strats')],
                    ]
                    await event.edit(detail, buttons=rows)
                    return
                if data == 'new':
                    # Always send a fresh builder message to avoid edit errors
                    st = {'mode': 'create', 'step': 'builder', 'data': {}}
                    CONV_STATE[chat_id] = st
                    try:
                        await event.answer('New Strategy')
                    except Exception:
                        pass
                    msg = await bot_client.send_message(chat_id, _builder_text(st), buttons=_builder_buttons(st))
                    st['msg_id'] = getattr(msg, 'id', None)
                    return
                if data.startswith('new_group:'):
                    group = data.split(':', 1)[1]
                    st = CONV_STATE.setdefault(chat_id, {'mode': 'create', 'step': 'group', 'data': {}})
                    st['data']['group'] = group
                    st['step'] = 'builder'
                    # Return to builder in-place
                    await event.edit(_builder_text(st), buttons=_builder_buttons(st))
                    return
                if data == 'builder':
                    st = CONV_STATE.get(chat_id)
                    if not st:
                        await event.answer('No active builder')
                        return
                    # Allow returning to builder from any edit step
                    st['step'] = 'builder'
                    await event.edit(_builder_text(st), buttons=_builder_buttons(st))
                    return
                if data.startswith('edit:'):
                    st = CONV_STATE.get(chat_id)
                    if not st:
                        await event.answer('No active builder')
                        return
                    key = data.split(':', 1)[1]
                    st['step'] = f'edit_{key}'
                    prompts = {
                        'token_age': "Enter Token Age as 'min,max' (minutes) or a single value:",
                        'first_buy': "Enter First Buy % as 'min,max' or a single value:",
                        'balance': "Enter Balance (SOL) as 'min,max' or a single value:",
                        'tx': "Enter Transactions as 'min,max' or a single value:",
                        'amount': 'Amount (SOL):',
                        'expiry': 'Expiry (e.g., 45s, 30m, 2h, 1d):',
                        'slippage': 'Slippage %:',
                        'trigger': 'Trigger Price (SOL):',
                        'action_target': 'Action Target (e.g., @mychannel):',
                        'action_template': 'Action Template (e.g., buy {token}):',
                    }
                    await event.edit(prompts.get(key, 'Enter value:'), buttons=[[Button.inline('⬅️ Back', data='builder')]])
                    return
                if data == 'menu:label':
                    st = CONV_STATE.get(chat_id)
                    if not st:
                        await event.answer('No active builder')
                        return
                    await event.edit(_label_menu_text(st), buttons=_label_menu_buttons())
                    return
                if data == 'menu:order':
                    st = CONV_STATE.get(chat_id)
                    if not st:
                        await event.answer('No active builder')
                        return
                    await event.edit(_order_menu_text(st), buttons=_order_menu_buttons())
                    return
                if data.startswith('label:'):
                    st = CONV_STATE.get(chat_id)
                    if not st:
                        await event.answer('No active builder')
                        return
                    label = data.split(':', 1)[1]
                    st.setdefault('data', {}).setdefault('filters', {})['Label'] = label
                    await event.edit(_builder_text(st), buttons=_builder_buttons(st))
                    return
                if data.startswith('set:'):
                    # Quick-pick setter: set:field:value
                    st = CONV_STATE.get(chat_id)
                    if not st:
                        await event.answer('No active builder')
                        return
                    _, field, val = data.split(':', 2)
                    d = st.setdefault('data', {})
                    if field == 'amount':
                        num = _parse_number(val)
                        if num is not None:
                            d.setdefault('trojan', {})['amount'] = num
                    elif field == 'expiry':
                        minutes = _parse_duration_to_minutes(val)
                        if minutes is not None:
                            d.setdefault('trojan', {})['expiry_minutes'] = minutes
                    elif field == 'slippage':
                        num = _parse_number(val)
                        if num is not None:
                            d.setdefault('trojan', {})['slippage_pct'] = num
                    elif field == 'trigger':
                        num = _parse_number(val)
                        if num is not None:
                            d.setdefault('trojan', {})['trigger_price'] = num
                    st['step'] = 'builder'
                    await event.edit(_builder_text(st), buttons=_builder_buttons(st))
                    return
                if data == 'save':
                    st = CONV_STATE.get(chat_id)
                    if not st:
                        await event.answer('Nothing to save')
                        return
                    d = st.get('data', {})
                    if not d.get('group'):
                        await event.answer('Choose a group first')
                        return
                    # Enforce minimum filters for non-Trojan strategies
                    if d.get('group') != '@solana_trojanbot':
                        f = d.get('filters', {})
                        if _count_filled_filters(f) < 3:
                            await event.answer('Set at least 3 filter options before saving', alert=False)
                            # Show builder again
                            await event.edit(_builder_text(st), buttons=_builder_buttons(st))
                            return
                    if st.get('mode') == 'edit':
                        # Update existing strategy (by owned index)
                        items = load_strategies()
                        owned = [s for s in items if _belongs_to(s, event.sender_id)]
                        try:
                            eidx = int(st.get('edit_index') or 0)
                        except Exception:
                            eidx = 0
                        if not (1 <= eidx <= len(owned)):
                            await event.edit('Save failed ❌', buttons=[[Button.inline('⬅️ Back', data='dash')]])
                            CONV_STATE.pop(chat_id, None)
                            return
                        target = owned[eidx - 1]
                        # Preserve id, owner_id, enabled unless explicitly set
                        target['name'] = d.get('name') or target.get('name') or f"Strategy {int(time.time())}"
                        target['group'] = d.get('group')
                        target['filters'] = d.get('filters', {})
                        target['action'] = d.get('action')
                        target['trojan'] = d.get('trojan')
                        ok = save_strategies(items)
                        CONV_STATE.pop(chat_id, None)
                        if ok:
                            await event.edit('Updated ✅', buttons=[[Button.inline('⬅️ Back', data='strats')]])
                        else:
                            await event.edit('Save failed ❌', buttons=[[Button.inline('⬅️ Back', data='dash')]])
                        return
                    else:
                        # Create new strategy
                        name = d.get('name') or f"Strategy {int(time.time())}"
                        strategy = {
                            'id': int(time.time()),
                            'name': name,
                            'group': d.get('group'),
                            'enabled': True,
                            'filters': d.get('filters', {}),
                            'action': d.get('action'),
                            'trojan': d.get('trojan'),
                            'owner_id': int(event.sender_id),
                        }
                        items = load_strategies()
                        items.append(strategy)
                        ok = save_strategies(items)
                        CONV_STATE.pop(chat_id, None)
                        if ok:
                            await event.edit('Saved ✅', buttons=[[Button.inline('⬅️ Back', data='dash')]])
                        else:
                            await event.edit('Save failed ❌', buttons=[[Button.inline('⬅️ Back', data='dash')]])
                        return
                if data in ('cancel',):
                    CONV_STATE.pop(chat_id, None)
                    await event.edit('Canceled', buttons=[[Button.inline('⬅️ Back', data='dash')]])
                    return
                if data == 'pause':
                    set_paused(True)
                    await event.answer('Paused')
                    # Return to My Strategies page to reflect change
                    text, rows = _strategies_page(event.sender_id, page=1)
                    await event.edit(text, buttons=rows)
                    return
                if data == 'resume':
                    set_paused(False)
                    await event.answer('Resumed')
                    text, rows = _strategies_page(event.sender_id, page=1)
                    await event.edit(text, buttons=rows)
                    return
                if data == 'reload':
                    _ = load_strategies()
                    await event.answer('Reloaded')
                    text, rows = _strategies_page(event.sender_id, page=1)
                    await event.edit(text, buttons=rows)
                    return
                if data == 'limit_menu':
                    # Ad-hoc LIMIT order flow
                    CONV_STATE[chat_id] = {'mode': 'limit_ad_hoc', 'step': 'token', 'data': {}}
                    await event.edit('Enter Token Address for LIMIT order:', buttons=[[Button.inline('⬅️ Back', data='dash')]])
                    return
                if data.startswith('limit:'):
                    # Start LIMIT convo from a pass context (token prefilled)
                    cid = data.split(':', 1)[1]
                    ctx = PASS_CTX.get(cid) or {}
                    token = ctx.get('token')
                    st = {'mode': 'limit', 'step': 'amount', 'data': {'token': token, 'strategy_name': ctx.get('strat_name')}}
                    CONV_STATE[chat_id] = st
                    await event.edit(f"Amount (SOL) for {token}:", buttons=[[Button.inline('⬅️ Back', data='dash')]])
                    return
                # Fallback: show dashboard
                await event.answer('Updated')
                await event.edit(_status_text(), buttons=[
                    [Button.inline('📊 STATUS', data='stat')],
                    [Button.inline('🟢 ON', data='resume')],
                    [Button.inline('🔴 OFF', data='pause')],
                    [Button.inline('📁 MY STRATEGIES', data='strats')],
                    [Button.inline('➕ NEW STRATEGY', data='new')],
                ])
            except Exception as e:
                logging.exception('Callback error: %s', e)
                try:
                    await event.answer('Error', alert=False)
                except Exception:
                    pass

        # ---------- Conversation text input ----------
    async def bot_conversation(event):
            if not event.is_private or not _authorized(event.sender_id):
                return
            chat_id = event.chat_id
            # Ignore slash commands here so they don't trigger the builder fallback
            txt = (event.raw_text or '').strip()
            if txt.startswith('/'):
                return
            st = CONV_STATE.get(chat_id)
            if not st or st.get('mode') not in ('create', 'edit', 'quick_action', 'limit', 'limit_ad_hoc'):
                return
            # Delete the user's input message so only the dashboard/prompt message remains
            try:
                await event.delete()
            except Exception:
                pass
            text = txt
            try:
                # Decide branch by group or edit-type
                group = st['data'].get('group')
                if st['step'] == 'name':
                    st['data']['name'] = text
                    st['step'] = 'builder'
                    msg_id = st.get('msg_id')
                    if msg_id:
                        await bot_client.edit_message(chat_id, msg_id, _builder_text(st), buttons=_builder_buttons(st))
                    else:
                        _msg = await bot_client.send_message(chat_id, _builder_text(st), buttons=_builder_buttons(st))
                        st['msg_id'] = getattr(_msg, 'id', None)
                    return
                if st['step'] == 'edit_name':
                    st.setdefault('data', {})['name'] = text
                    st['step'] = 'builder'
                    msg_id = st.get('msg_id')
                    if msg_id:
                        await bot_client.edit_message(chat_id, msg_id, _builder_text(st), buttons=_builder_buttons(st))
                    else:
                        _msg = await bot_client.send_message(chat_id, _builder_text(st), buttons=_builder_buttons(st))
                        st['msg_id'] = getattr(_msg, 'id', None)
                    return
                if st['step'].startswith('edit_'):
                    # Handle edits from builder
                    key = st['step'][5:]
                    if group == '@solana_trojanbot':
                        trojan = st['data'].setdefault('trojan', {})
                        if key == 'expiry':
                            number = _parse_duration_to_minutes(text)
                            if number is None:
                                msg_id = st.get('msg_id')
                                prompt = 'Invalid expiry. Use formats like 45s, 30m, 2h, 1d.'
                                if msg_id:
                                    await bot_client.edit_message(chat_id, msg_id, prompt, buttons=[[Button.inline('⬅️ Back', data='builder')]])
                                else:
                                    await event.respond(prompt, buttons=[[Button.inline('⬅️ Back', data='builder')]])
                                return
                        else:
                            number = _parse_number(text)
                            if number is None:
                                msg_id = st.get('msg_id')
                                prompt = 'Invalid number. Try again or tap Back.'
                                if msg_id:
                                    await bot_client.edit_message(chat_id, msg_id, prompt, buttons=[[Button.inline('⬅️ Back', data='builder')]])
                                else:
                                    await event.respond(prompt, buttons=[[Button.inline('⬅️ Back', data='builder')]])
                                return
                        mapping = {
                            'amount': 'amount',
                            'expiry': 'expiry_minutes',
                            'slippage': 'slippage_pct',
                            'trigger': 'trigger_price',
                        }
                        dest = mapping.get(key)
                        if dest:
                            trojan[dest] = number
                    else:
                        filters = st['data'].setdefault('filters', {})
                        # Allow setting Order LIMIT defaults in non-Trojan groups too
                        if key in ('amount','expiry','slippage','trigger'):
                            if key == 'expiry':
                                number = _parse_duration_to_minutes(text)
                                if number is None:
                                    msg_id = st.get('msg_id')
                                    prompt = 'Invalid expiry. Use formats like 45s, 30m, 2h, 1d.'
                                    if msg_id:
                                        await bot_client.edit_message(chat_id, msg_id, prompt, buttons=[[Button.inline('⬅️ Back', data='builder')]])
                                    else:
                                        await event.respond(prompt, buttons=[[Button.inline('⬅️ Back', data='builder')]])
                                    return
                            else:
                                number = _parse_number(text)
                                if number is None:
                                    msg_id = st.get('msg_id')
                                    prompt = 'Invalid number. Try again or tap Back.'
                                    if msg_id:
                                        await bot_client.edit_message(chat_id, msg_id, prompt, buttons=[[Button.inline('⬅️ Back', data='builder')]])
                                    else:
                                        await event.respond(prompt, buttons=[[Button.inline('⬅️ Back', data='builder')]])
                                    return
                            trojan = st['data'].setdefault('trojan', {})
                            mapping = {
                                'amount': 'amount',
                                'expiry': 'expiry_minutes',
                                'slippage': 'slippage_pct',
                                'trigger': 'trigger_price',
                            }
                            dest = mapping.get(key)
                            if dest:
                                trojan[dest] = number
                        else:
                            if key in ('token_age', 'first_buy', 'balance', 'tx'):
                                rng = _parse_range(text)
                                if not rng:
                                    msg_id = st.get('msg_id')
                                    prompt = "Invalid format. Use 'min,max'. Tap Back to cancel."
                                    if msg_id:
                                        await bot_client.edit_message(chat_id, msg_id, prompt, buttons=[[Button.inline('⬅️ Back', data='builder')]])
                                    else:
                                        await event.respond(prompt, buttons=[[Button.inline('⬅️ Back', data='builder')]])
                                    return
                                mapping = {
                                    'token_age': 'Token Age (minutes)',
                                    'first_buy': 'First Buy (%)',
                                    'balance': 'Balance (SOL)',
                                    'tx': 'Transactions (count)',
                                }
                                filters[mapping[key]] = rng
                            elif key == 'action_target':
                                st['data'].setdefault('action', {})['target'] = text
                            elif key == 'action_template':
                                st['data'].setdefault('action', {})['template'] = text
                    st['step'] = 'builder'
                    msg_id = st.get('msg_id')
                    if msg_id:
                        await bot_client.edit_message(chat_id, msg_id, _builder_text(st), buttons=_builder_buttons(st))
                    else:
                        _msg = await bot_client.send_message(chat_id, _builder_text(st), buttons=_builder_buttons(st))
                        st['msg_id'] = getattr(_msg, 'id', None)
                    return
                if st.get('mode') == 'quick_action':
                    # Prompt chain: target -> template -> send
                    d = st.setdefault('data', {})
                    items = load_strategies()
                    idx = int(d.get('index') or 0)
                    if not (1 <= idx <= len(items)):
                        await event.respond('Strategy not found. Use /start', buttons=[[Button.inline('⬅️ Back', data='dash')]])
                        CONV_STATE.pop(chat_id, None)
                        return
                    s = items[idx - 1]
                    if st['step'] == 'target':
                        target = text.lstrip('@')
                        s.setdefault('action', {})['target'] = '@' + target
                        save_strategies(items)
                        st['step'] = 'template'
                        await event.respond('Set Action Template (e.g., "buy {token}"). Placeholders: {token} {name}', buttons=[[Button.inline('⬅️ Back', data='dash')]])
                        return
                    if st['step'] == 'template':
                        tmpl = text if text else '{token}'
                        s.setdefault('action', {})['template'] = tmpl
                        save_strategies(items)
                        # Send now using stored token
                        token = d.get('token', '')
                        msg = _safe_format(tmpl, {'token': token, 'name': s.get('name', '')})
                        try:
                            await bot_client.send_message(s['action']['target'], msg)
                            await event.respond('Sent ✅')
                        except Exception as e:
                            logging.error('Quick action send failed: %s', e)
                            await event.respond('Failed to send')
                        CONV_STATE.pop(chat_id, None)
                        return
                if st.get('mode') == 'limit':
                    # Capture limit fields and send to Trojan
                    d = st.setdefault('data', {})
                    step = st.get('step')
                    def _num_or_error(_label):
                        val = _parse_number(text)
                        if val is None:
                            return None
                        return val
                    if step == 'amount':
                        val = _num_or_error('amount')
                        if val is None:
                            await event.respond('Invalid amount. Enter number.', buttons=[[Button.inline('⬅️ Back', data='dash')]])
                            return
                        d['amount'] = val
                        st['step'] = 'expiry'
                        await event.respond('Expiry (e.g., 45s, 30m, 2h, 1d):', buttons=[[Button.inline('⬅️ Back', data='dash')]])
                        return
                    if step == 'expiry':
                        val = _parse_duration_to_minutes(text)
                        if val is None:
                            await event.respond('Invalid expiry. Use formats like 45s, 30m, 2h, 1d.', buttons=[[Button.inline('⬅️ Back', data='dash')]])
                            return
                        d['expiry'] = val
                        st['step'] = 'slippage'
                        await event.respond('Slippage %:', buttons=[[Button.inline('⬅️ Back', data='dash')]])
                        return
                    if step == 'slippage':
                        val = _num_or_error('slippage')
                        if val is None:
                            await event.respond('Invalid slippage. Enter percent.', buttons=[[Button.inline('⬅️ Back', data='dash')]])
                            return
                        d['slippage'] = val
                        st['step'] = 'trigger'
                        await event.respond('Trigger Price (SOL):', buttons=[[Button.inline('⬅️ Back', data='dash')]])
                        return
                    if step == 'trigger':
                        val = _num_or_error('trigger')
                        if val is None:
                            await event.respond('Invalid trigger price. Enter number.', buttons=[[Button.inline('⬅️ Back', data='dash')]])
                            return
                        d['trigger'] = val
                        # Now send if action exists; else prompt quick action
                        items = load_strategies()
                        s = next((x for x in items if _belongs_to(x, event.sender_id) and x.get('name') == d.get('strategy_name')), None)
                        if s:
                            try:
                                await _send_limit_to_trojan(d.get('token',''), d.get('amount'), d.get('expiry'), d.get('slippage'), d.get('trigger'))
                                await event.respond('LIMIT sent ✅')
                            except Exception as e:
                                logging.error('LIMIT send failed: %s', e)
                                await event.respond('Failed to send')
                            CONV_STATE.pop(chat_id, None)
                            return
                        await event.respond('Strategy not found. Use /start', buttons=[[Button.inline('⬅️ Back', data='dash')]])
                        CONV_STATE.pop(chat_id, None)
                        return
                if st.get('mode') == 'limit_ad_hoc':
                    d = st.setdefault('data', {})
                    step = st.get('step')
                    if step == 'token':
                        d['token'] = text
                        st['step'] = 'amount'
                        await event.respond('Amount (SOL):', buttons=[[Button.inline('⬅️ Back', data='dash')]])
                        return
                    def _num():
                        return _parse_number(text)
                    if step == 'amount':
                        v = _num()
                        if v is None:
                            await event.respond('Invalid amount. Enter number.', buttons=[[Button.inline('⬅️ Back', data='dash')]])
                            return
                        d['amount'] = v
                        st['step'] = 'expiry'
                        await event.respond('Expiry (e.g., 45s, 30m, 2h, 1d):', buttons=[[Button.inline('⬅️ Back', data='dash')]])
                        return
                    if step == 'expiry':
                        v = _parse_duration_to_minutes(text)
                        if v is None:
                            await event.respond('Invalid expiry. Use formats like 45s, 30m, 2h, 1d.', buttons=[[Button.inline('⬅️ Back', data='dash')]])
                            return
                        d['expiry'] = v
                        st['step'] = 'slippage'
                        await event.respond('Slippage %:', buttons=[[Button.inline('⬅️ Back', data='dash')]])
                        return
                    if step == 'slippage':
                        v = _num()
                        if v is None:
                            await event.respond('Invalid slippage. Enter percent.', buttons=[[Button.inline('⬅️ Back', data='dash')]])
                            return
                        d['slippage'] = v
                        st['step'] = 'trigger'
                        await event.respond('Trigger Price (SOL):', buttons=[[Button.inline('⬅️ Back', data='dash')]])
                        return
                    if step == 'trigger':
                        v = _num()
                        if v is None:
                            await event.respond('Invalid trigger. Enter number.', buttons=[[Button.inline('⬅️ Back', data='dash')]])
                            return
                        d['trigger'] = v
                        try:
                            await _send_limit_to_trojan(d['token'], d['amount'], d['expiry'], d['slippage'], d['trigger'])
                            await event.respond('LIMIT sent ✅')
                        except Exception as e:
                            logging.error('LIMIT send failed: %s', e)
                            await event.respond('Failed to send')
                        CONV_STATE.pop(chat_id, None)
                        return
                # Fallback: don't auto-open the builder; keep quiet to avoid accidental popups
                return
            except Exception as e:
                logging.exception('Conversation error: %s', e)
                await event.respond('Error. Type /new to restart wizard.')

    # Register the conversation handler explicitly
    bot_client.add_event_handler(bot_conversation, events.NewMessage())

    # Command handler: control via your Saved Messages (chats='me'), using slash commands
    @client.on(events.NewMessage(outgoing=True, chats='me'))
    async def command_handler(event):
        global PROCESSING_PAUSED
        text = (event.raw_text or '').strip()
        if not text.startswith('/'):
            return  # ignore non-commands to avoid loops

        cmd, *rest = text.split(maxsplit=1)
        arg = rest[0].strip() if rest else ''

        async def reply(msg: str):
            try:
                await event.respond(msg)
            except Exception as e:
                logging.error(f"Failed to send reply: {e}")

        if cmd in ('/help', '/h'):
            await reply(
                "Commands:\n"
                "/help – this help\n"
                "/status – show bot status\n"
                "/strategies – list strategies with index\n"
                "/enable <#idx|name> – enable a strategy\n"
                "/disable <#idx|name> – disable a strategy\n"
                "/pause – pause processing group messages\n"
                "/resume – resume processing\n"
                "/reload – reload strategies.json (ack)\n"
                "/ping – health check"
            )
            return

        if cmd == '/ping':
            await reply('pong')
            return

        if cmd == '/status':
            strategies = load_strategies()
            enabled = sum(1 for s in strategies if s.get('enabled', True))
            total = len(strategies)
            cache_size = len(_processed_cache)
            await reply(
                f"Status:\nPaused: {PROCESSING_PAUSED}\n"
                f"Self ID: {SELF_ID}\n"
                f"Strategies: {enabled}/{total} enabled\n"
                f"Dedup cache size: {cache_size}"
            )
            return

        if cmd == '/strategies':
            strategies = load_strategies()
            if not strategies:
                await reply('No strategies found.')
                return
            lines = []
            for i, s in enumerate(strategies, start=1):
                flag = '✅' if s.get('enabled', True) else '⏸️'
                lines.append(f"#{i} {flag} {s.get('name','(unnamed)')} — {s.get('group','?')}")
            await reply("\n".join(lines))
            return

        def _match_strategy(strategies: list, key: str):
            if not key:
                return None, 'Provide a name or #index'
            if key.startswith('#') and key[1:].isdigit():
                idx = int(key[1:])
                if 1 <= idx <= len(strategies):
                    return strategies[idx-1], None
                return None, 'Index out of range'
            # name match (case-insensitive)
            low = key.lower()
            for s in strategies:
                if str(s.get('name','')).lower() == low:
                    return s, None
            return None, 'Strategy not found'

        if cmd in ('/enable', '/disable'):
            strategies = load_strategies()
            s, err = _match_strategy(strategies, arg)
            if err:
                await reply(f"Error: {err}")
                return
            s['enabled'] = (cmd == '/enable')
            if save_strategies(strategies):
                await reply(f"{cmd[1:].capitalize()}d: {s.get('name')}")
            else:
                await reply('Failed to save strategies.json')
            return

        if cmd == '/pause':
            PROCESSING_PAUSED = True
            await reply('Processing paused')
            return

        if cmd == '/resume':
            PROCESSING_PAUSED = False
            await reply('Processing resumed')
            return

        if cmd == '/reload':
            # No-op since we load on demand; provide ack
            _ = load_strategies()
            await reply('Reloaded strategies.json')
            return

        # Unknown
        await reply('Unknown command. Use /help')

    @client.on(events.NewMessage)
    async def handler(event):
        monitored_groups = [
            'SolanaNewPumpfun',
            'solana_trojanbot',
        ]

        if event.chat and hasattr(event.chat, 'username'):
            group_username = event.chat.username
            if group_username in monitored_groups:
                logging.info(f"📱 New message in @{group_username}")
                # De-dup across groups: prefer token address; fallback to message hash
                msg_text = event.message.message or ''
                if _is_duplicate(msg_text):
                    logging.info("⏩ Duplicate listing detected; skipping")
                    return
                if group_username == 'solana_trojanbot':
                    # Process with Trojan strategy semantics
                    await process_trojan_message(event, group_username)
                else:
                    await process_token_message(event, group_username)

    if bot_client:
        # Run both clients concurrently
        import asyncio as _asyncio
        await _asyncio.gather(
            client.run_until_disconnected(),
            bot_client.run_until_disconnected(),
        )
    else:
        await client.run_until_disconnected()

# To run Telethon client, call start_telethon() from your main event loop.

async def process_trojan_message(event, group_username: str):
    """Process messages for @solana_trojanbot: prepare LIMIT orders from strategy trojan config.
    We don't parse numeric fields from the message; we use the strategy's stored trojan settings.
    """
    global PROCESSING_PAUSED
    if PROCESSING_PAUSED:
        logging.debug("Processing paused; skipping trojan message")
        return
    msg_text = event.message.message or ''
    token_address = extract_token_address(msg_text)

    strategies = load_strategies()
    gui_group = f"@{group_username}"

    for idx, strat in enumerate(strategies, start=1):
        try:
            if not strat.get('enabled', True):
                continue
            if strat.get('group') != gui_group:
                continue
            trojan = strat.get('trojan') or {}
            # Require all trojan fields
            amount = trojan.get('amount')
            expiry = trojan.get('expiry_minutes')
            slippage = trojan.get('slippage_pct')
            trigger = trojan.get('trigger_price')
            if None in (amount, expiry, slippage, trigger):
                logging.debug(f"❌ {strat.get('name')} missing Trojan fields; skipping")
                continue

            logging.info(
                f"🧪 Trojan LIMIT order prepared for '{strat.get('name')}' | "
                f"amount={amount} SOL, expiry={expiry} min, slippage={slippage}%, trigger={trigger} SOL"
                + (f" | Token: {token_address}" if token_address else "")
            )
            # Notify control chat with inline actions
            if bot_client and token_address:
                ctx = {
                    'token': token_address,
                    'amount': amount,
                    'expiry': expiry,
                    'slippage': slippage,
                    'trigger': trigger,
                    'strat_index': idx,
                    'strat_name': strat.get('name',''),
                }
                cid = _store_pass_ctx(ctx)
                chat_id = strat.get('owner_id') or _get_control_chat_id()
                if chat_id:
                    ds_link = f"https://dexscreener.com/solana/{token_address}"
                    text = (
                        f"🧪 Trojan ready: {strat.get('name')}\n"
                        f"Token: {token_address}\n"
                        f"Amt: {amount} | Slippage: {slippage}% | Trigger: {trigger} | Exp: {_fmt_minutes_human(expiry)}\n\n"
                        f"🔗 {ds_link}"
                    )
                    buttons = [[Button.inline('📈 Order LIMIT', data=f'limit:{cid}')]]
                    try:
                        await bot_client.send_message(chat_id, text, buttons=buttons)
                    except Exception as _e:
                        logging.warning(f"Notify failed: {_e}")
                    if POST_ORDER_LIMIT_IN_GROUPS:
                        try:
                            await bot_client.send_message(event.chat_id, text, buttons=buttons)
                        except Exception as _e2:
                            logging.debug(f"Group notify skipped: {_e2}")
            # Execute action if configured (e.g., send command to @solana_trojanbot)
            action = strat.get('action') or {}
            target = action.get('target')
            template = action.get('template')
            if target and template:
                ctx = {
                    'token': token_address or '',
                    'amount': amount,
                    'expiry': expiry,
                    'slippage': slippage,
                    'trigger': trigger,
                    'name': strat.get('name', ''),
                }
                msg = _safe_format(template, ctx)
                await _send_action_message(target, msg)
        except Exception as e:
            logging.error(f"Error evaluating Trojan strategy '{strat.get('name')}': {e}")


def _normalize_text(t: str) -> str:
    t = t.strip().lower()
    t = re.sub(r'\s+', ' ', t)
    return t


def _dedupe_key_for(text: str) -> str:
    addr = extract_token_address(text or '')
    if addr:
        return f"token:{addr}"
    norm = _normalize_text(text or '')
    digest = hashlib.sha1(norm.encode('utf-8')).hexdigest()
    return f"msg:{digest}"

def _is_duplicate(text: str) -> bool:
    """Return True if this message (or token address) was seen recently.
    Uses a TTL cache keyed by token address when available, otherwise by
    a hash of normalized message text. TTL is defined by DEDUP_TTL_SECONDS.
    """
    now = time.time()
    # Evict expired entries
    for k, ts in list(_processed_cache.items()):
        try:
            if now - float(ts) > float(DEDUP_TTL_SECONDS):
                _processed_cache.pop(k, None)
        except Exception:
            _processed_cache.pop(k, None)
    key = _dedupe_key_for(text or '')
    ts = _processed_cache.get(key)
    if ts is not None and (now - float(ts)) <= float(DEDUP_TTL_SECONDS):
        return True
    _processed_cache[key] = now
    return False
