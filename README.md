# Solana Trading Telegram Bot

A professional, production-ready Python Telegram bot for automating Solana-based trading strategies.

## Features
- GUI to create/edit/pause/delete strategies (persisted to `strategies.json`)
- Five filters in this order: Token Age, First Buy %, Balance (SOL), Transactions, Label
- Telethon listener monitors @SolanaNewPumpfun and evaluates strategies in real time
 - Telethon listener monitors @SolanaNewPumpfun and @solana_trojanbot
- Async, modular, secure
- Ready for VPS deployment

## Prerequisites
- Python 3.10+
- Telegram API ID and API HASH from https://my.telegram.org (API Development Tools)

## Setup
1. Clone the repo
2. In Windows PowerShell, install deps:
	`pip install -r .\requirements.txt`
3. Copy `.env.example` to `.env` and fill:
	- `TELETHON_API_ID`
	- `TELETHON_API_HASH`

## Usage
There are two entry points you run in separate terminals:

1) GUI for strategies
- Start: `python .\app.py`
- Create strategies for `@SolanaNewPumpfun`; they save to `strategies.json`.

2) Telethon listener
- Start: `python .\run_bot.py`
- Loads `strategies.json`, listens to:
	- @SolanaNewPumpfun: evaluates Token Age, First Buy %, Balance (SOL), Transactions, Label
	- @solana_trojanbot: prepares LIMIT orders using strategy fields Amount (SOL), Expiry (min), Slippage (%), Trigger Price (SOL)

Notes:
- Token Age is computed from the Telegram message timestamp.
- Label filter checks for phrases “Dev Has Enough Money” or “Dev Wallet Empty” in the message.
- Other numeric filters (First Buy %, Balance SOL, Transactions) are parsed from message text when present; if the data isn’t in the message, that filter won’t pass.
 - For @solana_trojanbot, no parsing is done; the LIMIT order parameters come from the strategy.

## Security
- Keep your `.env` secret; don’t commit it.
- Minimal scopes; rotate tokens when needed.

## License
MIT
