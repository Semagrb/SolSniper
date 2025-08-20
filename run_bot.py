import asyncio
import logging

from telethon_client import start_telethon


def main():
	logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
	loop = asyncio.new_event_loop()
	asyncio.set_event_loop(loop)
	try:
		loop.run_until_complete(start_telethon())
	finally:
		loop.close()


if __name__ == '__main__':
	main()

