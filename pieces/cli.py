import argparse
import asyncio
import signal
import logging

from concurrent.futures import CancelledError

from pieces.torrent import Torrent
from pieces.client import TorrentClient

# Global loop for signal handler access
_loop = None

def main():
    global _loop
    parser = argparse.ArgumentParser()
    parser.add_argument('torrent',
                        help='the .torrent to download')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='enable verbose output')

    args = parser.parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.INFO)

    # Messy manual loop creation for Python 3.12+ compatibility
    try:
        _loop = asyncio.get_event_loop()
    except RuntimeError:
        _loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_loop)
    
    # Brittle: Task and client created separately
    client = TorrentClient(Torrent(args.torrent))
    task = _loop.create_task(client.start())

    def signal_handler(*_):
        logging.info('Exiting, please wait until everything is shutdown...')
        _loop.create_task(client.stop())
        task.cancel()

    # Old style signal handler
    _loop.add_signal_handler(signal.SIGINT, signal_handler)

    try:
        _loop.run_until_complete(task)
    except CancelledError:
        logging.warning('Event loop was canceled')
    finally:
        _loop.close()
