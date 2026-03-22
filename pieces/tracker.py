import aiohttp
import random
import logging
import socket
from struct import unpack
from urllib.parse import urlencode

from . import bencoding


class TrackerResponse:
    def __init__(self, response: dict):
        self.response = response

    @property
    def failure(self):
        if b'failure reason' in self.response:
            return self.response[b'failure reason'].decode('utf-8')
        return None

    @property
    def interval(self) -> int:
        return self.response.get(b'interval', 0)

    @property
    def complete(self) -> int:
        return self.response.get(b'complete', 0)

    @property
    def incomplete(self) -> int:
        return self.response.get(b'incomplete', 0)

    @property
    def peers(self):
        peers = self.response[b'peers']
        if type(peers) == list:
            # Dictionary model
            logging.debug('Dictionary model peers are returned by tracker')
            return [(p[b'ip'].decode('utf-8'), p[b'port']) for p in peers]
        else:
            # Binary model
            logging.debug('Binary model peers are returned by tracker')
            peers = [peers[i:i+6] for i in range(0, len(peers), 6)]
            return [(socket.inet_ntoa(p[:4]), _decode_port(p[4:]))
                    for p in peers]

class Tracker:
    """
    Tracker client for communicating with BitTorrent trackers via HTTP.

    This class manages a single aiohttp.ClientSession to enable connection
    pooling and reuse across multiple announce requests, avoiding the overhead
    of creating a new session per request.
    """
    def __init__(self, torrent):
        self.torrent = torrent
        self.peer_id = _calculate_peer_id()
        # Lazily created aiohttp session for connection pooling across requests
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        """
        Return the shared aiohttp.ClientSession, creating it if necessary.

        This ensures a single session is reused for all tracker HTTP requests,
        enabling connection pooling and DNS caching.
        """
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def connect(self,
                      first: bool = None,
                      uploaded: int = 0,
                      downloaded: int = 0):
        """
        Send an announce request to the tracker and return the response.

        Reuses the internal ClientSession for connection pooling.

        :param first: If truthy, includes 'event=started' to signal torrent start.
        :param uploaded: Bytes uploaded so far.
        :param downloaded: Bytes downloaded so far.
        :return: TrackerResponse containing interval and peer list.
        :raises ConnectionError: If the tracker returns a non-200 status or error.
        """
        params = {
            'info_hash': self.torrent.info_hash,
            'peer_id': self.peer_id,
            'port': 6889,
            'uploaded': uploaded,
            'downloaded': downloaded,
            'left': self.torrent.total_size - downloaded,
            'compact': 1}
        if first:
            params['event'] = 'started'

        url = self.torrent.announce + '?' + urlencode(params)
        logging.info('Connecting to tracker at: ' + url)

        session = await self._get_session()
        async with session.get(url) as response:
            if response.status != 200:
                raise ConnectionError('Unable to connect to tracker: status code {}'.format(response.status))
            data = await response.read()
            self.raise_for_error(data)
            return TrackerResponse(bencoding.Decoder(data).decode())

    async def close(self) -> None:
        """
        Close the underlying aiohttp.ClientSession if it exists.

        Should be called when the client is shutting down to release resources.
        """
        if self._session is not None and not self._session.closed:
            await self._session.close()
            self._session = None

    def raise_for_error(self, tracker_response):
        try:
            message = tracker_response.decode("utf-8")
            if "failure" in message:
                raise ConnectionError('Unable to connect to tracker: {}'.format(message))
        except UnicodeDecodeError:
            pass

def _calculate_peer_id():
    return '-PC0001-' + ''.join(
        [str(random.randint(0, 9)) for _ in range(12)])

def _decode_port(port):
    return unpack(">H", port)[0]
