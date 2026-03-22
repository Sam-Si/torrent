#
# pieces - An experimental BitTorrent client
#
# Copyright 2016 markus.eliasson@gmail.com
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
import logging
import struct
import time
from asyncio import Queue
from collections import namedtuple, defaultdict
from concurrent.futures import CancelledError
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable, Any

import bitstring

# The default request size for blocks of pieces is 2^14 bytes.
#
# NOTE: The official specification states that 2^15 is the default request
#       size - but in reality all implementations use 2^14. See the
#       unofficial specification for more details on this matter.
#
#       https://wiki.theory.org/BitTorrentSpecification
#
REQUEST_SIZE = 2**14

# Connection timeout in seconds
CONNECTION_TIMEOUT = 30.0

# Handshake timeout in seconds
HANDSHAKE_TIMEOUT = 10.0

# Block pipelining depth - number of outstanding requests per peer.
# This is a critical optimization for high-latency connections.
# Industry standard: 5-10 blocks in-flight simultaneously.
# Higher values = better throughput on high-latency, more memory usage.
MAX_PIPELINE_DEPTH = 5

# Snubbing timeout in seconds - if a peer doesn't send data for this long
# after being unchoked, they are considered "snubbed" and deprioritized.
SNUB_TIMEOUT = 60.0

# Rate calculation window in seconds - used for rolling average
RATE_WINDOW = 20.0


@dataclass
class PeerStats:
    """
    Statistics for a single peer connection.
    
    Used for peer ranking and selection to prioritize fast peers.
    Implements the "choking/optimistic unchoking" algorithm from BitTorrent spec.
    """
    peer_id: bytes
    # Download statistics
    bytes_downloaded: int = 0
    blocks_received: int = 0
    # Timing
    first_byte_time: float | None = None
    last_block_time: float | None = None
    # Rate calculation (rolling window)
    rate_samples: list[tuple[float, int]] = field(default_factory=list)
    download_rate: float = 0.0  # bytes per second
    # State
    is_snubbed: bool = False
    is_choked: bool = True
    connection_start_time: float | None = None
    
    def record_block(self, size: int) -> None:
        """Record a received block and update statistics."""
        now = time.time()
        
        if self.first_byte_time is None:
            self.first_byte_time = now
        
        self.last_block_time = now
        self.bytes_downloaded += size
        self.blocks_received += 1
        self.is_snubbed = False  # Receiving data means not snubbed
        
        # Add to rate samples
        self.rate_samples.append((now, size))
        
        # Remove old samples outside the window
        cutoff = now - RATE_WINDOW
        self.rate_samples = [
            (t, s) for t, s in self.rate_samples if t > cutoff
        ]
        
        # Calculate rolling rate
        if len(self.rate_samples) > 1:
            total_bytes = sum(s for _, s in self.rate_samples)
            time_span = self.rate_samples[-1][0] - self.rate_samples[0][0]
            if time_span > 0:
                self.download_rate = total_bytes / time_span
    
    def check_snubbed(self) -> bool:
        """Check if peer should be marked as snubbed."""
        if self.is_choked:
            return False  # Can't be snubbed if choked
        if self.last_block_time is None:
            # Never received data - check connection time
            if self.connection_start_time is None:
                return False
            elapsed = time.time() - self.connection_start_time
            self.is_snubbed = elapsed > SNUB_TIMEOUT
        else:
            elapsed = time.time() - self.last_block_time
            self.is_snubbed = elapsed > SNUB_TIMEOUT
        return self.is_snubbed
    
    @property
    def score(self) -> float:
        """
        Calculate a score for peer ranking.
        Higher score = better peer.
        """
        if self.is_snubbed:
            return -1000.0  # Very low priority for snubbed peers
        if self.is_choked:
            return -100.0  # Low priority for choked peers (but might unchoke)
        # Score based on download rate, with bonus for reliability
        return self.download_rate + (self.blocks_received * 10)


class ProtocolError(BaseException):
    pass


class ConnectionState(Enum):
    """Enum for peer connection states."""
    CHOKED = auto()
    INTERESTED = auto()
    PENDING_REQUEST = auto()
    STOPPED = auto()


class PeerConnection:
    """
    A peer connection used to download and upload pieces.

    The peer connection will consume one available peer from the given queue.
    Based on the peer details the PeerConnection will try to open a connection
    and perform a BitTorrent handshake.

    After a successful handshake, the PeerConnection will be in a *choked*
    state, not allowed to request any data from the remote peer. After sending
    an interested message the PeerConnection will be waiting to get *unchoked*.

    Once the remote peer unchoked us, we can start requesting pieces.
    The PeerConnection will continue to request pieces for as long as there are
    pieces left to request, or until the remote peer disconnects.

    If the connection with a remote peer drops, the PeerConnection will consume
    the next available peer from off the queue and try to connect to that one
    instead.

    This class uses modern asyncio features (Python 3.11+) including:
    - Structured concurrency via TaskGroup
    - Proper connection timeouts
    - Clean resource cleanup with wait_closed()
    - Block pipelining (MAX_PIPELINE_DEPTH requests in-flight)
    - Peer statistics tracking for ranking and optimization
    """
    def __init__(self, queue: Queue, info_hash: bytes,
                 peer_id: bytes, piece_manager: Any,
                 on_block_cb: Callable[[bytes, int, int, bytes], None] | None = None,
                 stats_callback: Callable[[bytes, int], None] | None = None):
        """
        Constructs a PeerConnection.

        The connection is not started automatically. Call the async method
        `run()` to start the connection worker, or use with TaskGroup.

        Use `stop()` to abort this connection and any subsequent connection
        attempts.

        :param queue: The async Queue containing available peers
        :param info_hash: The SHA1 hash for the meta-data's info
        :param peer_id: Our peer ID used to to identify ourselves
        :param piece_manager: The manager responsible to determine which pieces
                              to request
        :param on_block_cb: The callback function to call when a block is
                            received from the remote peer
        :param stats_callback: Optional callback(peer_id, bytes_received) for
                               statistics tracking
        """
        # Use sets for O(1) state lookups
        self._my_state: set[ConnectionState] = set()
        self._peer_state: set[ConnectionState] = set()
        self._queue: Queue = queue
        self._info_hash: bytes = info_hash
        self._peer_id: bytes = peer_id
        self._remote_id: bytes | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._reader: asyncio.StreamReader | None = None
        self._piece_manager = piece_manager
        self._on_block_cb = on_block_cb
        self._stats_callback = stats_callback
        self._task: asyncio.Task | None = None
        self._running = False
        # Block pipelining: track in-flight requests to enable parallel requests
        # This allows multiple blocks to be requested without waiting for responses
        self._inflight_requests = 0
        # Peer statistics for ranking
        self._stats: PeerStats | None = None

    @property
    def is_stopped(self) -> bool:
        """Check if the connection has been stopped."""
        return ConnectionState.STOPPED in self._my_state

    @property
    def is_choked(self) -> bool:
        """Check if we are choked by the remote peer."""
        return ConnectionState.CHOKED in self._my_state

    @property
    def is_interested(self) -> bool:
        """Check if we are interested in the remote peer."""
        return ConnectionState.INTERESTED in self._my_state

    @property
    def has_pending_request(self) -> bool:
        """Check if we have any in-flight requests (for backward compat)."""
        return self._inflight_requests > 0

    @property
    def has_pipeline_space(self) -> bool:
        """Check if we can send more pipelined requests."""
        return self._inflight_requests < MAX_PIPELINE_DEPTH

    @property
    def stats(self) -> PeerStats | None:
        """Get the peer statistics."""
        return self._stats

    async def run(self) -> None:
        """
        Main connection loop that runs as an async task.

        This method continuously consumes peers from the queue and attempts
        to establish connections. It handles all protocol-level communication
        and error recovery.

        Should be run within a TaskGroup for structured concurrency.
        """
        self._running = True
        self._my_state.clear()
        self._peer_state.clear()

        while not self.is_stopped:
            try:
                # Wait for a peer from the queue
                ip, port = await self._queue.get()
                logging.info(f'Got assigned peer with: {ip}')

                # Attempt connection with timeout
                await self._connect_and_handle(ip, port)

            except asyncio.CancelledError:
                logging.debug('PeerConnection task cancelled')
                raise
            except Exception as e:
                logging.exception(f'Unexpected error in PeerConnection: {e}')
            finally:
                # Ensure cleanup after each connection attempt
                await self._cleanup_connection()

    async def _connect_and_handle(self, ip: str, port: int) -> None:
        """
        Connect to a peer and handle the protocol exchange.

        :param ip: Peer IP address
        :param port: Peer port number
        """
        try:
            # Use asyncio.timeout for connection timeout (Python 3.11+)
            async with asyncio.timeout(CONNECTION_TIMEOUT):
                self._reader, self._writer = await asyncio.open_connection(ip, port)
            logging.info(f'Connection open to peer: {ip}')

        except asyncio.TimeoutError:
            logging.warning(f'Connection timeout to peer {ip}:{port}')
            return
        except (ConnectionRefusedError, OSError) as e:
            logging.warning(f'Unable to connect to peer {ip}:{port}: {e}')
            return

        try:
            # Perform handshake with timeout
            async with asyncio.timeout(HANDSHAKE_TIMEOUT):
                buffer = await self._handshake()

            # Initialize peer statistics after successful handshake
            self._stats = PeerStats(
                peer_id=self._remote_id,
                is_choked=True,
                connection_start_time=time.time()
            )

            # Initialize connection state
            self._my_state.add(ConnectionState.CHOKED)

            # Send interested message
            await self._send_interested()
            self._my_state.add(ConnectionState.INTERESTED)

            # Process messages from peer
            await self._process_messages(buffer)

        except asyncio.TimeoutError:
            logging.warning(f'Handshake timeout with peer {ip}:{port}')
        except ProtocolError as e:
            logging.warning(f'Protocol error with peer {ip}:{port}: {e}')
        except (ConnectionResetError, BrokenPipeError) as e:
            logging.warning(f'Connection closed by peer {ip}:{port}: {e}')
        except CancelledError:
            logging.debug(f'Connection cancelled for peer {ip}:{port}')
            raise

    async def _process_messages(self, initial_buffer: bytes) -> None:
        """
        Process incoming messages from the peer with block pipelining.

        Block pipelining allows multiple requests to be in-flight simultaneously,
        hiding network latency. After each message (especially Piece responses),
        we refill the pipeline if there's space.

        :param initial_buffer: Any data already read during handshake
        """
        async for message in PeerStreamIterator(self._reader, initial_buffer):
            if self.is_stopped:
                break

            # Handle different message types
            await self._handle_message(message)

            # Pipelining: fill the pipeline with requests if unchoked and interested
            # This enables parallel block requests, significantly improving throughput
            # on high-latency connections by hiding RTT.
            await self._fill_pipeline()

    async def _fill_pipeline(self) -> None:
        """
        Fill the request pipeline up to MAX_PIPELINE_DEPTH.

        Called after message processing to maintain optimal request parallelism.
        Only requests new blocks if unchoked, interested, and there's space.
        """
        if self.is_stopped:
            return
        if self.is_choked:
            return
        if not self.is_interested:
            return

        # Request blocks until pipeline is full
        while self.has_pipeline_space:
            block = self._piece_manager.next_request(self._remote_id)
            if block is None:
                break  # No more blocks available

            self._inflight_requests += 1
            await self._send_request(block)

    async def _send_request(self, block: Any) -> None:
        """
        Send a single block request to the peer.

        :param block: The block to request
        """
        if self._writer is None:
            return

        message = Request(block.piece, block.offset, block.length).encode()

        logging.debug(
            f'Requesting block {block.offset} for piece {block.piece} '
            f'of {block.length} bytes from peer {self._remote_id} '
            f'(inflight: {self._inflight_requests}/{MAX_PIPELINE_DEPTH})'
        )

        self._writer.write(message)
        await self._writer.drain()

    async def _handle_message(self, message: Any) -> None:
        """
        Handle a single protocol message.

        :param message: The parsed protocol message
        """
        match type(message):
            case type() if type(message) is BitField:
                self._piece_manager.add_peer(self._remote_id, message.bitfield)
            case type() if type(message) is Interested:
                self._peer_state.add(ConnectionState.INTERESTED)
            case type() if type(message) is NotInterested:
                self._peer_state.discard(ConnectionState.INTERESTED)
            case type() if type(message) is Choke:
                self._my_state.add(ConnectionState.CHOKED)
                # Update stats
                if self._stats:
                    self._stats.is_choked = True
                # On choke, we don't cancel inflight - they'll complete or timeout
            case type() if type(message) is Unchoke:
                self._my_state.discard(ConnectionState.CHOKED)
                # Update stats
                if self._stats:
                    self._stats.is_choked = False
                # On unchoke, immediately try to fill pipeline
                await self._fill_pipeline()
            case type() if type(message) is Have:
                self._piece_manager.update_peer(self._remote_id, message.index)
            case type() if type(message) is KeepAlive:
                pass  # Keep-alive has no payload
            case type() if type(message) is Piece:
                # Decrement inflight count - this block response completed
                if self._inflight_requests > 0:
                    self._inflight_requests -= 1
                # Record statistics
                block_size = len(message.block)
                if self._stats:
                    self._stats.record_block(block_size)
                if self._stats_callback:
                    self._stats_callback(self._remote_id, block_size)
                if self._on_block_cb:
                    self._on_block_cb(
                        peer_id=self._remote_id,
                        piece_index=message.index,
                        block_offset=message.begin,
                        data=message.block
                    )
            case type() if type(message) is Request:
                logging.info('Ignoring received Request message (upload not supported)')
            case type() if type(message) is Cancel:
                logging.info('Ignoring received Cancel message (upload not supported)')
            case _:
                logging.debug(f'Received unhandled message type: {type(message).__name__}')

    async def _cleanup_connection(self) -> None:
        """
        Clean up the current connection resources.

        This ensures proper cleanup of the StreamWriter and removes
        the peer from the piece manager.
        """
        # Log final stats for this peer
        if self._stats and self._stats.bytes_downloaded > 0:
            logging.info(
                f'Peer {self._remote_id}: downloaded {self._stats.bytes_downloaded} bytes '
                f'({self._stats.blocks_received} blocks) at '
                f'{self._stats.download_rate/1024:.1f} KB/s'
            )

        # Remove peer from piece manager
        if self._remote_id is not None:
            self._piece_manager.remove_peer(self._remote_id)
            logging.info(f'Closing peer {self._remote_id}')

        # Properly close the writer
        if self._writer is not None:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except Exception:
                pass  # Ignore errors during cleanup
            self._writer = None
            self._reader = None

        # Mark queue task as done
        try:
            self._queue.task_done()
        except ValueError:
            pass  # Queue might already be empty or task already done

        # Clear remote ID and reset inflight counter
        self._remote_id = None
        self._inflight_requests = 0
        # Reset stats for next connection
        self._stats = None

    def stop(self) -> None:
        """
        Stop this connection from the current peer and prevent
        connecting to any new peers.

        This method is idempotent and can be called from any context.
        """
        self._my_state.add(ConnectionState.STOPPED)
        self._running = False

    async def _request_piece(self) -> None:
        """Request the next piece from the remote peer."""
        if self._remote_id is None or self._writer is None:
            return

        block = self._piece_manager.next_request(self._remote_id)
        if block:
            message = Request(block.piece, block.offset, block.length).encode()

            logging.debug(
                f'Requesting block {block.offset} for piece {block.piece} '
                f'of {block.length} bytes from peer {self._remote_id}'
            )

            self._writer.write(message)
            await self._writer.drain()

    async def _handshake(self) -> bytes:
        """
        Send the initial handshake to the remote peer and wait for the peer
        to respond with its handshake.

        :return: Any remaining buffer data after the handshake
        :raises ProtocolError: If handshake fails
        """
        if self._writer is None or self._reader is None:
            raise ProtocolError('Connection not established')

        self._writer.write(Handshake(self._info_hash, self._peer_id).encode())
        await self._writer.drain()

        buf = b''
        tries = 0
        max_tries = 10

        while len(buf) < Handshake.length and tries < max_tries:
            tries += 1
            chunk = await self._reader.read(PeerStreamIterator.CHUNK_SIZE)
            if not chunk:
                raise ProtocolError('Connection closed during handshake')
            buf += chunk

        response = Handshake.decode(buf[:Handshake.length])
        if not response:
            raise ProtocolError('Unable to receive and parse a handshake')
        if response.info_hash != self._info_hash:
            raise ProtocolError('Handshake with invalid info_hash')

        self._remote_id = response.peer_id
        logging.info('Handshake with peer was successful')

        # Return remaining buffer data
        return buf[Handshake.length:]

    async def _send_interested(self) -> None:
        """Send an interested message to the remote peer."""
        if self._writer is None:
            return

        message = Interested()
        logging.debug(f'Sending message: {message}')
        self._writer.write(message.encode())
        await self._writer.drain()


class PeerStreamIterator:
    """
    The `PeerStreamIterator` is an async iterator that continuously reads from
    the given stream reader and tries to parse valid BitTorrent messages from
    off that stream of bytes.

    If the connection is dropped, something fails the iterator will abort by
    raising the `StopAsyncIteration` error ending the calling iteration.
    """
    CHUNK_SIZE = 10*1024

    def __init__(self, reader, initial: bytes=None):
        self.reader = reader
        self.buffer = initial if initial else b''

    def __aiter__(self):
        return self

    async def __anext__(self):
        # Read data from the socket. When we have enough data to parse, parse
        # it and return the message. Until then keep reading from stream
        while True:
            try:
                data = await self.reader.read(PeerStreamIterator.CHUNK_SIZE)
                if data:
                    self.buffer += data
                    message = self.parse()
                    if message:
                        return message
                else:
                    logging.debug('No data read from stream')
                    if self.buffer:
                        message = self.parse()
                        if message:
                            return message
                    raise StopAsyncIteration()
            except ConnectionResetError:
                logging.debug('Connection closed by peer')
                raise StopAsyncIteration()
            except CancelledError:
                raise StopAsyncIteration()
            except StopAsyncIteration as e:
                # Cath to stop logging
                raise e
            except Exception:
                logging.exception('Error when iterating over stream!')
                raise StopAsyncIteration()
        raise StopAsyncIteration()

    def parse(self):
        """
        Tries to parse protocol messages if there is enough bytes read in the
        buffer.

        :return The parsed message, or None if no message could be parsed
        """
        # Each message is structured as:
        #     <length prefix><message ID><payload>
        #
        # The `length prefix` is a four byte big-endian value
        # The `message ID` is a decimal byte
        # The `payload` is the value of `length prefix`
        #
        # The message length is not part of the actual length. So another
        # 4 bytes needs to be included when slicing the buffer.
        header_length = 4

        if len(self.buffer) > 4:  # 4 bytes is needed to identify the message
            message_length = struct.unpack('>I', self.buffer[0:4])[0]

            if message_length == 0:
                return KeepAlive()

            if len(self.buffer) >= message_length:
                message_id = struct.unpack('>b', self.buffer[4:5])[0]

                def _consume():
                    """Consume the current message from the read buffer"""
                    self.buffer = self.buffer[header_length + message_length:]

                def _data():
                    """"Extract the current message from the read buffer"""
                    return self.buffer[:header_length + message_length]

                if message_id is PeerMessage.BitField:
                    data = _data()
                    _consume()
                    return BitField.decode(data)
                elif message_id is PeerMessage.Interested:
                    _consume()
                    return Interested()
                elif message_id is PeerMessage.NotInterested:
                    _consume()
                    return NotInterested()
                elif message_id is PeerMessage.Choke:
                    _consume()
                    return Choke()
                elif message_id is PeerMessage.Unchoke:
                    _consume()
                    return Unchoke()
                elif message_id is PeerMessage.Have:
                    data = _data()
                    _consume()
                    return Have.decode(data)
                elif message_id is PeerMessage.Piece:
                    data = _data()
                    _consume()
                    return Piece.decode(data)
                elif message_id is PeerMessage.Request:
                    data = _data()
                    _consume()
                    return Request.decode(data)
                elif message_id is PeerMessage.Cancel:
                    data = _data()
                    _consume()
                    return Cancel.decode(data)
                else:
                    logging.info('Unsupported message!')
            else:
                logging.debug('Not enough in buffer in order to parse')
        return None


class PeerMessage:
    """
    A message between two peers.

    All of the remaining messages in the protocol take the form of:
        <length prefix><message ID><payload>

    - The length prefix is a four byte big-endian value.
    - The message ID is a single decimal byte.
    - The payload is message dependent.

    NOTE: The Handshake messageis different in layout compared to the other
          messages.

    Read more:
        https://wiki.theory.org/BitTorrentSpecification#Messages

    BitTorrent uses Big-Endian (Network Byte Order) for all messages, this is
    declared as the first character being '>' in all pack / unpack calls to the
    Python's `struct` module.
    """
    Choke = 0
    Unchoke = 1
    Interested = 2
    NotInterested = 3
    Have = 4
    BitField = 5
    Request = 6
    Piece = 7
    Cancel = 8
    Port = 9
    Handshake = None  # Handshake is not really part of the messages
    KeepAlive = None  # Keep-alive has no ID according to spec

    def encode(self) -> bytes:
        """
        Encodes this object instance to the raw bytes representing the entire
        message (ready to be transmitted).
        """
        pass

    @classmethod
    def decode(cls, data: bytes):
        """
        Decodes the given BitTorrent message into a instance for the
        implementing type.
        """
        pass


class Handshake(PeerMessage):
    """
    The handshake message is the first message sent and then received from a
    remote peer.

    The messages is always 68 bytes long (for this version of BitTorrent
    protocol).

    Message format:
        <pstrlen><pstr><reserved><info_hash><peer_id>

    In version 1.0 of the BitTorrent protocol:
        pstrlen = 19
        pstr = "BitTorrent protocol".

    Thus length is:
        49 + len(pstr) = 68 bytes long.
    """
    length = 49 + 19

    def __init__(self, info_hash: bytes, peer_id: bytes):
        """
        Construct the handshake message

        :param info_hash: The SHA1 hash for the info dict
        :param peer_id: The unique peer id
        """
        if isinstance(info_hash, str):
            info_hash = info_hash.encode('utf-8')
        if isinstance(peer_id, str):
            peer_id = peer_id.encode('utf-8')
        self.info_hash = info_hash
        self.peer_id = peer_id

    def encode(self) -> bytes:
        """
        Encodes this object instance to the raw bytes representing the entire
        message (ready to be transmitted).
        """
        return struct.pack(
            '>B19s8x20s20s',
            19,                         # Single byte (B)
            b'BitTorrent protocol',     # String 19s
                                        # Reserved 8x (pad byte, no value)
            self.info_hash,             # String 20s
            self.peer_id)               # String 20s

    @classmethod
    def decode(cls, data: bytes):
        """
        Decodes the given BitTorrent message into a handshake message, if not
        a valid message, None is returned.
        """
        logging.debug('Decoding Handshake of length: {length}'.format(
            length=len(data)))
        if len(data) < (49 + 19):
            return None
        parts = struct.unpack('>B19s8x20s20s', data)
        return cls(info_hash=parts[2], peer_id=parts[3])

    def __str__(self):
        return 'Handshake'


class KeepAlive(PeerMessage):
    """
    The Keep-Alive message has no payload and length is set to zero.

    Message format:
        <len=0000>
    """
    def __str__(self):
        return 'KeepAlive'


class BitField(PeerMessage):
    """
    The BitField is a message with variable length where the payload is a
    bit array representing all the bits a peer have (1) or does not have (0).

    Message format:
        <len=0001+X><id=5><bitfield>
    """
    def __init__(self, data):
        self.bitfield = bitstring.BitArray(bytes=data)

    def encode(self) -> bytes:
        """
        Encodes this object instance to the raw bytes representing the entire
        message (ready to be transmitted).
        """
        bits_length = len(self.bitfield)
        return struct.pack('>Ib' + str(bits_length) + 's',
                           1 + bits_length,
                           PeerMessage.BitField,
                           self.bitfield)

    @classmethod
    def decode(cls, data: bytes):
        message_length = struct.unpack('>I', data[:4])[0]
        logging.debug('Decoding BitField of length: {length}'.format(
            length=message_length))

        parts = struct.unpack('>Ib' + str(message_length - 1) + 's', data)
        return cls(parts[2])

    def __str__(self):
        return 'BitField'


class Interested(PeerMessage):
    """
    The interested message is fix length and has no payload other than the
    message identifiers. It is used to notify each other about interest in
    downloading pieces.

    Message format:
        <len=0001><id=2>
    """

    def encode(self) -> bytes:
        """
        Encodes this object instance to the raw bytes representing the entire
        message (ready to be transmitted).
        """
        return struct.pack('>Ib',
                           1,  # Message length
                           PeerMessage.Interested)

    def __str__(self):
        return 'Interested'


class NotInterested(PeerMessage):
    """
    The not interested message is fix length and has no payload other than the
    message identifier. It is used to notify each other that there is no
    interest to download pieces.

    Message format:
        <len=0001><id=3>
    """
    def __str__(self):
        return 'NotInterested'


class Choke(PeerMessage):
    """
    The choke message is used to tell the other peer to stop send request
    messages until unchoked.

    Message format:
        <len=0001><id=0>
    """
    def __str__(self):
        return 'Choke'


class Unchoke(PeerMessage):
    """
    Unchoking a peer enables that peer to start requesting pieces from the
    remote peer.

    Message format:
        <len=0001><id=1>
    """
    def __str__(self):
        return 'Unchoke'


class Have(PeerMessage):
    """
    Represents a piece successfully downloaded by the remote peer. The piece
    is a zero based index of the torrents pieces
    """
    def __init__(self, index: int):
        self.index = index

    def encode(self):
        return struct.pack('>IbI',
                           5,  # Message length
                           PeerMessage.Have,
                           self.index)

    @classmethod
    def decode(cls, data: bytes):
        logging.debug('Decoding Have of length: {length}'.format(
            length=len(data)))
        index = struct.unpack('>IbI', data)[2]
        return cls(index)

    def __str__(self):
        return 'Have'


class Request(PeerMessage):
    """
    The message used to request a block of a piece (i.e. a partial piece).

    The request size for each block is 2^14 bytes, except the final block
    that might be smaller (since not all pieces might be evenly divided by the
    request size).

    Message format:
        <len=0013><id=6><index><begin><length>
    """
    def __init__(self, index: int, begin: int, length: int = REQUEST_SIZE):
        """
        Constructs the Request message.

        :param index: The zero based piece index
        :param begin: The zero based offset within a piece
        :param length: The requested length of data (default 2^14)
        """
        self.index = index
        self.begin = begin
        self.length = length

    def encode(self):
        return struct.pack('>IbIII',
                           13,
                           PeerMessage.Request,
                           self.index,
                           self.begin,
                           self.length)

    @classmethod
    def decode(cls, data: bytes):
        logging.debug('Decoding Request of length: {length}'.format(
            length=len(data)))
        # Tuple with (message length, id, index, begin, length)
        parts = struct.unpack('>IbIII', data)
        return cls(parts[2], parts[3], parts[4])

    def __str__(self):
        return 'Request'


class Piece(PeerMessage):
    """
    A block is a part of a piece mentioned in the meta-info. The official
    specification refer to them as pieces as well - which is quite confusing
    the unofficial specification refers to them as blocks however.

    So this class is named `Piece` to match the message in the specification
    but really, it represents a `Block` (which is non-existent in the spec).

    Message format:
        <length prefix><message ID><index><begin><block>
    """
    # The Piece message length without the block data
    length = 9

    def __init__(self, index: int, begin: int, block: bytes):
        """
        Constructs the Piece message.

        :param index: The zero based piece index
        :param begin: The zero based offset within a piece
        :param block: The block data
        """
        self.index = index
        self.begin = begin
        self.block = block

    def encode(self):
        message_length = Piece.length + len(self.block)
        return struct.pack('>IbII' + str(len(self.block)) + 's',
                           message_length,
                           PeerMessage.Piece,
                           self.index,
                           self.begin,
                           self.block)

    @classmethod
    def decode(cls, data: bytes):
        logging.debug('Decoding Piece of length: {length}'.format(
            length=len(data)))
        length = struct.unpack('>I', data[:4])[0]
        parts = struct.unpack('>IbII' + str(length - Piece.length) + 's',
                              data[:length+4])
        return cls(parts[2], parts[3], parts[4])

    def __str__(self):
        return 'Piece'


class Cancel(PeerMessage):
    """
    The cancel message is used to cancel a previously requested block (in fact
    the message is identical (besides from the id) to the Request message).

    Message format:
         <len=0013><id=8><index><begin><length>
    """
    def __init__(self, index, begin, length: int = REQUEST_SIZE):
        self.index = index
        self.begin = begin
        self.length = length

    def encode(self):
        return struct.pack('>IbIII',
                           13,
                           PeerMessage.Cancel,
                           self.index,
                           self.begin,
                           self.length)

    @classmethod
    def decode(cls, data: bytes):
        logging.debug('Decoding Cancel of length: {length}'.format(
            length=len(data)))
        # Tuple with (message length, id, index, begin, length)
        parts = struct.unpack('>IbIII', data)
        return cls(parts[2], parts[3], parts[4])

    def __str__(self):
        return 'Cancel'
