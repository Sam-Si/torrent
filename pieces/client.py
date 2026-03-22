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
import math
import os
import time
from asyncio import Queue
from collections import namedtuple, defaultdict
from hashlib import sha1
from typing import Any

from pieces.protocol import PeerConnection, PeerStats, REQUEST_SIZE
from pieces.tracker import Tracker

# The number of max peer connections per TorrentClient
MAX_PEER_CONNECTIONS = 40

# Peer rotation interval in seconds
# Periodically disconnect slowest peer to try potentially faster ones
PEER_ROTATION_INTERVAL = 30.0

# Minimum peers before rotation kicks in
MIN_PEERS_FOR_ROTATION = 10


class TorrentClient:
    """
    The torrent client is the local peer that holds peer-to-peer
    connections to download and upload pieces for a given torrent.

    Once started, the client makes periodic announce calls to the tracker
    registered in the torrent meta-data. These calls results in a list of
    peers that should be tried in order to exchange pieces.

    Each received peer is kept in a queue that a pool of PeerConnection
    objects consume. There is a fix number of PeerConnections that can have
    a connection open to a peer. Since we are not creating expensive threads
    (or worse yet processes) we can create them all at once and they will
    be waiting until there is a peer to consume in the queue.

    This class uses modern asyncio features (Python 3.11+) including:
    - TaskGroup for structured concurrency
    - Event-driven signaling for efficient waiting
    - Peer statistics tracking and ranking
    - Optimistic peer rotation to discover faster peers
    """
    def __init__(self, torrent):
        self.tracker = Tracker(torrent)
        # The list of potential peers is the work queue, consumed by the
        # PeerConnections
        self.available_peers = Queue()
        # The list of peer connection objects (workers)
        self._peer_connections: list[PeerConnection] = []
        # The piece manager implements the strategy on which pieces to
        # request, as well as the logic to persist received pieces to disk.
        self.piece_manager = PieceManager(torrent)
        self.abort = False
        # Event-driven signaling for completion and abort
        # These events allow the main loop to wait efficiently instead of polling
        self._completed_event: asyncio.Event | None = None
        self._abort_event: asyncio.Event | None = None
        # Peer statistics tracking for ranking
        self._peer_stats: dict[bytes, PeerStats] = {}
        # Track last peer rotation time
        self._last_rotation_time: float = 0.0

    def _on_peer_stats(self, peer_id: bytes, bytes_received: int) -> None:
        """
        Callback for peer statistics updates.

        :param peer_id: The peer that sent data
        :param bytes_received: Number of bytes received
        """
        # Stats are tracked in PeerConnection._stats
        # This callback is for additional processing if needed
        pass

    def _get_slowest_peer(self) -> PeerConnection | None:
        """
        Find the peer connection with the lowest score.

        Used for peer rotation to disconnect slow peers.

        :return: The slowest PeerConnection, or None if no suitable peer
        """
        candidates = []
        for conn in self._peer_connections:
            if conn.stats is None:
                continue
            # Don't rotate peers that are actively downloading
            if conn.stats.download_rate > 0:
                candidates.append(conn)
        
        if not candidates:
            return None
        
        # Return the peer with the lowest score
        return min(candidates, key=lambda c: c.stats.score if c.stats else float('-inf'))

    def _get_peer_rankings(self) -> list[tuple[bytes, float]]:
        """
        Get a ranked list of peers by their scores.

        :return: List of (peer_id, score) tuples, sorted by score descending
        """
        rankings = []
        for conn in self._peer_connections:
            if conn.stats:
                rankings.append((conn._remote_id, conn.stats.score))
        return sorted(rankings, key=lambda x: x[1], reverse=True)

    async def _rotate_peers(self) -> None:
        """
        Optimistic peer rotation: disconnect the slowest peer to try a new one.

        This implements the "optimistic unchoke" concept from BitTorrent spec:
        periodically try new peers to discover potentially faster connections.

        Only rotates if:
        - There are peers waiting in the queue
        - There's an active peer to rotate (not all are equally slow)
        """
        # Check if there are peers waiting to be tried
        if self.available_peers.empty():
            return

        # Find the slowest active peer
        slowest = self._get_slowest_peer()
        if slowest is None:
            return

        # Log current peer rankings
        rankings = self._get_peer_rankings()
        if rankings:
            logging.debug(f'Peer rankings: {rankings[:5]}')  # Top 5

        # Disconnect the slowest peer
        if slowest.stats:
            logging.info(
                f'Rotating peer {slowest._remote_id}: '
                f'{slowest.stats.download_rate/1024:.1f} KB/s'
            )
        slowest.stop()

    async def start(self) -> None:
        """
        Start downloading the torrent held by this client.

        This results in connecting to the tracker to retrieve the list of
        peers to communicate with. Once the torrent is fully downloaded or
        if the download is aborted this method will complete.

        Uses TaskGroup for structured concurrency of peer connections.
        """
        # Initialize events for event-driven signaling
        self._completed_event = asyncio.Event()
        self._abort_event = asyncio.Event()

        # Create peer connection objects (but don't start them yet)
        self._peer_connections = [
            PeerConnection(
                self.available_peers,
                self.tracker.torrent.info_hash,
                self.tracker.peer_id,
                self.piece_manager,
                self._on_block_retrieved,
                self._on_peer_stats  # Stats callback
            )
            for _ in range(MAX_PEER_CONNECTIONS)
        ]

        # The time we last made an announce call (timestamp)
        previous: float | None = None
        # Default interval between announce calls (in seconds)
        interval = 30 * 60
        # Track last peer rotation time
        self._last_rotation_time = time.time()

        # Use TaskGroup for structured concurrency (Python 3.11+)
        async with asyncio.TaskGroup() as tg:
            # Start all peer connection workers
            for peer_conn in self._peer_connections:
                tg.create_task(self._run_peer_connection(peer_conn))

            # Main control loop
            while True:
                # Check completion/abort status
                if self.piece_manager.complete:
                    logging.info('Torrent fully downloaded!')
                    break
                if self.abort:
                    logging.info('Aborting download...')
                    break

                current = time.time()
                
                # Peer rotation: periodically disconnect slowest peer to try faster ones
                if current - self._last_rotation_time > PEER_ROTATION_INTERVAL:
                    self._last_rotation_time = current
                    await self._rotate_peers()
                
                if (not previous) or (previous + interval < current):
                    response = await self.tracker.connect(
                        first=previous if previous else False,
                        uploaded=self.piece_manager.bytes_uploaded,
                        downloaded=self.piece_manager.bytes_downloaded)

                    if response:
                        previous = current
                        interval = response.interval
                        self._empty_queue()
                        for peer in response.peers:
                            self.available_peers.put_nowait(peer)
                else:
                    # Calculate time until next tracker announce
                    time_until_announce = (previous + interval) - current

                    # Wait for either:
                    # - Download completion
                    # - Abort request
                    # - Timeout (time to check tracker again)
                    check_interval = min(5.0, time_until_announce)

                    try:
                        # Use asyncio.wait with FIRST_COMPLETED to wait for any event
                        completed, _ = await asyncio.wait(
                            [
                                asyncio.create_task(self._completed_event.wait()),
                                asyncio.create_task(self._abort_event.wait())
                            ],
                            timeout=check_interval,
                            return_when=asyncio.FIRST_COMPLETED
                        )
                        # Cancel any remaining tasks
                        for task in _:
                            task.cancel()
                    except Exception:
                        # Events might be set, continue to check conditions
                        pass

            # Stop all peer connections before exiting TaskGroup
            for peer_conn in self._peer_connections:
                peer_conn.stop()

        # TaskGroup ensures all tasks are complete before exiting
        await self.stop()

    async def _run_peer_connection(self, peer_conn: PeerConnection) -> None:
        """
        Run a single peer connection with proper error handling.

        This wrapper ensures that exceptions in peer connections don't
        crash the entire download.

        :param peer_conn: The PeerConnection to run
        """
        try:
            await peer_conn.run()
        except asyncio.CancelledError:
            logging.debug('Peer connection cancelled')
            raise
        except Exception as e:
            logging.exception(f'Peer connection error: {e}')

    def _empty_queue(self) -> None:
        """Empty the peer queue."""
        while not self.available_peers.empty():
            self.available_peers.get_nowait()

    async def stop(self) -> None:
        """
        Stop the download or seeding process.

        Ensures all peer connections are stopped and resources are cleaned up.
        """
        self.abort = True
        # Signal abort event to wake up the main loop if it's waiting
        if self._abort_event is not None:
            self._abort_event.set()

        # Stop all peer connections
        for peer_conn in self._peer_connections:
            peer_conn.stop()

        # Close piece manager and tracker
        self.piece_manager.close()
        await self.tracker.close()

    def _on_block_retrieved(self, peer_id, piece_index, block_offset, data):
        """
        Callback function called by the `PeerConnection` when a block is
        retrieved from a peer.

        :param peer_id: The id of the peer the block was retrieved from
        :param piece_index: The piece index this block is a part of
        :param block_offset: The block offset within its piece
        :param data: The binary data retrieved
        """
        self.piece_manager.block_received(
            peer_id=peer_id, piece_index=piece_index,
            block_offset=block_offset, data=data)
        # Signal completion event if download is complete
        if self.piece_manager.complete and self._completed_event is not None:
            self._completed_event.set()


class Block:
    """
    The block is a partial piece, this is what is requested and transferred
    between peers.

    A block is most often of the same size as the REQUEST_SIZE, except for the
    final block which might (most likely) is smaller than REQUEST_SIZE.
    """
    Missing = 0
    Pending = 1
    Retrieved = 2

    __slots__ = ('piece', 'offset', 'length', 'status', 'data')

    def __init__(self, piece: int, offset: int, length: int):
        self.piece = piece
        self.offset = offset
        self.length = length
        self.status = Block.Missing
        self.data: bytes | None = None


class Piece:
    """
    The piece is a part of of the torrents content. Each piece except the final
    piece for a torrent has the same length (the final piece might be shorter).

    A piece is what is defined in the torrent meta-data. However, when sharing
    data between peers a smaller unit is used - this smaller piece is refereed
    to as `Block` by the unofficial specification (the official specification
    uses piece for this one as well, which is slightly confusing).
    """
    __slots__ = ('index', 'blocks', '_blocks_by_offset', 'hash')

    def __init__(self, index: int, blocks: list[Block], hash_value: bytes):
        self.index = index
        self.blocks = blocks
        self.hash = hash_value
        # Efficient O(1) block lookup by offset
        self._blocks_by_offset: dict[int, Block] = {
            b.offset: b for b in blocks
        }

    def reset(self) -> None:
        """
        Reset all blocks to Missing regardless of current state.
        """
        for block in self.blocks:
            block.status = Block.Missing

    def next_request(self) -> Block | None:
        """
        Get the next Block to be requested.

        Uses generator for memory efficiency - no temporary list created.
        """
        for block in self.blocks:
            if block.status is Block.Missing:
                block.status = Block.Pending
                return block
        return None

    def block_received(self, offset: int, data: bytes) -> None:
        """
        Update block information that the given block is now received.

        Uses O(1) dict lookup instead of O(n) list search.

        :param offset: The block offset (within the piece)
        :param data: The block data
        """
        block = self._blocks_by_offset.get(offset)
        if block:
            block.status = Block.Retrieved
            block.data = data
        else:
            logging.warning(f'Trying to complete a non-existing block {offset}')

    def is_complete(self) -> bool:
        """
        Checks if all blocks for this piece are retrieved (regardless of SHA1).

        Uses generator with all() for memory efficiency.

        :return: True or False
        """
        return all(b.status is Block.Retrieved for b in self.blocks)

    def clear_block_data(self) -> None:
        """
        Clear block data to free memory after piece is written to disk.

        This is critical for memory efficiency - without this, all downloaded
        data would remain in RAM until the program exits.
        """
        for block in self.blocks:
            block.data = None

    def is_hash_matching(self) -> bool:
        """
        Check if a SHA1 hash for all the received blocks match the piece hash
        from the torrent meta-info.

        :return: True or False
        """
        piece_hash = sha1(self.data).digest()
        return self.hash == piece_hash

    @property
    def data(self) -> bytes:
        """
        Return the data for this piece (by concatenating all blocks in order).

        NOTE: This method does not control that all blocks are valid or even
        existing!
        """
        # Blocks are already in offset order, no need to sort
        return b''.join(b.data for b in self.blocks if b.data)

# The type used for keeping track of pending request that can be re-issued
PendingRequest = namedtuple('PendingRequest', ['block', 'added'])


class PieceSelectionStrategy:
    """
    Abstract base class for piece selection strategies.

    Piece selection strategies determine which piece to request next from
    a peer. Different strategies optimize for different use cases:
    - Rarest-first: Maximize piece availability across the swarm
    - Sequential: Enable streaming/sequential playback
    - Random: Simple load balancing
    """

    def select_piece(
        self,
        missing_pieces: list['Piece'],
        ongoing_pieces: list['Piece'],
        peers: dict,
        peer_id: bytes
    ) -> 'Piece | None':
        """
        Select the next piece to download for a given peer.

        :param missing_pieces: List of pieces not yet started
        :param ongoing_pieces: List of pieces currently being downloaded
        :param peers: Dict mapping peer_id to their bitfield
        :param peer_id: The peer to select a piece for
        :return: The selected Piece, or None if no suitable piece exists
        """
        raise NotImplementedError


class SequentialStrategy(PieceSelectionStrategy):
    """
    Sequential piece selection strategy.

    Downloads pieces in order from first to last. This is useful for:
    - Streaming media files
    - Sequential file access patterns
    - Preview/playback while downloading

    Note: This strategy may reduce overall download speed as it doesn't
    consider piece rarity.
    """

    def select_piece(
        self,
        missing_pieces: list['Piece'],
        ongoing_pieces: list['Piece'],
        peers: dict,
        peer_id: bytes
    ) -> 'Piece | None':
        """
        Select the first available piece that the peer has.

        Uses min() with generator for O(n) time, O(1) extra space.
        Avoids sorting which would be O(n log n) and allocate a list.

        :return: The first missing piece the peer has, or None
        """
        if peer_id not in peers:
            return None

        bitfield = peers[peer_id]
        # Use generator with min() - O(n) time, O(1) extra space
        # No temporary list created (unlike sorted())
        return min(
            (p for p in missing_pieces if bitfield[p.index]),
            key=lambda p: p.index,
            default=None
        )


class RarestFirstStrategy(PieceSelectionStrategy):
    """
    Rarest-first piece selection strategy.

    Downloads the piece that the fewest peers have first. This is the
    default BitTorrent strategy and helps:
    - Maintain high piece availability in the swarm
    - Prevent "last piece problem" where rare pieces stall downloads
    - Distribute load across available pieces

    This is the recommended strategy for most use cases.
    """

    def select_piece(
        self,
        missing_pieces: list['Piece'],
        ongoing_pieces: list['Piece'],
        peers: dict,
        peer_id: bytes
    ) -> 'Piece | None':
        """
        Select the rarest piece that the peer has.

        :return: The rarest missing piece the peer has, or None
        """
        if peer_id not in peers:
            return None

        bitfield = peers[peer_id]
        piece_count: dict['Piece', int] = defaultdict(int)

        # Count how many peers have each piece
        for piece in missing_pieces:
            if not bitfield[piece.index]:
                continue
            for p in peers:
                if peers[p][piece.index]:
                    piece_count[piece] += 1

        if not piece_count:
            return None

        # Return the piece with the lowest count (rarest)
        return min(piece_count, key=lambda p: piece_count[p])


class RandomStrategy(PieceSelectionStrategy):
    """
    Random piece selection strategy.

    Selects a random piece from the available pieces. This provides:
    - Simple load balancing across pieces
    - No preference for any particular piece
    - Useful for testing or when other strategies aren't appropriate

    Note: This strategy may not be optimal for swarm health.
    """

    def __init__(self, seed: int | None = None):
        """
        Initialize the random strategy.

        :param seed: Optional seed for reproducible random selection
        """
        import random
        self._random = random.Random(seed)

    def select_piece(
        self,
        missing_pieces: list['Piece'],
        ongoing_pieces: list['Piece'],
        peers: dict,
        peer_id: bytes
    ) -> 'Piece | None':
        """
        Select a random piece that the peer has.

        :return: A random missing piece the peer has, or None
        """
        if peer_id not in peers:
            return None

        bitfield = peers[peer_id]
        available = [p for p in missing_pieces if bitfield[p.index]]

        if not available:
            return None

        return self._random.choice(available)


class PieceManager:
    """
    The PieceManager is responsible for keeping track of all the available
    pieces for the connected peers as well as the pieces we have available for
    other peers.

    Supports pluggable piece selection strategies for different download
    patterns (sequential, rarest-first, random).

    The default strategy is rarest-first, which is optimal for swarm health.
    """

    def __init__(
        self,
        torrent,
        strategy: PieceSelectionStrategy | None = None
    ):
        """
        Initialize the PieceManager.

        :param torrent: The torrent metadata
        :param strategy: Piece selection strategy (default: RarestFirstStrategy)
        """
        self.torrent = torrent
        self.peers: dict[bytes, Any] = {}
        self.pending_blocks: list[PendingRequest] = []
        self.missing_pieces: list[Piece] = []
        self.ongoing_pieces: list[Piece] = []
        self.have_pieces: list[Piece] = []
        self.max_pending_time = 300 * 1000  # 5 minutes
        self.missing_pieces = self._initiate_pieces()
        self.total_pieces = len(torrent.pieces)
        self.fd = os.open(self.torrent.output_file, os.O_RDWR | os.O_CREAT)

        # Use rarest-first strategy by default
        self._strategy = strategy if strategy is not None else RarestFirstStrategy()

    @property
    def strategy(self) -> PieceSelectionStrategy:
        """Get the current piece selection strategy."""
        return self._strategy

    @strategy.setter
    def strategy(self, value: PieceSelectionStrategy) -> None:
        """Set a new piece selection strategy."""
        self._strategy = value

    def _initiate_pieces(self) -> list[Piece]:
        """
        Pre-construct the list of pieces and blocks based on the number of
        pieces and request size for this torrent.
        """
        torrent = self.torrent
        pieces: list[Piece] = []
        total_pieces = len(torrent.pieces)
        std_piece_blocks = math.ceil(torrent.piece_length / REQUEST_SIZE)

        for index, hash_value in enumerate(torrent.pieces):
            # The number of blocks for each piece can be calculated using the
            # request size as divisor for the piece length.
            # The final piece however, will most likely have fewer blocks
            # than 'regular' pieces, and that final block might be smaller
            # then the other blocks.
            if index < (total_pieces - 1):
                blocks = [Block(index, offset * REQUEST_SIZE, REQUEST_SIZE)
                          for offset in range(std_piece_blocks)]
            else:
                last_length = torrent.total_size % torrent.piece_length
                num_blocks = math.ceil(last_length / REQUEST_SIZE)
                blocks = [Block(index, offset * REQUEST_SIZE, REQUEST_SIZE)
                          for offset in range(num_blocks)]

                if last_length % REQUEST_SIZE > 0:
                    # Last block of the last piece might be smaller than
                    # the ordinary request size.
                    blocks[-1].length = last_length % REQUEST_SIZE
                    
            pieces.append(Piece(index, blocks, hash_value))
        return pieces

    def close(self):
        """
        Close any resources used by the PieceManager (such as open files)
        """
        if self.fd:
            os.close(self.fd)

    @property
    def complete(self):
        """
        Checks whether or not the all pieces are downloaded for this torrent.

        :return: True if all pieces are fully downloaded else False
        """
        return len(self.have_pieces) == self.total_pieces

    @property
    def bytes_downloaded(self) -> int:
        """
        Get the number of bytes downloaded.

        This method Only counts full, verified, pieces, not single blocks.
        """
        return len(self.have_pieces) * self.torrent.piece_length

    @property
    def bytes_uploaded(self) -> int:
        # TODO Add support for sending data
        return 0

    def add_peer(self, peer_id, bitfield):
        """
        Adds a peer and the bitfield representing the pieces the peer has.
        """
        self.peers[peer_id] = bitfield

    def update_peer(self, peer_id, index: int):
        """
        Updates the information about which pieces a peer has (reflects a Have
        message).
        """
        if peer_id in self.peers:
            self.peers[peer_id][index] = 1

    def remove_peer(self, peer_id):
        """
        Tries to remove a previously added peer (e.g. used if a peer connection
        is dropped)
        """
        if peer_id in self.peers:
            del self.peers[peer_id]

    def next_request(self, peer_id) -> Block:
        """
        Get the next Block that should be requested from the given peer.

        Uses the configured piece selection strategy to determine which piece
        to request. The algorithm:

        1. Check for expired requests to re-issue (timeout handling)
        2. Continue downloading ongoing pieces first
        3. Use strategy to select a new piece from missing pieces

        :param peer_id: The peer to request a block from
        :return: The next Block to request, or None if no blocks available
        """
        if peer_id not in self.peers:
            return None

        # 1. Check for expired requests to re-issue
        block = self._expired_requests(peer_id)
        if block:
            return block

        # 2. Continue ongoing pieces first
        block = self._next_ongoing(peer_id)
        if block:
            return block

        # 3. Use strategy to select a new piece
        piece = self._strategy.select_piece(
            self.missing_pieces,
            self.ongoing_pieces,
            self.peers,
            peer_id
        )
        if piece:
            # Move piece from missing to ongoing
            self.missing_pieces.remove(piece)
            self.ongoing_pieces.append(piece)
            block = piece.next_request()
            if block:
                self.pending_blocks.append(
                    PendingRequest(block, int(round(time.time() * 1000))))
            return block

        return None

    def block_received(self, peer_id: bytes, piece_index: int, 
                       block_offset: int, data: bytes) -> None:
        """
        This method must be called when a block has successfully been retrieved
        by a peer.

        Once a full piece have been retrieved, a SHA1 hash control is made. If
        the check fails all the pieces blocks are put back in missing state to
        be fetched again. If the hash succeeds the partial piece is written to
        disk and the piece is indicated as Have.
        """
        logging.debug(f'Received block {block_offset} for piece {piece_index} '
                      f'from peer {peer_id}')

        # Remove from pending requests - use generator for efficiency
        request_key = (piece_index, block_offset)
        for i, request in enumerate(self.pending_blocks):
            if (request.block.piece, request.block.offset) == request_key:
                del self.pending_blocks[i]
                break

        # Find ongoing piece - use generator with next() to avoid list creation
        piece = next(
            (p for p in self.ongoing_pieces if p.index == piece_index),
            None
        )
        
        if piece:
            piece.block_received(block_offset, data)
            if piece.is_complete():
                if piece.is_hash_matching():
                    self._write(piece)
                    self.ongoing_pieces.remove(piece)
                    self.have_pieces.append(piece)
                    complete = (self.total_pieces -
                                len(self.missing_pieces) -
                                len(self.ongoing_pieces))
                    logging.info(
                        f'{complete} / {self.total_pieces} pieces downloaded '
                        f'{(complete/self.total_pieces)*100:.3f}%'
                    )
                else:
                    logging.info(f'Discarding corrupt piece {piece.index}')
                    piece.reset()
        else:
            logging.warning('Trying to update piece that is not ongoing!')

    def _expired_requests(self, peer_id: bytes) -> Block | None:
        """
        Go through previously requested blocks, if any one have been in the
        requested state for longer than `MAX_PENDING_TIME` return the block to
        be re-requested.

        If no pending blocks exist, None is returned
        """
        current = int(round(time.time() * 1000))
        for request in self.pending_blocks:
            if self.peers[peer_id][request.block.piece]:
                if request.added + self.max_pending_time < current:
                    logging.info(
                        f'Re-requesting block {request.block.offset} '
                        f'for piece {request.block.piece}'
                    )
                    # Reset expiration timer
                    request.added = current
                    return request.block
        return None

    def _next_ongoing(self, peer_id: bytes) -> Block | None:
        """
        Go through the ongoing pieces and return the next block to be
        requested or None if no block is left to be requested.
        """
        for piece in self.ongoing_pieces:
            if self.peers[peer_id][piece.index]:
                # Is there any blocks left to request in this piece?
                block = piece.next_request()
                if block:
                    self.pending_blocks.append(
                        PendingRequest(block, int(round(time.time() * 1000))))
                    return block
        return None

    def _write(self, piece: Piece) -> None:
        """
        Write the given piece to disk and clear block data to free memory.
        """
        pos = piece.index * self.torrent.piece_length
        os.lseek(self.fd, pos, os.SEEK_SET)
        os.write(self.fd, piece.data)
        # Critical: Clear block data to free memory after write
        # Without this, all downloaded data would stay in RAM
        piece.clear_block_data()
