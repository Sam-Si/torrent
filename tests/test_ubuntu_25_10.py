import unittest
from pieces.torrent import Torrent

class Ubuntu2510TorrentTests(unittest.TestCase):
    def setUp(self):
        self.t = Torrent('tests/data/ubuntu-25.10-desktop-amd64.iso.torrent')

    def test_instantiate(self):
        self.assertIsNotNone(self.t)

    def test_is_single_file(self):
        self.assertFalse(self.t.multi_file)

    def test_announce(self):
        # Modern Ubuntu trackers often use https
        self.assertEqual(
            'https://torrent.ubuntu.com/announce', self.t.announce)

    def test_file(self):
        self.assertEqual(1, len(self.t.files))
        self.assertIn('ubuntu-25.10-desktop-amd64.iso', self.t.files[0].name)
        # Ubuntu 25.10 Desktop is around 6GB
        self.assertGreater(self.t.total_size, 5000000000)

    def test_pieces(self):
        # The test output showed 262144 (256KB)
        self.assertEqual(self.t.piece_length, 262144)
        self.assertGreater(len(self.t.pieces), 0)
