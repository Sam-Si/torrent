# pieces (macOS Edition)

An experimental BitTorrent client implemented in Python 3 using asyncio.

This fork has been modernized to run on **macOS** using **Python 3.13** and **aiohttp 3.x**.

The client is not a practical BitTorrent client, it lacks too many
features to really be useful. It was implemented for fun in order to
learn more about BitTorrent as well as Python's asyncio library.

## Getting started (macOS)

### Prerequisites

You need Python 3.13 installed. You can install it via Homebrew:

```sh
brew install python@3.13
```

### Installation

Install the needed dependencies and run the unit tests with:

```sh
python3 -m venv venv
source venv/bin/activate
pip install -r requirements_macos.txt
```

### Running Tests

To run the unit tests:

```sh
python3 -m unittest
```

### Usage

In order to download a torrent file, run this command:

```sh
python3 pieces.py -v tests/data/ubuntu-25.10-desktop-amd64.iso.torrent
```

If everything goes well, your torrent should be downloaded and the
program terminated. You can stop the client using `Ctrl + C`.

## Design considerations

The purpose with implementing this client was to learn `asyncio` (and other Python 3.5 features, such as _type hinting_)
together with the BitTorrent protocol.

## Known issues (Python 3.13 updates)

* **Asyncio Loop:** Modernized to use `asyncio.run()` and `asyncio.create_task()`.
* **Signal Handling:** Fixed for macOS using `loop.add_signal_handler(signal.SIGINT, ...)`.
* **aiohttp:** Upgraded from v0.22 to v3.13 to support Python 3.13.

# License

The client is released under the Apache v2 license, see LICENCE.
