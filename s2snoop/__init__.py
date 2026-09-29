"""s2snoop: observe OpenAI Realtime voice sessions live."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("s2snoop")  # single source of truth: pyproject.toml
except PackageNotFoundError:  # running from a source tree without install
    __version__ = "0+unknown"
