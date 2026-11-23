"""raftkv: a distributed key-value store in Python, built on Raft consensus."""

from .raft import RaftNode
from .store import KVStore
from .watch import WatchManager

__all__ = ["RaftNode", "KVStore", "WatchManager"]
__version__ = "0.12.0"
