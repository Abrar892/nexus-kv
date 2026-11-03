"""Core in-memory data store with TTL expiration and LRU/LFU eviction policies."""

from __future__ import annotations

import fnmatch
import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple


class EvictionPolicy(str, Enum):
    NOEVICTION = "noeviction"
    ALLKEYS_LRU = "allkeys-lru"
    VOLATILE_LRU = "volatile-lru"
    ALLKEYS_LFU = "allkeys-lfu"
    VOLATILE_LFU = "volatile-lfu"
    VOLATILE_TTL = "volatile-ttl"


@dataclass
class DataEntry:
    """Internal container wrapping values with expiration and access metrics."""
    value: Any
    data_type: str
    created_at: float
    expires_at: Optional[float] = None
    last_accessed: float = 0.0
    access_count: int = 1

    def is_expired(self, now: Optional[float] = None) -> bool:
        if self.expires_at is None:
            return False
        current_time = now if now is not None else time.time()
        return current_time >= self.expires_at

    def touch(self, now: Optional[float] = None) -> None:
        self.last_accessed = now if now is not None else time.time()
        self.access_count += 1


class MemoryStore:
    """Thread-safe key-value data storage engine with TTL and eviction."""

    def __init__(
        self,
        max_keys: int = 0,
        max_memory_bytes: int = 0,
        eviction_policy: EvictionPolicy = EvictionPolicy.ALLKEYS_LRU,
    ):
        self.max_keys = max_keys
        self.max_memory_bytes = max_memory_bytes
        self.eviction_policy = eviction_policy
        self._store: Dict[str, DataEntry] = {}
        self._lock = threading.RLock()

        # Operational metrics
        self.hits = 0
        self.misses = 0
        self.evictions = 0
        self.expired_keys = 0

    def get_entry(self, key: str) -> Optional[DataEntry]:
        """Fetch entry, automatically purging if expired. Updates access metrics."""
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                self.misses += 1
                return None

            now = time.time()
            if entry.is_expired(now):
                del self._store[key]
                self.expired_keys += 1
                self.misses += 1
                return None

            entry.touch(now)
            self.hits += 1
            return entry

    def set_entry(
        self,
        key: str,
        value: Any,
        data_type: str,
        ttl_seconds: Optional[float] = None,
    ) -> None:
        """Store or update entry with optional TTL, triggering eviction if capacity reached."""
        with self._lock:
            now = time.time()
            expires_at = (now + ttl_seconds) if ttl_seconds is not None else None

            # Check if key is new and max_keys reached
            if key not in self._store and self.max_keys > 0 and len(self._store) >= self.max_keys:
                self._evict()

            entry = DataEntry(
                value=value,
                data_type=data_type,
                created_at=now,
                expires_at=expires_at,
                last_accessed=now,
                access_count=1,
            )
            self._store[key] = entry

    def delete(self, *keys: str) -> int:
        """Delete one or more keys. Returns number of keys deleted."""
        with self._lock:
            deleted = 0
            for k in keys:
                if k in self._store:
                    del self._store[k]
                    deleted += 1
            return deleted

    def exists(self, *keys: str) -> int:
        """Return count of keys that exist and are unexpired."""
        with self._lock:
            count = 0
            for k in keys:
                if self.get_entry(k) is not None:
                    count += 1
            return count

    def expire(self, key: str, seconds: float) -> bool:
        """Set relative expiration in seconds for an existing key."""
        with self._lock:
            entry = self.get_entry(key)
            if entry is None:
                return False
            entry.expires_at = time.time() + seconds
            return True

    def expire_at(self, key: str, timestamp: float) -> bool:
        """Set absolute UNIX timestamp expiration for an existing key."""
        with self._lock:
            entry = self.get_entry(key)
            if entry is None:
                return False
            entry.expires_at = timestamp
            return True

    def ttl(self, key: str) -> int:
        """
        Return remaining TTL in seconds.
        Returns -2 if key does not exist.
        Returns -1 if key exists with no expiration.
        """
        with self._lock:
            entry = self.get_entry(key)
            if entry is None:
                return -2
            if entry.expires_at is None:
                return -1
            rem = int(entry.expires_at - time.time())
            return max(0, rem)

    def persist(self, key: str) -> bool:
        """Remove expiration from an existing key."""
        with self._lock:
            entry = self.get_entry(key)
            if entry is None or entry.expires_at is None:
                return False
            entry.expires_at = None
            return True

    def dbsize(self) -> int:
        """Return total active key count."""
        with self._lock:
            self._purge_expired()
            return len(self._store)

    def flushdb(self) -> None:
        """Remove all keys from the database."""
        with self._lock:
            self._store.clear()

    def keys(self, pattern: str = "*") -> List[str]:
        """Return list of unexpired keys matching glob pattern."""
        with self._lock:
            self._purge_expired()
            return [k for k in self._store if fnmatch.fnmatch(k, pattern)]

    def _purge_expired(self) -> None:
        """Scan and evict all expired keys."""
        now = time.time()
        expired = [k for k, v in self._store.items() if v.is_expired(now)]
        for k in expired:
            del self._store[k]
        self.expired_keys += len(expired)

    def _evict(self) -> None:
        """Execute configured eviction policy to free space."""
        if self.eviction_policy == EvictionPolicy.NOEVICTION:
            raise MemoryError("OOM: max_keys reached and policy is noeviction")

        # First, purge any already expired keys
        self._purge_expired()
        if len(self._store) < self.max_keys:
            return

        candidates = list(self._store.items())
        if not candidates:
            return

        # Filter volatile candidates if policy requires
        if self.eviction_policy in (
            EvictionPolicy.VOLATILE_LRU,
            EvictionPolicy.VOLATILE_LFU,
            EvictionPolicy.VOLATILE_TTL,
        ):
            candidates = [(k, v) for k, v in candidates if v.expires_at is not None]
            if not candidates:
                # Fall back to all keys if no volatile keys exist
                candidates = list(self._store.items())

        victim_key: Optional[str] = None

        if self.eviction_policy in (EvictionPolicy.ALLKEYS_LRU, EvictionPolicy.VOLATILE_LRU):
            # Oldest last_accessed
            candidates.sort(key=lambda item: item[1].last_accessed)
            victim_key = candidates[0][0]

        elif self.eviction_policy in (EvictionPolicy.ALLKEYS_LFU, EvictionPolicy.VOLATILE_LFU):
            # Lowest access_count
            candidates.sort(key=lambda item: item[1].access_count)
            victim_key = candidates[0][0]

        elif self.eviction_policy == EvictionPolicy.VOLATILE_TTL:
            # Shortest remaining TTL
            candidates.sort(
                key=lambda item: item[1].expires_at if item[1].expires_at is not None else float("inf")
            )
            victim_key = candidates[0][0]

        if victim_key and victim_key in self._store:
            del self._store[victim_key]
            self.evictions += 1
