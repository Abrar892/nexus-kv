"""Tests for in-memory storage, TTL expiration, and eviction policies."""

import time
import unittest

from nexus_kv.storage import EvictionPolicy, MemoryStore


class TestStorage(unittest.TestCase):
    def test_basic_set_get_delete(self):
        store = MemoryStore()
        store.set_entry("k1", "v1", "string")
        self.assertEqual(store.dbsize(), 1)
        self.assertEqual(store.exists("k1"), 1)

        entry = store.get_entry("k1")
        self.assertIsNotNone(entry)
        self.assertEqual(entry.value, "v1")

        deleted = store.delete("k1")
        self.assertEqual(deleted, 1)
        self.assertIsNone(store.get_entry("k1"))
        self.assertEqual(store.dbsize(), 0)

    def test_ttl_expiration(self):
        store = MemoryStore()
        store.set_entry("short_lived", "data", "string", ttl_seconds=0.1)
        self.assertEqual(store.ttl("short_lived"), 0)
        self.assertIsNotNone(store.get_entry("short_lived"))

        # Sleep past expiration
        time.sleep(0.15)
        self.assertIsNone(store.get_entry("short_lived"))
        self.assertEqual(store.ttl("short_lived"), -2)

    def test_persist_removes_expiration(self):
        store = MemoryStore()
        store.set_entry("k2", "val", "string", ttl_seconds=100.0)
        self.assertGreater(store.ttl("k2"), 0)

        persisted = store.persist("k2")
        self.assertTrue(persisted)
        self.assertEqual(store.ttl("k2"), -1)

    def test_eviction_allkeys_lru(self):
        # Store with max 2 keys, LRU policy
        store = MemoryStore(max_keys=2, eviction_policy=EvictionPolicy.ALLKEYS_LRU)
        store.set_entry("a", "1", "string")
        store.set_entry("b", "2", "string")

        # Access "a" to make "b" the least recently used
        store.get_entry("a")

        # Insert "c", should evict "b"
        store.set_entry("c", "3", "string")
        self.assertEqual(store.dbsize(), 2)
        self.assertIsNotNone(store.get_entry("a"))
        self.assertIsNone(store.get_entry("b"))
        self.assertIsNotNone(store.get_entry("c"))
        self.assertEqual(store.evictions, 1)

    def test_eviction_allkeys_lfu(self):
        store = MemoryStore(max_keys=2, eviction_policy=EvictionPolicy.ALLKEYS_LFU)
        store.set_entry("k1", "val1", "string")
        store.set_entry("k2", "val2", "string")

        # Touch k1 multiple times to increase access count
        store.get_entry("k1")
        store.get_entry("k1")

        # Inserting k3 should evict lowest frequency key (k2)
        store.set_entry("k3", "val3", "string")
        self.assertIsNotNone(store.get_entry("k1"))
        self.assertIsNone(store.get_entry("k2"))
        self.assertIsNotNone(store.get_entry("k3"))

    def test_eviction_noeviction_error(self):
        store = MemoryStore(max_keys=1, eviction_policy=EvictionPolicy.NOEVICTION)
        store.set_entry("first", "data", "string")
        with self.assertRaises(MemoryError):
            store.set_entry("second", "overflow", "string")


if __name__ == "__main__":
    unittest.main()
