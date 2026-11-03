"""Tests for Append-Only File (AOF) persistence and compaction."""

import os
import tempfile
import unittest

from nexus_kv.commands import CommandDispatcher
from nexus_kv.persistence import AOFEngine
from nexus_kv.storage import MemoryStore


class TestPersistence(unittest.TestCase):
    def test_aof_append_and_replay(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            aof_path = os.path.join(tmpdir, "test.aof")
            aof = AOFEngine(filepath=aof_path, sync_policy="always")

            # Append mutation commands
            aof.append(["SET", "greeting", "Hello"])
            aof.append(["RPUSH", "items", "x", "y"])
            aof.append(["HSET", "profile", "age", "30"])
            aof.close()

            # Replay onto fresh MemoryStore
            new_store = MemoryStore()
            dispatcher = CommandDispatcher(new_store)
            replayed = aof.replay(dispatcher.execute)
            self.assertEqual(replayed, 3)

            # Assert state reconstructed accurately
            self.assertEqual(new_store.get_entry("greeting").value, "Hello")
            self.assertEqual(new_store.get_entry("items").value, ["x", "y"])
            self.assertEqual(new_store.get_entry("profile").value, {"age": "30"})

    def test_aof_compaction_rewrite(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            aof_path = os.path.join(tmpdir, "compact.aof")
            aof = AOFEngine(filepath=aof_path, sync_policy="always")

            # Write redundant updates
            for i in range(50):
                aof.append(["SET", "key", f"val_{i}"])

            initial_size = os.path.getsize(aof_path)

            # Create store matching final state
            store = MemoryStore()
            store.set_entry("key", "val_49", "string")

            # Rewrite AOF log
            aof.rewrite(store)
            aof.close()

            compacted_size = os.path.getsize(aof_path)
            self.assertLess(compacted_size, initial_size)

            # Replay compacted AOF
            fresh_store = MemoryStore()
            dispatcher = CommandDispatcher(fresh_store)
            replayed = aof.replay(dispatcher.execute)
            self.assertEqual(replayed, 1)
            self.assertEqual(fresh_store.get_entry("key").value, "val_49")


if __name__ == "__main__":
    unittest.main()
