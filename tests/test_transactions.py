"""Tests for transaction engine: MULTI, EXEC, DISCARD."""

import unittest

from nexus_kv.commands import CommandDispatcher
from nexus_kv.resp import RespParser
from nexus_kv.storage import MemoryStore
from nexus_kv.transactions import TransactionState


class TestTransactions(unittest.TestCase):
    def setUp(self):
        self.store = MemoryStore()
        self.dispatcher = CommandDispatcher(self.store)
        self.tx = TransactionState()

    def test_multi_exec_atomic_flow(self):
        # Start transaction
        start_res = self.tx.start()
        val, _ = RespParser.parse(start_res)
        self.assertEqual(val, "OK")
        self.assertTrue(self.tx.is_active)

        # Queue commands
        q1 = self.tx.queue(["SET", "a", "100"])
        q2 = self.tx.queue(["INCRBY", "a", "50"])
        q3 = self.tx.queue(["GET", "a"])

        self.assertEqual(RespParser.parse(q1)[0], "QUEUED")
        self.assertEqual(RespParser.parse(q2)[0], "QUEUED")
        self.assertEqual(RespParser.parse(q3)[0], "QUEUED")

        # Key 'a' should not exist yet before EXEC
        self.assertEqual(self.store.exists("a"), 0)

        # Execute transaction
        exec_bytes = self.tx.execute(self.dispatcher.execute)
        results, _ = RespParser.parse(exec_bytes)
        self.assertEqual(results, ["OK", 150, "150"])
        self.assertFalse(self.tx.is_active)

        # Value persisted in store
        self.assertEqual(self.store.get_entry("a").value, "150")

    def test_multi_discard(self):
        self.tx.start()
        self.tx.queue(["SET", "temp", "val"])
        discard_bytes = self.tx.discard()
        val, _ = RespParser.parse(discard_bytes)
        self.assertEqual(val, "OK")
        self.assertFalse(self.tx.is_active)
        self.assertEqual(self.store.exists("temp"), 0)

    def test_exec_without_multi_fails(self):
        exec_bytes = self.tx.execute(self.dispatcher.execute)
        val, _ = RespParser.parse(exec_bytes)
        self.assertIn("ERR EXEC without MULTI", str(val))


if __name__ == "__main__":
    unittest.main()
