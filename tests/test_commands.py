"""Tests for command dispatcher across Strings, Lists, Sets, Hashes, and ZSets."""

import unittest

from nexus_kv.commands import CommandDispatcher
from nexus_kv.resp import RespParser
from nexus_kv.storage import MemoryStore


class TestCommands(unittest.TestCase):
    def setUp(self):
        self.store = MemoryStore()
        self.dispatcher = CommandDispatcher(self.store)

    def exec(self, *tokens: str):
        raw_bytes, is_mutation = self.dispatcher.execute(list(tokens))
        val, _ = RespParser.parse(raw_bytes)
        return val

    def test_strings_operations(self):
        # SET & GET
        res = self.exec("SET", "name", "Nexus")
        self.assertEqual(res, "OK")
        self.assertEqual(self.exec("GET", "name"), "Nexus")

        # INCR & INCRBY
        self.assertEqual(self.exec("INCR", "counter"), 1)
        self.assertEqual(self.exec("INCRBY", "counter", "10"), 11)
        self.assertEqual(self.exec("DECR", "counter"), 10)

        # MSET & MGET
        self.assertEqual(self.exec("MSET", "k1", "v1", "k2", "v2"), "OK")
        self.assertEqual(self.exec("MGET", "k1", "k2", "missing"), ["v1", "v2", None])

        # APPEND & STRLEN
        self.assertEqual(self.exec("APPEND", "name", "-DB"), 8)
        self.assertEqual(self.exec("STRLEN", "name"), 8)

    def test_lists_operations(self):
        self.assertEqual(self.exec("RPUSH", "tasks", "task1", "task2"), 2)
        self.assertEqual(self.exec("LPUSH", "tasks", "task0"), 3)
        self.assertEqual(self.exec("LRANGE", "tasks", "0", "-1"), ["task0", "task1", "task2"])
        self.assertEqual(self.exec("LPOP", "tasks"), "task0")
        self.assertEqual(self.exec("RPOP", "tasks"), "task2")
        self.assertEqual(self.exec("LLEN", "tasks"), 1)

    def test_sets_operations(self):
        self.assertEqual(self.exec("SADD", "fruits", "apple", "banana", "apple"), 2)
        self.assertEqual(self.exec("SCARD", "fruits"), 2)
        self.assertEqual(self.exec("SISMEMBER", "fruits", "apple"), 1)
        self.assertEqual(self.exec("SISMEMBER", "fruits", "orange"), 0)
        self.assertEqual(self.exec("SMEMBERS", "fruits"), ["apple", "banana"])

        self.exec("SADD", "setA", "1", "2", "3")
        self.exec("SADD", "setB", "2", "3", "4")
        self.assertEqual(self.exec("SINTER", "setA", "setB"), ["2", "3"])
        self.assertEqual(self.exec("SDIFF", "setA", "setB"), ["1"])
        self.assertEqual(self.exec("SUNION", "setA", "setB"), ["1", "2", "3", "4"])

    def test_hashes_operations(self):
        self.assertEqual(self.exec("HSET", "user:100", "name", "Bob", "role", "admin"), 2)
        self.assertEqual(self.exec("HGET", "user:100", "name"), "Bob")
        self.assertEqual(self.exec("HEXISTS", "user:100", "role"), 1)
        self.assertEqual(self.exec("HLEN", "user:100"), 2)
        self.assertEqual(self.exec("HINCRBY", "user:100", "score", "50"), 50)
        self.assertEqual(self.exec("HDEL", "user:100", "role"), 1)
        self.assertIsNone(self.exec("HGET", "user:100", "role"))

    def test_zsets_operations(self):
        self.assertEqual(self.exec("ZADD", "leaderboard", "100", "Alice", "250", "Bob", "50", "Charlie"), 3)
        self.assertEqual(self.exec("ZCARD", "leaderboard"), 3)
        self.assertEqual(self.exec("ZSCORE", "leaderboard", "Bob"), "250")
        # ZRANGE sorted by score ascending
        self.assertEqual(self.exec("ZRANGE", "leaderboard", "0", "-1"), ["Charlie", "Alice", "Bob"])
        # With scores
        self.assertEqual(self.exec("ZRANGE", "leaderboard", "0", "1", "WITHSCORES"), ["Charlie", "50", "Alice", "100"])


if __name__ == "__main__":
    unittest.main()
