"""Tests for RESP v2 protocol serialization and parsing."""

import unittest

from nexus_kv.resp import RespParser, RespProtocolError, RespSerializer


class TestResp(unittest.TestCase):
    def test_simple_string(self):
        encoded = RespSerializer.simple_string("OK")
        self.assertEqual(encoded, b"+OK\r\n")
        val, consumed = RespParser.parse(encoded)
        self.assertEqual(val, "OK")
        self.assertEqual(consumed, len(encoded))

    def test_error(self):
        encoded = RespSerializer.error("ERR invalid syntax")
        self.assertEqual(encoded, b"-ERR invalid syntax\r\n")
        val, consumed = RespParser.parse(encoded)
        self.assertIsInstance(val, RespProtocolError)
        self.assertEqual(str(val), "ERR invalid syntax")

    def test_integer(self):
        encoded = RespSerializer.integer(1048576)
        self.assertEqual(encoded, b":1048576\r\n")
        val, consumed = RespParser.parse(encoded)
        self.assertEqual(val, 1048576)

    def test_bulk_string(self):
        encoded = RespSerializer.bulk_string("foobar")
        self.assertEqual(encoded, b"$6\r\nfoobar\r\n")
        val, consumed = RespParser.parse(encoded)
        self.assertEqual(val, "foobar")

        # Null bulk string
        null_encoded = RespSerializer.bulk_string(None)
        self.assertEqual(null_encoded, b"$-1\r\n")
        val_null, consumed = RespParser.parse(null_encoded)
        self.assertIsNone(val_null)

    def test_array(self):
        items = ["SET", "foo", "bar"]
        encoded = RespSerializer.array(items)
        val, consumed = RespParser.parse(encoded)
        self.assertEqual(val, items)

        # Null array
        val_null, _ = RespParser.parse(b"*-1\r\n")
        self.assertIsNone(val_null)

    def test_inline_command(self):
        inline_buf = b"PING\r\n"
        val, consumed = RespParser.parse(inline_buf)
        self.assertEqual(val, ["PING"])
        self.assertEqual(consumed, len(inline_buf))

        inline_args = b"SET mykey 12345\r\n"
        val, consumed = RespParser.parse(inline_args)
        self.assertEqual(val, ["SET", "mykey", "12345"])

    def test_incomplete_buffer(self):
        # Buffer ending prematurely
        val, consumed = RespParser.parse(b"$10\r\nshort")
        self.assertIsNone(val)
        self.assertEqual(consumed, 0)


if __name__ == "__main__":
    unittest.main()
