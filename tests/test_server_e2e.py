"""End-to-end integration tests over live TCP sockets."""

import time
import unittest

from nexus_kv.cli import NexusClient
from nexus_kv.server import NexusServer


class TestServerE2E(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Start NexusServer on loopback with port 0 (ephemeral port)
        cls.server = NexusServer(host="127.0.0.1", port=0)
        cls.server.start(block=False)
        cls.port = cls.server.port
        # Give server thread moment to bind
        time.sleep(0.1)

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()

    def test_client_server_commands(self):
        client = NexusClient("127.0.0.1", self.port)

        # PING
        self.assertEqual(client.execute_command("PING"), "PONG")

        # SET & GET
        self.assertEqual(client.execute_command("SET", "alpha", "1234"), "OK")
        self.assertEqual(client.execute_command("GET", "alpha"), "1234")

        # INCRBY
        self.assertEqual(client.execute_command("INCRBY", "alpha", "6"), 1240)

        # LIST
        self.assertEqual(client.execute_command("RPUSH", "queue", "a", "b", "c"), 3)
        self.assertEqual(client.execute_command("LPOP", "queue"), "a")

        # HASH
        self.assertEqual(client.execute_command("HSET", "map", "x", "10", "y", "20"), 2)
        self.assertEqual(client.execute_command("HGETALL", "map"), ["x", "10", "y", "20"])

        # DBSIZE
        self.assertGreaterEqual(client.execute_command("DBSIZE"), 3)

        client.close()

    def test_client_multi_exec_transaction(self):
        client = NexusClient("127.0.0.1", self.port)
        self.assertEqual(client.execute_command("MULTI"), "OK")
        self.assertEqual(client.execute_command("SET", "tx_key", "999"), "QUEUED")
        self.assertEqual(client.execute_command("GET", "tx_key"), "QUEUED")

        res = client.execute_command("EXEC")
        self.assertEqual(res, ["OK", "999"])
        client.close()

    def test_client_pubsub_channel(self):
        # Subscriber client
        sub_client = NexusClient("127.0.0.1", self.port)
        # Publisher client
        pub_client = NexusClient("127.0.0.1", self.port)

        # Subscribe
        sub_res = sub_client.execute_command("SUBSCRIBE", "news")
        self.assertEqual(sub_res, ["subscribe", "news", 1])

        # Publish
        recipients = pub_client.execute_command("PUBLISH", "news", "headline-1")
        self.assertEqual(recipients, 1)

        pub_client.close()
        sub_client.close()


if __name__ == "__main__":
    unittest.main()
