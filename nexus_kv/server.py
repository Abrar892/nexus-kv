"""Multi-threaded TCP server implementing the Redis wire protocol."""

from __future__ import annotations

import argparse
import socket
import sys
import threading
import time
import uuid
from typing import Any, List, Optional, Tuple

from .commands import CommandDispatcher
from .persistence import AOFEngine
from .pubsub import PubSubHub
from .resp import RespParser, RespSerializer
from .storage import EvictionPolicy, MemoryStore
from .transactions import TransactionState


class ClientSession:
    """Manages individual TCP client lifecycle, transaction state, and pub/sub mode."""

    def __init__(self, conn: socket.socket, addr: Tuple[str, int], server: NexusServer):
        self.conn = conn
        self.addr = addr
        self.server = server
        self.client_id = f"client_{uuid.uuid4().hex[:8]}"
        self.tx = TransactionState()
        self.is_subscribed = False
        self._lock = threading.Lock()

    def send_raw(self, data: bytes) -> None:
        """Send raw bytes over the client connection safely."""
        with self._lock:
            try:
                self.conn.sendall(data)
            except Exception:
                pass

    def run(self) -> None:
        """Main client loop processing streaming RESP input buffer."""
        buffer = bytearray()
        try:
            while self.server.running:
                try:
                    chunk = self.conn.recv(4096)
                except (socket.timeout, TimeoutError):
                    continue

                if not chunk:
                    break

                buffer.extend(chunk)

                # Process all complete commands in buffer
                while buffer:
                    parsed, consumed = RespParser.parse(bytes(buffer))
                    if consumed == 0 or parsed is None:
                        break

                    del buffer[:consumed]

                    if isinstance(parsed, list):
                        tokens = [str(x) for x in parsed]
                    elif isinstance(parsed, str):
                        tokens = parsed.split()
                    else:
                        tokens = [str(parsed)]

                    if not tokens:
                        continue

                    resp_bytes = self._handle_command(tokens)
                    if resp_bytes:
                        self.send_raw(resp_bytes)

        except Exception:
            pass
        finally:
            self._cleanup()

    def _handle_command(self, tokens: List[str]) -> Optional[bytes]:
        cmd = tokens[0].upper()
        args = tokens[1:]

        # 1. Pub/Sub Special Commands
        if cmd == "SUBSCRIBE":
            if not args:
                return RespSerializer.error("ERR wrong number of arguments for 'subscribe' command")
            self.is_subscribed = True
            for ch in args:
                sub_count = self.server.pubsub.subscribe(self.client_id, ch, self.send_raw)
                self.send_raw(RespSerializer.array(["subscribe", ch, sub_count]))
            return None

        if cmd == "UNSUBSCRIBE":
            channels = args if args else []
            if not channels:
                self.server.pubsub.unsubscribe_all(self.client_id)
                self.is_subscribed = False
                return RespSerializer.array(["unsubscribe", None, 0])
            for ch in channels:
                rem = self.server.pubsub.unsubscribe(self.client_id, ch)
                self.send_raw(RespSerializer.array(["unsubscribe", ch, rem]))
            return None

        if cmd == "PUBLISH":
            if len(args) != 2:
                return RespSerializer.error("ERR wrong number of arguments for 'publish' command")
            ch, msg = args[0], args[1]
            recipients = self.server.pubsub.publish(ch, msg)
            return RespSerializer.integer(recipients)

        # In subscriber mode, clients may only call SUBSCRIBE, UNSUBSCRIBE, PING, QUIT
        if self.is_subscribed and cmd not in ("PING", "QUIT"):
            return RespSerializer.error(
                f"ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context"
            )

        # 2. Transaction Commands
        if cmd == "MULTI":
            return self.tx.start()

        if cmd == "DISCARD":
            return self.tx.discard()

        if cmd == "EXEC":
            return self.tx.execute(self._execute_with_aof)

        if self.tx.is_active:
            # Queue command inside active transaction
            return self.tx.queue(tokens)

        # 3. Server Administrative Commands
        if cmd == "BGREWRITEAOF":
            if self.server.aof:
                threading.Thread(target=self.server.aof.rewrite, args=(self.server.store,), daemon=True).start()
                return RespSerializer.simple_string("Background append only file rewriting started")
            return RespSerializer.error("ERR AOF is not enabled")

        if cmd == "QUIT":
            self.send_raw(RespSerializer.simple_string("OK"))
            self.conn.close()
            return None

        # 4. Standard Command Execution
        resp_bytes, _ = self._execute_with_aof(tokens)
        return resp_bytes

    def _execute_with_aof(self, tokens: List[str]) -> Tuple[bytes, bool]:
        resp_bytes, is_mutation = self.server.dispatcher.execute(tokens)
        if is_mutation and self.server.aof:
            self.server.aof.append(tokens)
        return resp_bytes, is_mutation

    def _cleanup(self) -> None:
        self.server.pubsub.unsubscribe_all(self.client_id)
        try:
            self.conn.close()
        except Exception:
            pass


class NexusServer:
    """Threaded TCP Server hosting the NexusKV database instance."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6380,
        max_keys: int = 0,
        eviction_policy: str = "allkeys-lru",
        aof_path: Optional[str] = None,
        aof_sync: str = "everysec",
    ):
        self.host = host
        self.port = port
        self.policy = EvictionPolicy(eviction_policy)
        self.store = MemoryStore(max_keys=max_keys, eviction_policy=self.policy)
        self.dispatcher = CommandDispatcher(self.store)
        self.pubsub = PubSubHub()
        self.aof = AOFEngine(aof_path, sync_policy=aof_sync) if aof_path else None

        self.running = False
        self._server_sock: Optional[socket.socket] = None
        self._accept_thread: Optional[threading.Thread] = None

        # Replay persisted log if present
        if self.aof:
            replayed = self.aof.replay(self.dispatcher.execute)
            if replayed > 0:
                print(f"[NexusKV] Replayed {replayed} commands from AOF log: {aof_path}")

    def start(self, block: bool = False) -> None:
        """Start listening for incoming TCP connections."""
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind((self.host, self.port))
        self.port = self._server_sock.getsockname()[1]
        self._server_sock.listen(128)
        self._server_sock.settimeout(1.0)
        self.running = True

        print(f"[NexusKV] Server running at {self.host}:{self.port} (Policy: {self.policy.value})")

        self._accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._accept_thread.start()

        if block:
            try:
                while self.running:
                    time.sleep(1.0)
            except KeyboardInterrupt:
                self.stop()

    def _accept_loop(self) -> None:
        while self.running:
            try:
                conn, addr = self._server_sock.accept()
                conn.settimeout(0.5)
                session = ClientSession(conn, addr, self)
                t = threading.Thread(target=session.run, daemon=True)
                t.start()
            except (socket.timeout, TimeoutError):
                continue
            except Exception:
                if not self.running:
                    break

    def stop(self) -> None:
        """Gracefully terminate server and flush persistence."""
        self.running = False
        if self._server_sock:
            try:
                self._server_sock.close()
            except Exception:
                pass
        if self.aof:
            self.aof.close()
        print("[NexusKV] Server shutdown completed.")


def main() -> None:
    parser = argparse.ArgumentParser(description="NexusKV - In-memory key-value server with Redis protocol")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host (default: 127.0.0.1)")
    parser.add_argument("-p", "--port", type=int, default=6380, help="Bind port (default: 6380)")
    parser.add_argument("--max-keys", type=int, default=0, help="Maximum key capacity (0 for unlimited)")
    parser.add_argument(
        "--policy",
        choices=["noeviction", "allkeys-lru", "volatile-lru", "allkeys-lfu", "volatile-lfu", "volatile-ttl"],
        default="allkeys-lru",
        help="Memory eviction policy",
    )
    parser.add_argument("--aof", default=None, help="Path to AOF persistence file (optional)")
    parser.add_argument("--aof-sync", choices=["always", "everysec", "no"], default="everysec", help="AOF fsync mode")

    args = parser.parse_args()
    server = NexusServer(
        host=args.host,
        port=args.port,
        max_keys=args.max_keys,
        eviction_policy=args.policy,
        aof_path=args.aof,
        aof_sync=args.aof_sync,
    )
    server.start(block=True)


if __name__ == "__main__":
    main()
