"""Interactive command-line interface client for NexusKV / Redis servers."""

from __future__ import annotations

import argparse
import socket
import sys
from typing import Any, List, Optional

from .resp import RespParser, RespSerializer


class NexusClient:
    """Lightweight zero-dependency Redis-compatible TCP client."""

    def __init__(self, host: str = "127.0.0.1", port: int = 6380, timeout: float = 5.0):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.sock: Optional[socket.socket] = None
        self._connect()

    def _connect(self) -> None:
        self.sock = socket.create_connection((self.host, self.port), timeout=self.timeout)

    def execute_command(self, *args: str) -> Any:
        """Send command to server and return parsed RESP response."""
        if not self.sock:
            self._connect()

        payload = RespSerializer.array(list(args))
        self.sock.sendall(payload)

        # Read response
        buf = bytearray()
        while True:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionResetError("Connection closed by server")
            buf.extend(chunk)
            parsed, consumed = RespParser.parse(bytes(buf))
            if consumed > 0 and parsed is not None:
                return parsed

    def close(self) -> None:
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None


def format_output(val: Any) -> str:
    if val is None:
        return "(nil)"
    if isinstance(val, bool):
        return "(integer) 1" if val else "(integer) 0"
    if isinstance(val, int):
        return f"(integer) {val}"
    if isinstance(val, str):
        return f'"{val}"'
    if isinstance(val, list):
        if not val:
            return "(empty list or set)"
        lines = []
        for i, item in enumerate(val, 1):
            lines.append(f"{i}) {format_output(item)}")
        return "\n".join(lines)
    if isinstance(val, Exception):
        return f"(error) {val}"
    return str(val)


def repl(host: str, port: int) -> None:
    print(f"Connected to NexusKV at {host}:{port}")
    print("Type 'help' for hints, 'quit' or 'exit' to terminate.\n")
    try:
        client = NexusClient(host, port)
    except Exception as err:
        print(f"Could not connect to {host}:{port}: {err}", file=sys.stderr)
        return

    prompt = f"{host}:{port}> "
    while True:
        try:
            line = input(prompt).strip()
        except (KeyboardInterrupt, EOFError):
            print("\nGoodbye!")
            break

        if not line:
            continue
        if line.lower() in ("quit", "exit"):
            break
        if line.lower() == "help":
            print("Supported commands: GET, SET, DEL, LPUSH, LPOP, SADD, SMEMBERS, HSET, HGET, ZADD, ZRANGE, MULTI, EXEC, INFO, PING, etc.")
            continue

        parts = line.split()
        try:
            res = client.execute_command(*parts)
            print(format_output(res))
        except Exception as err:
            print(f"(error) {err}")

    client.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="NexusKV Interactive Command-Line Client")
    parser.add_argument("-h", "--host", default="127.0.0.1", help="Server host (default: 127.0.0.1)")
    parser.add_argument("-p", "--port", type=int, default=6380, help="Server port (default: 6380)")
    parser.add_argument("command", nargs="*", help="Command and arguments to execute (omit for REPL)")

    args = parser.parse_args()
    if args.command:
        try:
            client = NexusClient(args.host, args.port)
            res = client.execute_command(*args.command)
            print(format_output(res))
            client.close()
        except Exception as err:
            print(f"(error) {err}", file=sys.stderr)
            sys.exit(1)
    else:
        repl(args.host, args.port)


if __name__ == "__main__":
    main()
