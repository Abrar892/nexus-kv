"""Transaction engine providing MULTI, EXEC, and DISCARD atomicity."""

from __future__ import annotations

from typing import Any, Callable, List, Optional, Tuple

from .resp import RespSerializer


class TransactionState:
    """Per-client transaction session state."""

    def __init__(self):
        self.is_active = False
        self.queued_commands: List[List[str]] = []

    def start(self) -> bytes:
        if self.is_active:
            return RespSerializer.error("ERR MULTI calls can not be nested")
        self.is_active = True
        self.queued_commands.clear()
        return RespSerializer.simple_string("OK")

    def queue(self, cmd_tokens: List[str]) -> bytes:
        self.queued_commands.append(cmd_tokens)
        return RespSerializer.simple_string("QUEUED")

    def discard(self) -> bytes:
        if not self.is_active:
            return RespSerializer.error("ERR DISCARD without MULTI")
        self.is_active = False
        self.queued_commands.clear()
        return RespSerializer.simple_string("OK")

    def execute(self, executor: Callable[[List[str]], Tuple[bytes, bool]]) -> bytes:
        """Execute all queued commands and return RESP array of serialized outputs."""
        if not self.is_active:
            return RespSerializer.error("ERR EXEC without MULTI")

        commands = list(self.queued_commands)
        self.is_active = False
        self.queued_commands.clear()

        results_bytes: List[bytes] = []
        for cmd in commands:
            resp_bytes, _ = executor(cmd)
            results_bytes.append(resp_bytes)

        # Build raw array header
        header = f"*{len(results_bytes)}\r\n".encode("utf-8")
        return header + b"".join(results_bytes)
