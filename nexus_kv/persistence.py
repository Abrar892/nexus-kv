"""Append-Only File (AOF) persistence engine with log compaction."""

from __future__ import annotations

import os
import threading
import time
from typing import Any, Callable, List, Optional

from .resp import RespParser, RespSerializer
from .storage import MemoryStore


class AOFEngine:
    """Manages disk persistence, mutation logging, replay, and AOF compaction."""

    def __init__(self, filepath: str = "nexus.aof", sync_policy: str = "everysec"):
        self.filepath = filepath
        self.sync_policy = sync_policy  # "always", "everysec", "no"
        self._lock = threading.Lock()
        self._file = None
        self._running = True

        os.makedirs(os.path.dirname(os.path.abspath(filepath)), exist_ok=True)
        self._file = open(self.filepath, "a+b")

        if self.sync_policy == "everysec":
            self._sync_thread = threading.Thread(target=self._periodic_flush, daemon=True)
            self._sync_thread.start()

    def append(self, cmd_tokens: List[str]) -> None:
        """Serialize and append mutation command in RESP format to the AOF log."""
        data = RespSerializer.array(cmd_tokens)
        with self._lock:
            if self._file and not self._file.closed:
                self._file.write(data)
                if self.sync_policy == "always":
                    self._file.flush()
                    os.fsync(self._file.fileno())

    def replay(self, executor: Callable[[List[str]], Any]) -> int:
        """Replay all commands from the AOF file to reconstruct in-memory state."""
        if not os.path.exists(self.filepath):
            return 0

        with open(self.filepath, "rb") as f:
            content = f.read()

        offset = 0
        replayed_count = 0
        while offset < len(content):
            parsed, consumed = RespParser.parse(content[offset:])
            if consumed == 0 or parsed is None:
                break
            offset += consumed

            if isinstance(parsed, list) and parsed:
                cmd_tokens = [str(x) for x in parsed]
                try:
                    executor(cmd_tokens)
                    replayed_count += 1
                except Exception:
                    pass

        return replayed_count

    def rewrite(self, store: MemoryStore) -> None:
        """
        Compacts the AOF log by serializing the current snapshot of live keys.
        Replaces the old log with a fresh, minimal sequence of reconstruction commands.
        """
        tmp_filepath = f"{self.filepath}.tmp"
        with open(tmp_filepath, "wb") as tmp_f:
            with store._lock:
                now = time.time()
                for key, entry in store._store.items():
                    if entry.is_expired(now):
                        continue

                    data_type = entry.data_type
                    val = entry.value

                    # Generate minimal reconstruction command
                    if data_type == "string":
                        tmp_f.write(RespSerializer.array(["SET", key, str(val)]))
                    elif data_type == "list":
                        if val:
                            tmp_f.write(RespSerializer.array(["RPUSH", key] + [str(x) for x in val]))
                    elif data_type == "set":
                        if val:
                            tmp_f.write(RespSerializer.array(["SADD", key] + [str(x) for x in val]))
                    elif data_type == "hash":
                        if val:
                            hargs = ["HSET", key]
                            for hk, hv in val.items():
                                hargs.extend([str(hk), str(hv)])
                            tmp_f.write(RespSerializer.array(hargs))
                    elif data_type == "zset":
                        if val:
                            zargs = ["ZADD", key]
                            for zk, zv in val.items():
                                zargs.extend([str(zv), str(zk)])
                            tmp_f.write(RespSerializer.array(zargs))

                    # If key has expiration, append EXPIREAT
                    if entry.expires_at is not None:
                        tmp_f.write(RespSerializer.array(["EXPIREAT", key, str(int(entry.expires_at))]))

            tmp_f.flush()
            os.fsync(tmp_f.fileno())

        # Safely swap files under lock
        with self._lock:
            if self._file and not self._file.closed:
                self._file.close()

            # Atomic replace (works on Windows & POSIX in Python 3.3+)
            if os.path.exists(self.filepath):
                os.replace(tmp_filepath, self.filepath)
            else:
                os.rename(tmp_filepath, self.filepath)

            self._file = open(self.filepath, "a+b")

    def _periodic_flush(self) -> None:
        """Background daemon thread performing fsync every second for 'everysec' policy."""
        while self._running:
            time.sleep(1.0)
            with self._lock:
                if self._file and not self._file.closed:
                    try:
                        self._file.flush()
                        os.fsync(self._file.fileno())
                    except Exception:
                        pass

    def close(self) -> None:
        """Flush buffers and close file descriptor."""
        self._running = False
        with self._lock:
            if self._file and not self._file.closed:
                self._file.flush()
                try:
                    os.fsync(self._file.fileno())
                except Exception:
                    pass
                self._file.close()
