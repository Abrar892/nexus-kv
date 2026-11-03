"""Command execution dispatcher implementing Redis data structure operations."""

from __future__ import annotations

import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

from .resp import RespProtocolError, RespSerializer
from .storage import DataEntry, MemoryStore


MUTATION_COMMANDS = {
    "SET", "SETEX", "SETNX", "MSET", "DEL", "INCR", "INCRBY", "DECR", "DECRBY",
    "APPEND", "EXPIRE", "EXPIREAT", "PERSIST", "FLUSHDB",
    "LPUSH", "RPUSH", "LPOP", "RPOP",
    "SADD", "SREM",
    "HSET", "HDEL", "HINCRBY",
    "ZADD", "ZREM"
}


class CommandDispatcher:
    """Dispatches Redis-compatible commands against the MemoryStore."""

    def __init__(self, store: MemoryStore):
        self.store = store
        self.started_at = time.time()
        self.total_commands_processed = 0

        # Registry mapping command name (uppercase) to handler method
        self._handlers: Dict[str, Callable[[List[str]], Any]] = {
            # Server & Connection
            "PING": self._cmd_ping,
            "ECHO": self._cmd_echo,
            "INFO": self._cmd_info,
            "DBSIZE": self._cmd_dbsize,
            "FLUSHDB": self._cmd_flushdb,
            "TYPE": self._cmd_type,
            "KEYS": self._cmd_keys,
            "EXISTS": self._cmd_exists,
            "DEL": self._cmd_del,
            "EXPIRE": self._cmd_expire,
            "EXPIREAT": self._cmd_expireat,
            "TTL": self._cmd_ttl,
            "PERSIST": self._cmd_persist,

            # Strings
            "GET": self._cmd_get,
            "SET": self._cmd_set,
            "SETEX": self._cmd_setex,
            "SETNX": self._cmd_setnx,
            "MGET": self._cmd_mget,
            "MSET": self._cmd_mset,
            "INCR": self._cmd_incr,
            "INCRBY": self._cmd_incrby,
            "DECR": self._cmd_decr,
            "DECRBY": self._cmd_decrby,
            "APPEND": self._cmd_append,
            "STRLEN": self._cmd_strlen,

            # Lists
            "LPUSH": self._cmd_lpush,
            "RPUSH": self._cmd_rpush,
            "LPOP": self._cmd_lpop,
            "RPOP": self._cmd_rpop,
            "LRANGE": self._cmd_lrange,
            "LLEN": self._cmd_llen,
            "LINDEX": self._cmd_lindex,

            # Sets
            "SADD": self._cmd_sadd,
            "SREM": self._cmd_srem,
            "SMEMBERS": self._cmd_smembers,
            "SISMEMBER": self._cmd_sismember,
            "SCARD": self._cmd_scard,
            "SDIFF": self._cmd_sdiff,
            "SINTER": self._cmd_sinter,
            "SUNION": self._cmd_sunion,

            # Hashes
            "HSET": self._cmd_hset,
            "HGET": self._cmd_hget,
            "HDEL": self._cmd_hdel,
            "HEXISTS": self._cmd_hexists,
            "HGETALL": self._cmd_hgetall,
            "HKEYS": self._cmd_hkeys,
            "HVALS": self._cmd_hvals,
            "HLEN": self._cmd_hlen,
            "HINCRBY": self._cmd_hincrby,

            # Sorted Sets
            "ZADD": self._cmd_zadd,
            "ZRANGE": self._cmd_zrange,
            "ZSCORE": self._cmd_zscore,
            "ZCARD": self._cmd_zcard,
            "ZREM": self._cmd_zrem,
        }

    def execute(self, tokens: List[str]) -> Tuple[bytes, bool]:
        """
        Execute command tokens and return (serialized_resp_bytes, is_mutation).
        """
        if not tokens:
            return RespSerializer.error("ERR empty command"), False

        cmd_name = tokens[0].upper()
        args = tokens[1:]

        handler = self._handlers.get(cmd_name)
        if not handler:
            return RespSerializer.error(f"ERR unknown command '{cmd_name}'"), False

        self.total_commands_processed += 1
        is_mutation = cmd_name in MUTATION_COMMANDS

        try:
            result = handler(args)
            return RespSerializer.serialize(result), is_mutation
        except (ValueError, TypeError) as err:
            return RespSerializer.error(f"ERR {err}"), False
        except Exception as err:
            return RespSerializer.error(f"ERR {err}"), False

    # --- Connection & Server ---

    def _cmd_ping(self, args: List[str]) -> Any:
        return args[0] if args else RespSerializer.simple_string("PONG")

    def _cmd_echo(self, args: List[str]) -> str:
        if not args:
            raise ValueError("wrong number of arguments for 'echo' command")
        return " ".join(args)

    def _cmd_info(self, args: List[str]) -> str:
        uptime = int(time.time() - self.started_at)
        lines = [
            "# Server",
            "nexus_version:0.1.0",
            f"uptime_in_seconds:{uptime}",
            f"total_commands_processed:{self.total_commands_processed}",
            "# Keyspace",
            f"db0:keys={self.store.dbsize()},hits={self.store.hits},misses={self.store.misses},evictions={self.store.evictions}",
        ]
        return "\r\n".join(lines)

    def _cmd_dbsize(self, args: List[str]) -> int:
        return self.store.dbsize()

    def _cmd_flushdb(self, args: List[str]) -> bytes:
        self.store.flushdb()
        return RespSerializer.simple_string("OK")

    def _cmd_type(self, args: List[str]) -> bytes:
        if not args:
            raise ValueError("wrong number of arguments for 'type' command")
        entry = self.store.get_entry(args[0])
        type_str = entry.data_type if entry else "none"
        return RespSerializer.simple_string(type_str)

    def _cmd_keys(self, args: List[str]) -> List[str]:
        pattern = args[0] if args else "*"
        return self.store.keys(pattern)

    def _cmd_exists(self, args: List[str]) -> int:
        if not args:
            raise ValueError("wrong number of arguments for 'exists' command")
        return self.store.exists(*args)

    def _cmd_del(self, args: List[str]) -> int:
        if not args:
            raise ValueError("wrong number of arguments for 'del' command")
        return self.store.delete(*args)

    def _cmd_expire(self, args: List[str]) -> int:
        if len(args) != 2:
            raise ValueError("wrong number of arguments for 'expire' command")
        success = self.store.expire(args[0], float(args[1]))
        return 1 if success else 0

    def _cmd_expireat(self, args: List[str]) -> int:
        if len(args) != 2:
            raise ValueError("wrong number of arguments for 'expireat' command")
        success = self.store.expire_at(args[0], float(args[1]))
        return 1 if success else 0

    def _cmd_ttl(self, args: List[str]) -> int:
        if not args:
            raise ValueError("wrong number of arguments for 'ttl' command")
        return self.store.ttl(args[0])

    def _cmd_persist(self, args: List[str]) -> int:
        if not args:
            raise ValueError("wrong number of arguments for 'persist' command")
        return 1 if self.store.persist(args[0]) else 0

    # --- Strings ---

    def _cmd_get(self, args: List[str]) -> Optional[str]:
        if not args:
            raise ValueError("wrong number of arguments for 'get' command")
        entry = self.store.get_entry(args[0])
        if entry is None:
            return None
        if entry.data_type != "string":
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
        return entry.value

    def _cmd_set(self, args: List[str]) -> Any:
        if len(args) < 2:
            raise ValueError("wrong number of arguments for 'set' command")
        key = args[0]
        value = args[1]
        ttl: Optional[float] = None
        nx = False
        xx = False

        idx = 2
        while idx < len(args):
            flag = args[idx].upper()
            if flag == "EX" and idx + 1 < len(args):
                ttl = float(args[idx + 1])
                idx += 2
            elif flag == "PX" and idx + 1 < len(args):
                ttl = float(args[idx + 1]) / 1000.0
                idx += 2
            elif flag == "NX":
                nx = True
                idx += 1
            elif flag == "XX":
                xx = True
                idx += 1
            else:
                raise ValueError("syntax error")

        exists = self.store.exists(key) > 0
        if nx and exists:
            return None
        if xx and not exists:
            return None

        self.store.set_entry(key, value, "string", ttl)
        return RespSerializer.simple_string("OK")

    def _cmd_setex(self, args: List[str]) -> bytes:
        if len(args) != 3:
            raise ValueError("wrong number of arguments for 'setex' command")
        key, seconds, val = args[0], float(args[1]), args[2]
        self.store.set_entry(key, val, "string", seconds)
        return RespSerializer.simple_string("OK")

    def _cmd_setnx(self, args: List[str]) -> int:
        if len(args) != 2:
            raise ValueError("wrong number of arguments for 'setnx' command")
        if self.store.exists(args[0]):
            return 0
        self.store.set_entry(args[0], args[1], "string")
        return 1

    def _cmd_mget(self, args: List[str]) -> List[Optional[str]]:
        if not args:
            raise ValueError("wrong number of arguments for 'mget' command")
        results = []
        for k in args:
            entry = self.store.get_entry(k)
            results.append(entry.value if entry and entry.data_type == "string" else None)
        return results

    def _cmd_mset(self, args: List[str]) -> bytes:
        if len(args) < 2 or len(args) % 2 != 0:
            raise ValueError("wrong number of arguments for 'mset' command")
        for i in range(0, len(args), 2):
            self.store.set_entry(args[i], args[i + 1], "string")
        return RespSerializer.simple_string("OK")

    def _cmd_incr(self, args: List[str]) -> int:
        return self._incr_by(args[0], 1)

    def _cmd_incrby(self, args: List[str]) -> int:
        if len(args) != 2:
            raise ValueError("wrong number of arguments for 'incrby' command")
        return self._incr_by(args[0], int(args[1]))

    def _cmd_decr(self, args: List[str]) -> int:
        return self._incr_by(args[0], -1)

    def _cmd_decrby(self, args: List[str]) -> int:
        if len(args) != 2:
            raise ValueError("wrong number of arguments for 'decrby' command")
        return self._incr_by(args[0], -int(args[1]))

    def _incr_by(self, key: str, amount: int) -> int:
        entry = self.store.get_entry(key)
        if entry is None:
            new_val = amount
            self.store.set_entry(key, str(new_val), "string")
            return new_val
        if entry.data_type != "string":
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
        try:
            curr = int(entry.value)
        except ValueError:
            raise ValueError("ERR value is not an integer or out of range")
        new_val = curr + amount
        entry.value = str(new_val)
        return new_val

    def _cmd_append(self, args: List[str]) -> int:
        if len(args) != 2:
            raise ValueError("wrong number of arguments for 'append' command")
        key, append_val = args[0], args[1]
        entry = self.store.get_entry(key)
        if entry is None:
            self.store.set_entry(key, append_val, "string")
            return len(append_val)
        if entry.data_type != "string":
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
        entry.value = str(entry.value) + append_val
        return len(entry.value)

    def _cmd_strlen(self, args: List[str]) -> int:
        if not args:
            raise ValueError("wrong number of arguments for 'strlen' command")
        entry = self.store.get_entry(args[0])
        if entry is None:
            return 0
        if entry.data_type != "string":
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
        return len(str(entry.value))

    # --- Lists ---

    def _cmd_lpush(self, args: List[str]) -> int:
        if len(args) < 2:
            raise ValueError("wrong number of arguments for 'lpush' command")
        key = args[0]
        values = args[1:]
        entry = self.store.get_entry(key)
        if entry is None:
            lst = list(reversed(values))
            self.store.set_entry(key, lst, "list")
            return len(lst)
        if entry.data_type != "list":
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
        for v in values:
            entry.value.insert(0, v)
        return len(entry.value)

    def _cmd_rpush(self, args: List[str]) -> int:
        if len(args) < 2:
            raise ValueError("wrong number of arguments for 'rpush' command")
        key = args[0]
        values = args[1:]
        entry = self.store.get_entry(key)
        if entry is None:
            lst = list(values)
            self.store.set_entry(key, lst, "list")
            return len(lst)
        if entry.data_type != "list":
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
        entry.value.extend(values)
        return len(entry.value)

    def _cmd_lpop(self, args: List[str]) -> Optional[str]:
        if not args:
            raise ValueError("wrong number of arguments for 'lpop' command")
        entry = self.store.get_entry(args[0])
        if entry is None:
            return None
        if entry.data_type != "list":
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
        return entry.value.pop(0) if entry.value else None

    def _cmd_rpop(self, args: List[str]) -> Optional[str]:
        if not args:
            raise ValueError("wrong number of arguments for 'rpop' command")
        entry = self.store.get_entry(args[0])
        if entry is None:
            return None
        if entry.data_type != "list":
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
        return entry.value.pop() if entry.value else None

    def _cmd_lrange(self, args: List[str]) -> List[str]:
        if len(args) != 3:
            raise ValueError("wrong number of arguments for 'lrange' command")
        entry = self.store.get_entry(args[0])
        if entry is None:
            return []
        if entry.data_type != "list":
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
        start = int(args[1])
        stop = int(args[2])
        length = len(entry.value)

        # Handle negative indices
        if start < 0:
            start = max(0, length + start)
        if stop < 0:
            stop = length + stop
        return entry.value[start : stop + 1]

    def _cmd_llen(self, args: List[str]) -> int:
        if not args:
            raise ValueError("wrong number of arguments for 'llen' command")
        entry = self.store.get_entry(args[0])
        if entry is None:
            return 0
        if entry.data_type != "list":
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
        return len(entry.value)

    def _cmd_lindex(self, args: List[str]) -> Optional[str]:
        if len(args) != 2:
            raise ValueError("wrong number of arguments for 'lindex' command")
        entry = self.store.get_entry(args[0])
        if entry is None:
            return None
        if entry.data_type != "list":
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
        idx = int(args[1])
        try:
            return entry.value[idx]
        except IndexError:
            return None

    # --- Sets ---

    def _cmd_sadd(self, args: List[str]) -> int:
        if len(args) < 2:
            raise ValueError("wrong number of arguments for 'sadd' command")
        key = args[0]
        members = args[1:]
        entry = self.store.get_entry(key)
        if entry is None:
            s = set(members)
            self.store.set_entry(key, s, "set")
            return len(s)
        if entry.data_type != "set":
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
        added = 0
        for m in members:
            if m not in entry.value:
                entry.value.add(m)
                added += 1
        return added

    def _cmd_srem(self, args: List[str]) -> int:
        if len(args) < 2:
            raise ValueError("wrong number of arguments for 'srem' command")
        key = args[0]
        members = args[1:]
        entry = self.store.get_entry(key)
        if entry is None:
            return 0
        if entry.data_type != "set":
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
        removed = 0
        for m in members:
            if m in entry.value:
                entry.value.remove(m)
                removed += 1
        return removed

    def _cmd_smembers(self, args: List[str]) -> List[str]:
        if not args:
            raise ValueError("wrong number of arguments for 'smembers' command")
        entry = self.store.get_entry(args[0])
        if entry is None:
            return []
        if entry.data_type != "set":
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
        return sorted(list(entry.value))

    def _cmd_sismember(self, args: List[str]) -> int:
        if len(args) != 2:
            raise ValueError("wrong number of arguments for 'sismember' command")
        entry = self.store.get_entry(args[0])
        if entry is None:
            return 0
        if entry.data_type != "set":
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
        return 1 if args[1] in entry.value else 0

    def _cmd_scard(self, args: List[str]) -> int:
        if not args:
            raise ValueError("wrong number of arguments for 'scard' command")
        entry = self.store.get_entry(args[0])
        if entry is None:
            return 0
        if entry.data_type != "set":
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
        return len(entry.value)

    def _cmd_sdiff(self, args: List[str]) -> List[str]:
        if not args:
            raise ValueError("wrong number of arguments for 'sdiff' command")
        base = self._get_set_or_empty(args[0])
        for k in args[1:]:
            base = base - self._get_set_or_empty(k)
        return sorted(list(base))

    def _cmd_sinter(self, args: List[str]) -> List[str]:
        if not args:
            raise ValueError("wrong number of arguments for 'sinter' command")
        base = self._get_set_or_empty(args[0])
        for k in args[1:]:
            base = base & self._get_set_or_empty(k)
        return sorted(list(base))

    def _cmd_sunion(self, args: List[str]) -> List[str]:
        if not args:
            raise ValueError("wrong number of arguments for 'sunion' command")
        base: Set[str] = set()
        for k in args:
            base = base | self._get_set_or_empty(k)
        return sorted(list(base))

    def _get_set_or_empty(self, key: str) -> Set[str]:
        entry = self.store.get_entry(key)
        if entry is None:
            return set()
        if entry.data_type != "set":
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
        return set(entry.value)

    # --- Hashes ---

    def _cmd_hset(self, args: List[str]) -> int:
        if len(args) < 3 or (len(args) - 1) % 2 != 0:
            raise ValueError("wrong number of arguments for 'hset' command")
        key = args[0]
        fields_vals = args[1:]
        entry = self.store.get_entry(key)
        created = 0

        if entry is None:
            h = {}
            for i in range(0, len(fields_vals), 2):
                h[fields_vals[i]] = fields_vals[i + 1]
                created += 1
            self.store.set_entry(key, h, "hash")
            return created

        if entry.data_type != "hash":
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

        for i in range(0, len(fields_vals), 2):
            field_name = fields_vals[i]
            if field_name not in entry.value:
                created += 1
            entry.value[field_name] = fields_vals[i + 1]
        return created

    def _cmd_hget(self, args: List[str]) -> Optional[str]:
        if len(args) != 2:
            raise ValueError("wrong number of arguments for 'hget' command")
        entry = self.store.get_entry(args[0])
        if entry is None:
            return None
        if entry.data_type != "hash":
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
        return entry.value.get(args[1])

    def _cmd_hdel(self, args: List[str]) -> int:
        if len(args) < 2:
            raise ValueError("wrong number of arguments for 'hdel' command")
        entry = self.store.get_entry(args[0])
        if entry is None:
            return 0
        if entry.data_type != "hash":
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
        deleted = 0
        for f in args[1:]:
            if f in entry.value:
                del entry.value[f]
                deleted += 1
        return deleted

    def _cmd_hexists(self, args: List[str]) -> int:
        if len(args) != 2:
            raise ValueError("wrong number of arguments for 'hexists' command")
        entry = self.store.get_entry(args[0])
        if entry is None:
            return 0
        if entry.data_type != "hash":
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
        return 1 if args[1] in entry.value else 0

    def _cmd_hgetall(self, args: List[str]) -> List[str]:
        if not args:
            raise ValueError("wrong number of arguments for 'hgetall' command")
        entry = self.store.get_entry(args[0])
        if entry is None:
            return []
        if entry.data_type != "hash":
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
        flat = []
        for k, v in entry.value.items():
            flat.append(k)
            flat.append(str(v))
        return flat

    def _cmd_hkeys(self, args: List[str]) -> List[str]:
        if not args:
            raise ValueError("wrong number of arguments for 'hkeys' command")
        entry = self.store.get_entry(args[0])
        if entry is None:
            return []
        if entry.data_type != "hash":
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
        return list(entry.value.keys())

    def _cmd_hvals(self, args: List[str]) -> List[str]:
        if not args:
            raise ValueError("wrong number of arguments for 'hvals' command")
        entry = self.store.get_entry(args[0])
        if entry is None:
            return []
        if entry.data_type != "hash":
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
        return [str(v) for v in entry.value.values()]

    def _cmd_hlen(self, args: List[str]) -> int:
        if not args:
            raise ValueError("wrong number of arguments for 'hlen' command")
        entry = self.store.get_entry(args[0])
        if entry is None:
            return 0
        if entry.data_type != "hash":
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
        return len(entry.value)

    def _cmd_hincrby(self, args: List[str]) -> int:
        if len(args) != 3:
            raise ValueError("wrong number of arguments for 'hincrby' command")
        key, field_name, amount = args[0], args[1], int(args[2])
        entry = self.store.get_entry(key)
        if entry is None:
            self.store.set_entry(key, {field_name: str(amount)}, "hash")
            return amount
        if entry.data_type != "hash":
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
        curr = int(entry.value.get(field_name, 0))
        new_val = curr + amount
        entry.value[field_name] = str(new_val)
        return new_val

    # --- Sorted Sets (ZSets) ---

    def _cmd_zadd(self, args: List[str]) -> int:
        if len(args) < 3 or (len(args) - 1) % 2 != 0:
            raise ValueError("wrong number of arguments for 'zadd' command")
        key = args[0]
        score_members = args[1:]
        entry = self.store.get_entry(key)
        added = 0

        if entry is None:
            z = {}
            for i in range(0, len(score_members), 2):
                score = float(score_members[i])
                member = score_members[i + 1]
                z[member] = score
                added += 1
            self.store.set_entry(key, z, "zset")
            return added

        if entry.data_type != "zset":
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

        for i in range(0, len(score_members), 2):
            score = float(score_members[i])
            member = score_members[i + 1]
            if member not in entry.value:
                added += 1
            entry.value[member] = score
        return added

    def _cmd_zrange(self, args: List[str]) -> List[str]:
        if len(args) < 3:
            raise ValueError("wrong number of arguments for 'zrange' command")
        key = args[0]
        start = int(args[1])
        stop = int(args[2])
        with_scores = len(args) >= 4 and args[3].upper() == "WITHSCORES"

        entry = self.store.get_entry(key)
        if entry is None:
            return []
        if entry.data_type != "zset":
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

        # Sort by score ascending, then member name ascending
        sorted_items = sorted(entry.value.items(), key=lambda x: (x[1], x[0]))
        length = len(sorted_items)

        if start < 0:
            start = max(0, length + start)
        if stop < 0:
            stop = length + stop
        slice_items = sorted_items[start : stop + 1]

        res = []
        for member, score in slice_items:
            res.append(member)
            if with_scores:
                res.append(str(int(score) if score.is_integer() else score))
        return res

    def _cmd_zscore(self, args: List[str]) -> Optional[str]:
        if len(args) != 2:
            raise ValueError("wrong number of arguments for 'zscore' command")
        entry = self.store.get_entry(args[0])
        if entry is None or entry.data_type != "zset":
            return None
        score = entry.value.get(args[1])
        if score is None:
            return None
        return str(int(score) if score.is_integer() else score)

    def _cmd_zcard(self, args: List[str]) -> int:
        if not args:
            raise ValueError("wrong number of arguments for 'zcard' command")
        entry = self.store.get_entry(args[0])
        if entry is None:
            return 0
        if entry.data_type != "zset":
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
        return len(entry.value)

    def _cmd_zrem(self, args: List[str]) -> int:
        if len(args) < 2:
            raise ValueError("wrong number of arguments for 'zrem' command")
        entry = self.store.get_entry(args[0])
        if entry is None:
            return 0
        if entry.data_type != "zset":
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
        removed = 0
        for m in args[1:]:
            if m in entry.value:
                del entry.value[m]
                removed += 1
        return removed
