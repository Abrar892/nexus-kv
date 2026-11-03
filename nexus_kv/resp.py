"""REdis Serialization Protocol (RESP v2) parser and serializer."""

from __future__ import annotations

from typing import Any, List, Optional, Tuple, Union


class RespProtocolError(Exception):
    """Raised when RESP input bytes violate protocol specifications."""
    pass


class RespParser:
    """Stream parser for the Redis Serialization Protocol (RESP v2) and inline commands."""

    @classmethod
    def parse(cls, buffer: bytes) -> Tuple[Optional[Any], int]:
        """
        Parse one complete RESP object from the buffer.
        Returns (parsed_object, bytes_consumed) or (None, 0) if incomplete.
        """
        if not buffer:
            return None, 0

        first_byte = buffer[0:1]

        # 1. Simple String (+)
        if first_byte == b"+":
            idx = buffer.find(b"\r\n")
            if idx == -1:
                return None, 0
            val = buffer[1:idx].decode("utf-8", errors="replace")
            return val, idx + 2

        # 2. Error (-)
        if first_byte == b"-":
            idx = buffer.find(b"\r\n")
            if idx == -1:
                return None, 0
            err_msg = buffer[1:idx].decode("utf-8", errors="replace")
            return RespProtocolError(err_msg), idx + 2

        # 3. Integer (:)
        if first_byte == b":":
            idx = buffer.find(b"\r\n")
            if idx == -1:
                return None, 0
            try:
                val = int(buffer[1:idx])
            except ValueError:
                raise RespProtocolError(f"Invalid integer format: {buffer[1:idx]!r}")
            return val, idx + 2

        # 4. Bulk String ($)
        if first_byte == b"$":
            idx = buffer.find(b"\r\n")
            if idx == -1:
                return None, 0
            try:
                length = int(buffer[1:idx])
            except ValueError:
                raise RespProtocolError(f"Invalid bulk string length: {buffer[1:idx]!r}")

            # Null bulk string ($-1\r\n)
            if length == -1:
                return None, idx + 2

            start = idx + 2
            end = start + length
            if len(buffer) < end + 2:
                return None, 0

            if buffer[end : end + 2] != b"\r\n":
                raise RespProtocolError("Missing CRLF at end of bulk string")

            payload = buffer[start:end].decode("utf-8", errors="replace")
            return payload, end + 2

        # 5. Array (*)
        if first_byte == b"*":
            idx = buffer.find(b"\r\n")
            if idx == -1:
                return None, 0
            try:
                num_elements = int(buffer[1:idx])
            except ValueError:
                raise RespProtocolError(f"Invalid array count: {buffer[1:idx]!r}")

            # Null array (*-1\r\n)
            if num_elements == -1:
                return None, idx + 2

            offset = idx + 2
            elements: List[Any] = []
            for _ in range(num_elements):
                if offset >= len(buffer):
                    return None, 0
                elem, consumed = cls.parse(buffer[offset:])
                if consumed == 0:
                    return None, 0
                elements.append(elem)
                offset += consumed

            return elements, offset

        # 6. Fallback: Inline command (e.g. "PING\r\n" or "SET foo bar\r\n")
        idx = buffer.find(b"\r\n")
        if idx != -1:
            line = buffer[:idx].decode("utf-8", errors="replace").strip()
            if not line:
                return None, idx + 2
            parts = line.split()
            return parts, idx + 2

        return None, 0


class RespSerializer:
    """Serializes Python values to RESP wire format bytes."""

    @staticmethod
    def simple_string(value: str) -> bytes:
        return f"+{value}\r\n".encode("utf-8")

    @staticmethod
    def error(message: str) -> bytes:
        return f"-{message}\r\n".encode("utf-8")

    @staticmethod
    def integer(value: int) -> bytes:
        return f":{value}\r\n".encode("utf-8")

    @staticmethod
    def bulk_string(value: Optional[Union[str, bytes]]) -> bytes:
        if value is None:
            return b"$-1\r\n"
        if isinstance(value, str):
            encoded = value.encode("utf-8")
        else:
            encoded = value
        return f"${len(encoded)}\r\n".encode("utf-8") + encoded + b"\r\n"

    @classmethod
    def array(cls, items: Optional[List[Any]]) -> bytes:
        if items is None:
            return b"*-1\r\n"
        parts = [f"*{len(items)}\r\n".encode("utf-8")]
        for item in items:
            parts.append(cls.serialize(item))
        return b"".join(parts)

    @classmethod
    def serialize(cls, value: Any) -> bytes:
        """Dynamically serialize value to appropriate RESP representation."""
        if value is None:
            return b"$-1\r\n"
        if isinstance(value, bool):
            return cls.integer(1 if value else 0)
        if isinstance(value, int):
            return cls.integer(value)
        if isinstance(value, (str, bytes)):
            return cls.bulk_string(value)
        if isinstance(value, (list, tuple, set)):
            return cls.array(list(value))
        if isinstance(value, Exception):
            return cls.error(str(value))
        return cls.bulk_string(str(value))
