"""Publish/Subscribe channel broker for real-time messaging."""

from __future__ import annotations

import threading
from typing import Callable, Dict, List, Set

from .resp import RespSerializer


class PubSubHub:
    """Thread-safe Pub/Sub message broker delivering messages to subscriber callbacks."""

    def __init__(self):
        # channel_name -> {client_id: callback_fn}
        self._channels: Dict[str, Dict[str, Callable[[bytes], None]]] = {}
        # client_id -> {channel_name}
        self._client_subscriptions: Dict[str, Set[str]] = {}
        self._lock = threading.RLock()

    def subscribe(
        self, client_id: str, channel: str, callback: Callable[[bytes], None]
    ) -> int:
        """Subscribe client to channel. Returns total subscriptions for this client."""
        with self._lock:
            if channel not in self._channels:
                self._channels[channel] = {}
            self._channels[channel][client_id] = callback

            if client_id not in self._client_subscriptions:
                self._client_subscriptions[client_id] = set()
            self._client_subscriptions[client_id].add(channel)

            return len(self._client_subscriptions[client_id])

    def unsubscribe(self, client_id: str, channel: str) -> int:
        """Unsubscribe client from channel. Returns remaining subscriptions for client."""
        with self._lock:
            if channel in self._channels and client_id in self._channels[channel]:
                del self._channels[channel][client_id]
                if not self._channels[channel]:
                    del self._channels[channel]

            if client_id in self._client_subscriptions:
                self._client_subscriptions[client_id].discard(channel)
                remaining = len(self._client_subscriptions[client_id])
                if remaining == 0:
                    del self._client_subscriptions[client_id]
                return remaining

            return 0

    def unsubscribe_all(self, client_id: str) -> None:
        """Cleanly remove client from all subscribed channels upon disconnect."""
        with self._lock:
            channels = list(self._client_subscriptions.get(client_id, set()))
            for ch in channels:
                self.unsubscribe(client_id, ch)

    def publish(self, channel: str, message: str) -> int:
        """Publish message to all subscribers of a channel. Returns number of recipients."""
        with self._lock:
            subscribers = self._channels.get(channel, {})
            if not subscribers:
                return 0

            # RESP array format: ["message", channel, message]
            payload = RespSerializer.array(["message", channel, message])

            count = 0
            for cb in subscribers.values():
                try:
                    cb(payload)
                    count += 1
                except Exception:
                    pass

            return count

    def subscriber_count(self, channel: str) -> int:
        """Return count of active subscribers for a channel."""
        with self._lock:
            return len(self._channels.get(channel, {}))
