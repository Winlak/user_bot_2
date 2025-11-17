"""Utilities for deduplicating forwarded Telegram posts."""

from __future__ import annotations

import asyncio
import logging
from urllib.parse import urlparse
from typing import Iterable

import aiosqlite

from telethon.tl.types import Message

from .messages import message_identity


class ForwardedMessageStore:
    """Persistence layer for forwarded messages.

    Messages are identified by their (chat_id, message_id) tuple. The store uses a
    simple SQLite database so duplicate detection survives restarts.
    """

    def __init__(self, database: str) -> None:
        self.database = database
        self._connection: aiosqlite.Connection | None = None
        self._lock = asyncio.Lock()
        self.logger = logging.getLogger("keyword_forwarder.store")

    @classmethod
    def from_url(cls, url: str) -> "ForwardedMessageStore":
        """Create a store from a SQLite URL."""

        database = _sqlite_path_from_url(url)
        return cls(database)

    async def connect(self) -> None:
        if self._connection is not None:
            return
        self._connection = await aiosqlite.connect(self.database)
        await self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS forwarded_messages (
                chat_id INTEGER,
                message_id INTEGER,
                PRIMARY KEY (chat_id, message_id)
            )
            """
        )
        await self._connection.commit()

    async def close(self) -> None:
        if self._connection is None:
            return
        await self._connection.close()
        self._connection = None

    async def contains(self, identity: tuple[int | None, int]) -> bool:
        connection = await self._require_connection()
        async with self._lock:
            cursor = await connection.execute(
                """
                SELECT 1 FROM forwarded_messages
                WHERE chat_id = ? AND message_id = ?
                LIMIT 1
                """,
                identity,
            )
            row = await cursor.fetchone()
            await cursor.close()
            return row is not None

    async def store_many(self, identities: Iterable[tuple[int | None, int]]) -> None:
        connection = await self._require_connection()
        entries = [(chat_id, message_id) for chat_id, message_id in identities]
        if not entries:
            return
        async with self._lock:
            await connection.executemany(
                """
                INSERT OR IGNORE INTO forwarded_messages (chat_id, message_id)
                VALUES (?, ?)
                """,
                entries,
            )
            await connection.commit()

    async def _require_connection(self) -> aiosqlite.Connection:
        if self._connection is None:
            raise RuntimeError("ForwardedMessageStore is not connected")
        return self._connection


class MessageDeduplicator:
    """Filter out posts that have already been forwarded."""

    def __init__(self, store: ForwardedMessageStore | None = None) -> None:
        self.store = store
        self.logger = logging.getLogger("keyword_forwarder.deduplicator")
        self._seen_identities: set[tuple[int | None, int]] = set()

    async def filter_new(self, messages: list[Message]) -> list[Message]:
        """Return only messages that have not been processed before."""

        if not messages:
            return []

        new_messages: list[Message] = []
        new_identities: list[tuple[int | None, int]] = []

        for message in messages:
            identity = message_identity(message)
            if identity in self._seen_identities:
                continue
            if self.store and await self.store.contains(identity):
                self.logger.debug("Skipping already forwarded message %s", identity)
                continue
            new_messages.append(message)
            new_identities.append(identity)

        if new_identities:
            self._seen_identities.update(new_identities)
            if self.store:
                await self.store.store_many(new_identities)

        return new_messages


def _sqlite_path_from_url(url: str) -> str:
    """Extract a database path from a SQLite URL."""

    if not url.startswith("sqlite"):
        raise ValueError("Only sqlite URLs are supported for deduplication storage")

    cleaned = url.replace("sqlite+aiosqlite", "sqlite", 1)
    parsed = urlparse(cleaned)
    if parsed.scheme != "sqlite":
        raise ValueError(f"Unsupported database scheme: {parsed.scheme}")

    path = parsed.path
    if path.startswith("///"):
        path = path[2:]
    elif path.startswith("//"):
        path = path[1:]
    elif path.startswith("/"):
        path = path[1:]

    return path or ":memory:"
