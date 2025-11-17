"""Keyword-based message forwarding for Telethon with queueing support."""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass, field
from typing import Iterable, Sequence

from telethon.errors import RPCError
from telethon.events import NewMessage
from telethon.tl.types import Message


def _message_identity(message: Message) -> tuple[int | None, int]:
    """Return a hashable identity for a Telethon message."""

    peer = message.peer_id
    if peer is None:
        return (None, message.id)
    channel_id = getattr(peer, "channel_id", None)
    if channel_id is not None:
        return (channel_id, message.id)
    chat_id = getattr(peer, "chat_id", None)
    if chat_id is not None:
        return (chat_id, message.id)
    user_id = getattr(peer, "user_id", None)
    return (user_id, message.id)

ChannelRef = int | str

_MESSAGE_LINK_RE = re.compile(
    r"https?://t\.me/(c/)?([A-Za-z0-9_]+)/([0-9]+)", re.IGNORECASE
)


@dataclass(slots=True)
class ForwardPayload:
    """Container with the data required to forward a matched event."""

    event: NewMessage.Event
    matched_keyword: str
    links: tuple[str, ...]


@dataclass(slots=True)
class KeywordForwarder:
    """Forward messages that match configured keywords to target channels."""

    keywords: Sequence[str]
    target_channels: Sequence[ChannelRef]
    case_sensitive: bool = False
    forwarding_enabled: bool = True
    logger: logging.Logger = field(
        default_factory=lambda: logging.getLogger("keyword_forwarder")
    )

    def __post_init__(self) -> None:
        self.update_keywords(self.keywords)
        self._targets: tuple[ChannelRef, ...] = tuple(self.target_channels)
        if not self._targets:
            self.logger.warning(
                "KeywordForwarder initialised without target channels; matched messages will not be forwarded."
            )

    def update_keywords(self, keywords: Iterable[str]) -> None:
        """Replace the internal keyword list."""

        filtered = [kw.strip() for kw in keywords if kw and kw.strip()]
        # Preserve the original order while deduplicating
        seen: set[str] = set()
        ordered: list[str] = []
        for keyword in filtered:
            if keyword not in seen:
                seen.add(keyword)
                ordered.append(keyword)
        self._keywords: tuple[str, ...] = tuple(ordered)
        normalised = (
            ordered
            if self.case_sensitive
            else [keyword.casefold() for keyword in ordered]
        )
        self._normalised_keywords: tuple[str, ...] = tuple(normalised)
        if not self._keywords:
            self.logger.warning("KeywordForwarder initialised without any keywords.")

    def _match_keyword(self, text: str) -> str | None:
        if not text:
            return None
        haystack = text if self.case_sensitive else text.casefold()
        for original, normalised in zip(self._keywords, self._normalised_keywords):
            if normalised in haystack:
                return original
        return None

    def build_payload(self, event: NewMessage.Event) -> ForwardPayload | None:
        """Return a :class:`ForwardPayload` if the event should be processed."""

        text = event.raw_text or ""
        matched_keyword = self._match_keyword(text)
        if matched_keyword is None:
            return None

        links = tuple(dict.fromkeys(match.group(0) for match in _MESSAGE_LINK_RE.finditer(text)))
        return ForwardPayload(event=event, matched_keyword=matched_keyword, links=links)

    async def __call__(self, event: NewMessage.Event) -> None:
        """Telethon-compatible event handler."""

        payload = self.build_payload(event)
        if payload is None:
            return
        await self.process_payload(payload)

    async def process_payload(self, payload: ForwardPayload) -> None:
        """Forward the messages referenced by ``payload`` to the configured targets."""

        if not self._targets:
            return

        self.logger.info(
            "Processing message %s (keyword %r) with %d link(s)",
            payload.event.id,
            payload.matched_keyword,
            len(payload.links),
        )

        if not self.forwarding_enabled:
            self.logger.info(
                "Skipping forwarding of message %s because forwarding is disabled",
                payload.event.id,
            )
            return

        messages = await self._resolve_messages(payload)
        if not messages:
            self.logger.warning(
                "No resolvable target messages found for source %s; forwarding original alert.",
                payload.event.id,
            )
            messages = [payload.event.message]

        for message in messages:
            await self._forward_to_targets(payload, message)

    async def _resolve_messages(self, payload: ForwardPayload) -> list[Message]:
        """Resolve message links in the payload into :class:`Message` objects."""

        if not payload.links:
            return []

        client = payload.event.client
        resolved: list[Message] = []
        for link in payload.links:
            message = await self._fetch_linked_message(client, link, payload)
            if message is None:
                continue
            if isinstance(message, list):
                resolved.extend(msg for msg in message if msg)
            elif message:
                resolved.append(message)

        deduped: list[Message] = []
        seen: set[tuple[int | None, int]] = set()
        for message in resolved:
            identity = _message_identity(message)
            if identity in seen:
                continue
            seen.add(identity)
            deduped.append(message)
        return deduped

    async def _fetch_linked_message(
        self, client, link: str, payload: ForwardPayload
    ) -> Message | list[Message] | None:
        """Fetch a message referenced by a Telegram link."""

        try:
            return await client.get_messages(link)
        except Exception:  # pragma: no cover - network dependent
            match = _MESSAGE_LINK_RE.search(link)
            if not match:
                self.logger.error(
                    "Failed to fetch linked message %r for source %s: unsupported format",
                    link,
                    payload.event.id,
                )
                return None
            is_private = match.group(1) is not None
            channel_part = match.group(2)
            message_id = int(match.group(3))
            entity: ChannelRef
            if is_private:
                entity = int(f"-100{channel_part}")
            else:
                entity = channel_part
            try:
                return await client.get_messages(entity, ids=message_id)
            except Exception as exc:  # pragma: no cover - network dependent
                self.logger.error(
                    "Failed to fetch linked message %r for source %s: %s",
                    link,
                    payload.event.id,
                    exc,
                )
                return None

    async def _forward_to_targets(
        self, payload: ForwardPayload, message: Message
    ) -> None:
        for channel in self._targets:
            try:
                await payload.event.client.forward_messages(
                    channel, message, silent=True
                )
            except RPCError as exc:  # pragma: no cover - network dependent
                self.logger.error(
                    "Failed to forward message %s to %s: %s",
                    payload.event.id,
                    channel,
                    exc,
                )


class ForwardingQueue:
    """Queue matched payloads and process them sequentially to avoid rate limits."""

    def __init__(
        self,
        forwarder: KeywordForwarder,
        *,
        maxsize: int = 0,
        delay_seconds: float = 0.0,
    ) -> None:
        self.forwarder = forwarder
        self.delay_seconds = max(0.0, delay_seconds)
        self._queue: asyncio.Queue[ForwardPayload] = asyncio.Queue(maxsize=maxsize)
        self._worker: asyncio.Task[None] | None = None
        self.logger = logging.getLogger("keyword_forwarder.queue")

    async def start(self) -> None:
        if self._worker is None or self._worker.done():
            self._worker = asyncio.create_task(self._run())

    async def stop(self) -> None:
        if self._worker is None:
            return
        self._worker.cancel()
        try:
            await self._worker
        except asyncio.CancelledError:  # pragma: no cover - cancellation path
            pass
        finally:
            self._worker = None

    async def enqueue(self, payload: ForwardPayload) -> None:
        await self._queue.put(payload)

    async def join(self) -> None:
        await self._queue.join()

    async def _run(self) -> None:
        while True:
            payload = await self._queue.get()
            try:
                await self.forwarder.process_payload(payload)
            except Exception as exc:  # pragma: no cover - defensive logging
                self.logger.exception(
                    "Unexpected error while processing payload for message %s: %s",
                    payload.event.id,
                    exc,
                )
            finally:
                self._queue.task_done()
            if self.delay_seconds:
                await asyncio.sleep(self.delay_seconds)
