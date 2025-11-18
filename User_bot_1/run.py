"""Entry point for the Telethon keyword forwarding bot."""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from contextlib import suppress
from typing import Callable, Sequence

from telethon import TelegramClient, events
from telethon.sessions import StringSession

from app import ForwardingQueue, KeywordForwarder
from app.dedup import ForwardedMessageStore, MessageDeduplicator
from config import Settings, get_settings, load_keywords

logger = logging.getLogger(__name__)


async def _prepare_forwarder(
    settings: Settings, deduplicator: MessageDeduplicator | None
) -> tuple[KeywordForwarder, list[str]]:
    keywords = load_keywords(settings)
    logger.info("Loaded %d keywords", len(keywords))
    forwarder = KeywordForwarder(
        keywords=keywords,
        target_channels=settings.target_channels,
        case_sensitive=settings.case_sensitive_keywords,
        forwarding_enabled=settings.forwarding_enabled,
        deduplicator=deduplicator,
    )
    if not settings.forwarding_enabled:
        logger.warning(
            "Forwarding is disabled. Messages that match keywords will be logged but not sent."
        )
    return forwarder, keywords


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


async def main() -> None:
    settings = get_settings()
    _configure_logging(settings.log_level)
    store: ForwardedMessageStore | None = None
    deduplicator: MessageDeduplicator | None = None
    last_message_at = time.monotonic()

    if settings.db_url:
        try:
            store = ForwardedMessageStore.from_url(settings.db_url)
            await store.connect()
            deduplicator = MessageDeduplicator(store)
            logger.info("Deduplication store initialised at %s", store.database)
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("Failed to initialise deduplication store: %s", exc)

    forwarder, keywords = await _prepare_forwarder(settings, deduplicator)
    queue = ForwardingQueue(
        forwarder,
        maxsize=settings.forwarding_queue_maxsize,
        delay_seconds=settings.forwarding_delay_seconds,
        max_messages_per_second=settings.forwarding_max_messages_per_second,
    )
    await queue.start()

    session: str | StringSession
    if settings.session_string:
        session = StringSession(settings.session_string)
    else:
        session = settings.session_name

    client = TelegramClient(session, settings.api_id, settings.api_hash)

    @client.on(events.NewMessage(chats=settings.source_channel))
    async def handler(event):  # type: ignore[no-untyped-def]
        nonlocal last_message_at
        last_message_at = time.monotonic()
        payload = forwarder.build_payload(event)
        if payload is None:
            return
        await queue.enqueue(payload)

    logger.info(
        "Starting keyword forwarder for source %s -> targets %s (%d keywords)",
        settings.source_channel,
        _format_targets(settings.target_channels),
        len(keywords),
    )

    keepalive_task: asyncio.Task[None] | None = None
    try:
        await client.connect()

        if settings.bot_token and not await client.is_user_authorized():
            await client.start(bot_token=settings.bot_token)

        if not await client.is_user_authorized():
            session_file = Path(f"{settings.session_name}.session")
            raise RuntimeError(
                "Session is not authorised. Log in once interactively and mount the resulting "
                f"session file ({session_file}), supply SESSION_STRING, or provide BOT_TOKEN for "
                "non-interactive startup."
            )

        if settings.keepalive_enabled and settings.keepalive_chat:
            keepalive_task = asyncio.create_task(
                _run_keepalive(
                    client,
                    chat=settings.keepalive_chat,
                    command=settings.keepalive_command,
                    interval_seconds=settings.keepalive_interval_seconds,
                    last_message_at=lambda: last_message_at,
                )
            )

        await client.run_until_disconnected()
    finally:
        await queue.join()
        await queue.stop()
        if keepalive_task:
            keepalive_task.cancel()
            with suppress(asyncio.CancelledError):
                await keepalive_task
        if store:
            await store.close()
        await client.disconnect()


def _format_targets(targets: Sequence[int | str]) -> str:
    if not targets:
        return "<none>"
    return ", ".join(str(target) for target in targets)


async def _run_keepalive(
    client: TelegramClient,
    *,
    chat: int | str,
    command: str,
    interval_seconds: float,
    last_message_at: Callable[[], float],
) -> None:
    """Send a keepalive command periodically to trigger new alerts."""

    while True:
        await asyncio.sleep(interval_seconds)
        idle_for = time.monotonic() - last_message_at()
        if idle_for < interval_seconds:
            continue
        try:
            await client.send_message(chat, command)
            logger.info(
                "Sent keepalive command %s to %s after %.1fs of inactivity",
                command,
                chat,
                idle_for,
            )
        except Exception as exc:  # pragma: no cover - network dependent
            logger.error(
                "Failed to send keepalive command %s to %s: %s", command, chat, exc
            )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
