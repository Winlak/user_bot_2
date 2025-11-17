"""Entry point for the Telethon keyword forwarding bot."""

from __future__ import annotations

import asyncio
import logging
from typing import Sequence

from telethon import TelegramClient, events

from app import ForwardingQueue, KeywordForwarder
from config import get_settings, load_keywords

logger = logging.getLogger(__name__)


def _prepare_forwarder() -> tuple[KeywordForwarder, list[str]]:
    settings = get_settings()
    keywords = load_keywords(settings)
    logger.info("Loaded %d keywords", len(keywords))
    forwarder = KeywordForwarder(
        keywords=keywords,
        target_channels=settings.target_channels,
        case_sensitive=settings.case_sensitive_keywords,
        forwarding_enabled=settings.forwarding_enabled,
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
    forwarder, keywords = _prepare_forwarder()
    queue = ForwardingQueue(
        forwarder,
        maxsize=settings.forwarding_queue_maxsize,
        delay_seconds=settings.forwarding_delay_seconds,
    )
    await queue.start()

    client = TelegramClient(settings.session_name, settings.api_id, settings.api_hash)

    @client.on(events.NewMessage(chats=settings.source_channel))
    async def handler(event):  # type: ignore[no-untyped-def]
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
    async with client:
        try:
            await client.run_until_disconnected()
        finally:
            await queue.join()
            await queue.stop()


def _format_targets(targets: Sequence[int | str]) -> str:
    if not targets:
        return "<none>"
    return ", ".join(str(target) for target in targets)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
