"""Helpers for working with Telegram messages."""

from __future__ import annotations

from telethon.tl.types import Message


def message_identity(message: Message) -> tuple[int | None, int]:
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
