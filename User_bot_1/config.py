"""Application configuration for the keyword forwarding bot."""

from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path
from typing import Iterable, Sequence

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from app.forwarder import ChannelRef

DEFAULT_KEYWORDS: tuple[str, ...] = (
    "розыгрыш",
    "приз",
    "конкурс",
    "призы",
    "#розыгрыш",
    "#конкурс",
    "giveaway",
    "prize",
    "raffle",
    "winners",
    "soon",
    "условия участия розыгрыш",
    "правила конкурса",
    "подпишись лайк комментарий розыгрыш",
    "репост отметить друга конкурс",
    "deadline до сегодня розыгрыш",
    "deadline до завтра розыгрыш",
    "разыгрываем среди подписчиков",
    "🎁 розыгрыш",
)


def _split_items(value: str) -> list[str]:
    raw_items = re.split(r"[\n,;]+", value)
    return [item.strip() for item in raw_items if item and item.strip()]


def _parse_channel(value: ChannelRef) -> ChannelRef:
    if isinstance(value, str):
        candidate = value.strip()
        if not candidate:
            return ""
        if candidate.startswith("-") and candidate[1:].isdigit():
            return int(candidate)
        if candidate.isdigit():
            return int(candidate)
        return candidate
    return value


def _parse_channels(value: Sequence[ChannelRef] | ChannelRef | None) -> list[ChannelRef]:
    if value is None:
        return []
    if isinstance(value, (str, int)):
        value = [value]
    parsed: list[ChannelRef] = []
    for item in value:
        if isinstance(item, str):
            parts = _split_items(item)
            if len(parts) > 1:
                parsed.extend(_parse_channel(part) for part in parts)
            else:
                parsed.append(_parse_channel(item))
        else:
            parsed.append(item)
    return [channel for channel in parsed if channel != ""]


class Settings(BaseSettings):
    """Typed configuration loaded from environment variables."""

    api_id: int
    api_hash: str
    session_name: str = "trustat_keyword_forwarder"
    source_channel: ChannelRef = "@trustat"
    target_channels: list[ChannelRef] = Field(default_factory=list)
    keywords: list[str] = Field(default_factory=list)
    keywords_file: Path | None = Path("keywords.txt")
    case_sensitive_keywords: bool = False
    forwarding_enabled: bool = False
    forwarding_queue_maxsize: int = Field(default=0, ge=0)
    forwarding_delay_seconds: float = Field(default=1.0, ge=0.0)
    forwarding_max_messages_per_second: float | None = Field(default=1.0)

    keepalive_enabled: bool = True
    keepalive_chat: ChannelRef = "@TrustatAlertsBot"
    keepalive_command: str = "/start"
    keepalive_interval_seconds: float = Field(default=60.0, ge=1.0)

    db_url: str | None = "sqlite+aiosqlite:///db.sqlite3"
    log_level: str = "INFO"

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls,
        init_settings,
        env_settings,
        file_secret_settings,
    ):
        def _clean_env_settings():
            env_vars = env_settings()
            target_channels_key = "target_channels"
            if target_channels_key in env_vars:
                value = env_vars[target_channels_key]
                if isinstance(value, str) and not value.strip():
                    env_vars.pop(target_channels_key)
            return env_vars

        return init_settings, _clean_env_settings, file_secret_settings

    @field_validator("source_channel", mode="before")
    @classmethod
    def _validate_source_channel(cls, value: ChannelRef) -> ChannelRef:
        return _parse_channel(value)

    @field_validator("target_channels", mode="before")
    @classmethod
    def _validate_target_channels(
        cls, value: Sequence[ChannelRef] | ChannelRef | None
    ) -> list[ChannelRef]:
        return _parse_channels(value)

    @field_validator("keywords", mode="before")
    @classmethod
    def _validate_keywords(cls, value: Iterable[str] | str | None) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            return _split_items(value)
        return [item for item in value if item]

    @field_validator("keywords_file", mode="before")
    @classmethod
    def _validate_keywords_file(cls, value: Path | str | None) -> Path | None:
        if value is None or value == "":
            return None
        if isinstance(value, Path):
            return value
        return Path(value).expanduser()

    @field_validator("forwarding_max_messages_per_second")
    @classmethod
    def _validate_forwarding_rate(
        cls, value: float | None
    ) -> float | None:
        if value is None:
            return None
        if value <= 0:
            raise ValueError("forwarding_max_messages_per_second must be greater than zero")
        return value

    @field_validator("keepalive_chat", mode="before")
    @classmethod
    def _validate_keepalive_chat(cls, value: ChannelRef) -> ChannelRef:
        return _parse_channel(value)



@lru_cache
def get_settings() -> Settings:
    """Return a cached instance of :class:`Settings`."""

    return Settings()


def load_keywords(config: Settings | None = None) -> list[str]:
    """Load keywords from the configured sources."""

    settings = config or get_settings()

    if settings.keywords_file:
        file_path = settings.keywords_file
        if not file_path.is_absolute():
            file_path = Path.cwd() / file_path
        if file_path.exists():
            contents = file_path.read_text(encoding="utf-8")
            keywords = [line.strip() for line in contents.splitlines() if line.strip()]
            if keywords:
                return keywords
        else:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text(
                "\n".join(DEFAULT_KEYWORDS) + "\n", encoding="utf-8"
            )
            return list(DEFAULT_KEYWORDS)

    if settings.keywords:
        return list(dict.fromkeys(settings.keywords))

    return list(DEFAULT_KEYWORDS)


def get_db_url() -> str | None:
    """Expose the optional database URL for backwards compatibility."""

    return get_settings().db_url
