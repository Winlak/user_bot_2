# Trustat keyword forwarder

This project contains a [Telethon](https://github.com/LonamiWebs/Telethon) client that listens to
messages published by a source channel (by default `@trustat`) and forwards every post that
contains one of the configured keywords to the target channels of your choice. When the matching
message is an alert that links to another post, the bot follows the link, fetches the referenced
message and forwards the original content instead of the notification.

## Quick start

1. Install the dependencies:

   ```bash
   pip install -r requirements.txt
   ```

2. Create a `.env` file next to `run.py` and provide your Telegram API credentials and
   configuration. A minimal configuration looks like this:

   ```env
    API_ID=123456
    API_HASH=0123456789abcdef0123456789abcdef
    BOT_TOKEN=1234567:ABCDEF_your_bot_token
    TARGET_CHANNELS=@my_forward_channel
    FORWARDING_ENABLED=true
    # Optional rate/queue controls and persistence:
    # FORWARDING_MAX_MESSAGES_PER_SECOND=1.0
    # FORWARDING_QUEUE_MAXSIZE=100
    # FORWARDING_DELAY_SECONDS=1.5
    # DB_URL=sqlite+aiosqlite:///db.sqlite3
    # Optional keepalive to retrigger alerts when the source stays quiet:
    # KEEPALIVE_ENABLED=true
    # KEEPALIVE_CHAT=@TrustatAlertsBot
    # KEEPALIVE_COMMAND=/start
    # KEEPALIVE_INTERVAL_SECONDS=60
    # Optional overrides:
    # SESSION_NAME=trustat_keyword_forwarder
    # SOURCE_CHANNEL=@trustat
    # LOG_LEVEL=INFO
    # KEYWORDS_FILE=keywords.txt
    # KEYWORDS=keyword1,keyword2
    ```

   - `TARGET_CHANNELS` accepts a comma-separated list of usernames (prefixed with `@`) or
     numeric channel IDs (for private channels start them with `-100`).
   - By default the bot reads the keyword list from `keywords.txt`. You can edit the file to
     customise the keywords without touching the code. If you prefer to store the list in the
     environment you can set the `KEYWORDS` variable instead.

3. (Optional) If you already have an authorised session, you can export it as a Telethon
   `SESSION_STRING` and provide it via environment variable to skip any login prompts entirely.
   Alternatively, mount a `SESSION_NAME.session` file in the working directory; both approaches
   avoid the interactive phone/code step. The default path is
   `session/trustat_keyword_forwarder.session`, which fits the included Docker volume mapping.

4. Run the bot:

   ```bash
   python run.py
   ```

   If you supply `SESSION_STRING`, Telethon reuses that authorised user session without asking for
   any credentials. With `BOT_TOKEN`, Telethon performs a fully non-interactive login as a bot.
   Without those values, Telethon will ask for the phone number linked to the Telegram API
   credentials and a login code on the first run (or you can pre-fill `PHONE_NUMBER` to avoid the
   phone prompt but you will still need the code). The session is stored locally using the
   `SESSION_NAME` value (by default `session/trustat_keyword_forwarder`), so it lands inside the
   bundled `session/` directory when you run under Docker. If no session file, `SESSION_STRING`, or
   `BOT_TOKEN`/`PHONE_NUMBER` is present, the bot exits immediately with a clear error message
   instead of hanging on an interactive prompt.


   > **Safety net:** Unless `FORWARDING_ENABLED` is explicitly set to `true`, the bot stays in
   > a dry-run mode and will never send messages to the target channels. This makes it safe to
   > test the configuration without accidentally publishing content.

## Keyword matching rules

- Matching is case-insensitive by default. Set `CASE_SENSITIVE_KEYWORDS=true` in the environment
  to change this behaviour.
- Keywords may contain spaces, emojis and hashtags.
- If a keyword is a substring of the message, the message will be forwarded. This makes it easy to
  use both single words and phrases.

## Project layout

- `run.py` – the application entry point. It configures logging, creates the Telethon client and
  registers the keyword-forwarding handler.
- `config.py` – settings management powered by `pydantic-settings`. It loads environment variables,
  the keyword list and takes care of converting channel identifiers.
- `app/forwarder.py` – the forwarding logic encapsulated in a reusable callable class with a queue
  that sequences outgoing forwards to respect Telegram rate limits.
- `keywords.txt` – default keyword list that can be adjusted without modifying the code.

## Docker image

The repository ships with a `Dockerfile` so you can build a container image and run the bot in an
isolated environment:

```bash
docker build -t trustat-forwarder .
docker run --rm \
  -e API_ID=123456 \
  -e API_HASH=0123456789abcdef0123456789abcdef \
  -e BOT_TOKEN=1234567:ABCDEF_your_bot_token \
  -e TARGET_CHANNELS=@my_forward_channel \
  -e FORWARDING_ENABLED=true \
  -v $(pwd)/session:/app/session \
  trustat-forwarder
```

The example mounts a local `session/` directory into the container so Telethon can persist the
authorisation session between runs.

## Rate limiting queue

- The bot buffers matched messages inside a queue to avoid hitting Telegram forwarding limits.
- The queue size is controlled by `FORWARDING_QUEUE_MAXSIZE` (set it to `0` for an unbounded
  queue).
- After each forwarding operation the bot waits for `FORWARDING_DELAY_SECONDS` before processing
  the next payload. Increase this value if you experience flood wait errors.
- `FORWARDING_MAX_MESSAGES_PER_SECOND` enforces a hard throughput ceiling; the worker will not
  send more messages per second than the configured value (set to an empty string or remove the
  variable to disable the cap).

## Keepalive pings
- When enabled (default), the bot sends `/start` to `@TrustatAlertsBot` if no new messages arrive
  from the source channel for at least 60 seconds. This nudges the alert bot to emit a fresh
  notification.
- Tune the behaviour with `KEEPALIVE_ENABLED`, `KEEPALIVE_CHAT`, `KEEPALIVE_COMMAND` and
  `KEEPALIVE_INTERVAL_SECONDS`.

## Duplicate protection for linked posts

- Each matched message is scanned for Telegram links; the referenced posts are fetched and
  forwarded instead of the alert wrapper.
- Linked posts are deduplicated using a small SQLite database (configurable via `DB_URL`), so the
  same referenced post will not be forwarded twice even if multiple alerts include it.

## Docker Compose
A `docker-compose.yml` file is provided for convenience and keeps both the Telethon session and
the deduplication database on the host machine:

```bash
mkdir -p session data
docker compose up --build
```

The Compose service builds the image from the local `Dockerfile`, uses the `.env` file for
configuration, and mounts:

- `session/` → `/app/session` to persist the Telethon session across restarts.
- `data/` → `/app/data` with `DB_URL=sqlite+aiosqlite:///data/db.sqlite3` injected automatically
  so the deduplication cache lives outside the container.
- `keywords.txt` in read-only mode so you can adjust the keyword list without rebuilding the image.

## Requirements

The project depends on:

- `Telethon` for interacting with Telegram.
- `pydantic` and `pydantic-settings` for structured configuration loading.

Install them with `pip install -r requirements.txt` before running the bot.
