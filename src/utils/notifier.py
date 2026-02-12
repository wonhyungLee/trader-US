from __future__ import annotations

import logging
import time
from typing import List

import requests

DISCORD_MAX_LEN = 2000
DISCORD_SAFE_LEN = 1900


def _append_site(settings: dict, message: str) -> str:
    url = settings.get("site_url") or settings.get("site", {}).get("url")
    if not url or url in message:
        return message
    return f"{message} | site: {url}"


def _chunk_message(message: str, limit: int = DISCORD_SAFE_LEN) -> List[str]:
    message = message or ""
    if len(message) <= limit:
        return [message]

    parts: List[str] = []
    buf = ""
    for line in message.splitlines(True):
        if len(buf) + len(line) <= limit:
            buf += line
            continue
        if buf:
            parts.append(buf.rstrip("\n"))
            buf = ""
        while len(line) > limit:
            parts.append(line[:limit])
            line = line[limit:]
        buf = line
    if buf:
        parts.append(buf.rstrip("\n"))
    return [p for p in parts if p]


def send_telegram(bot_token: str, chat_id: str, text: str, parse_mode: str = "Markdown") -> bool:
    if not bot_token or not chat_id:
        return False
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    try:
        resp = requests.post(url, data={"chat_id": chat_id, "text": text, "parse_mode": parse_mode}, timeout=7)
        if not resp.ok:
            logging.warning("telegram send failed: %s", resp.text)
            return False
        return True
    except Exception as e:
        logging.warning("telegram send error: %s", e)
        return False


def _send_discord(webhook: str, content: str) -> bool:
    if not webhook:
        return False
    payload = {"content": content}
    backoff = 0.5
    for attempt in range(1, 4):
        try:
            resp = requests.post(webhook, json=payload, timeout=7)

            if resp.status_code == 429:
                try:
                    retry_after = float(resp.json().get("retry_after", 1))
                except Exception:
                    retry_after = 1.0
                logging.warning("Discord rate limited. Retrying after %ss (attempt %s)", retry_after, attempt)
                time.sleep(retry_after + 0.2)
                continue

            if 500 <= resp.status_code < 600:
                logging.warning("Discord server error %s. Backoff %ss (attempt %s)", resp.status_code, backoff, attempt)
                time.sleep(backoff)
                backoff = min(5.0, backoff * 2)
                continue

            if resp.ok:
                return True

            logging.warning("discord send failed: %s", resp.text)
            return False

        except Exception:
            logging.exception("discord send failed")
            time.sleep(backoff)
            backoff = min(5.0, backoff * 2)

    return False


def maybe_notify(settings: dict, message: str) -> None:
    message = _append_site(settings, message)

    dc = settings.get("discord", {}) or {}
    tg = settings.get("telegram", {}) or {}

    dc_success = False
    if dc.get("enabled") and dc.get("webhook"):
        dc_success = True
        for chunk in _chunk_message(message, DISCORD_SAFE_LEN):
            if not _send_discord(dc["webhook"], chunk):
                dc_success = False
                break

    if not dc_success and tg.get("enabled"):
        send_telegram(tg.get("token"), tg.get("chat_id"), message)
