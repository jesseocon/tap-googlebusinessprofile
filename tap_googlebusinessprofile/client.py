"""Google Business Profile API calls: one OAuth token, paced and retried, safe across threads.

Every API has a per-project, per-minute quota (Business Information and Account Management 300,
Performance 600, My Business v4 600 for the GCP project that owns the OAuth client), shared by
every connection using that client. Each host gets its own pace (requests_per_minute) so one run
never takes a whole quota, and a 429 backs off and retries.
"""

from __future__ import annotations

import logging
import random
import threading
import time
from collections.abc import Iterator
from typing import Any
from urllib.parse import urlparse

import requests

TOKEN_URL = "https://oauth2.googleapis.com/token"
ACCOUNTS_API = "https://mybusinessaccountmanagement.googleapis.com/v1"
INFO_API = "https://mybusinessbusinessinformation.googleapis.com/v1"
PERFORMANCE_API = "https://businessprofileperformance.googleapis.com/v1"
REVIEWS_API = "https://mybusiness.googleapis.com/v4"

RETRY_STATUS = {429, 500, 502, 503, 504}
MAX_TRIES = 8
MAX_BACKOFF = 120.0

log = logging.getLogger("tap-googlebusinessprofile")


class AuthError(RuntimeError):
    """Google refused the sign-in (invalid_grant: revoked or expired refresh token)."""


class APIError(RuntimeError):
    def __init__(self, status: int, message: str, url: str) -> None:
        self.status = status
        super().__init__(f"HTTP {status} from {urlparse(url).netloc}{urlparse(url).path}: {message}")


class Pace:
    """At most `per_minute` request starts per minute, spread evenly, across threads."""

    def __init__(self, per_minute: int) -> None:
        self.interval = 60.0 / per_minute if per_minute > 0 else 0.0
        self._next = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        if not self.interval:
            return
        with self._lock:
            now = time.monotonic()
            start = max(now, self._next)
            self._next = start + self.interval
        if start > now:
            time.sleep(start - now)


class GBPClient:
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        *,
        requests_per_minute: int = 240,
        session: requests.Session | None = None,
        sleep: Any = time.sleep,
    ) -> None:
        self._creds = {"client_id": client_id, "client_secret": client_secret, "refresh_token": refresh_token}
        self._per_minute = requests_per_minute
        self._paces: dict[str, Pace] = {}
        self._session = session or requests.Session()
        self._token: str | None = None
        self._token_expires = 0.0
        self._lock = threading.Lock()
        self._sleep = sleep
        self.calls: dict[str, int] = {}

    # --- auth -----------------------------------------------------------------------------
    def _access_token(self, *, force: bool = False) -> str:
        with self._lock:
            if not force and self._token and time.monotonic() < self._token_expires - 120:
                return self._token
            response = self._session.post(TOKEN_URL, data={**self._creds, "grant_type": "refresh_token"}, timeout=30)
            try:
                body = response.json()
            except ValueError:
                body = {}
            if response.status_code >= 400 or not body.get("access_token"):
                error = body.get("error") if isinstance(body, dict) else None
                if error in ("invalid_grant", "unauthorized_client", "invalid_client"):
                    raise AuthError(f"Google refused the sign-in: {error} ({body.get('error_description', '')})")
                raise APIError(response.status_code, str(error or "token refresh failed"), TOKEN_URL)
            self._token = body["access_token"]
            self._token_expires = time.monotonic() + float(body.get("expires_in") or 3600)
            return self._token

    # --- requests -----------------------------------------------------------------------
    def _pace(self, url: str) -> Pace:
        host = urlparse(url).netloc
        with self._lock:
            if host not in self._paces:
                self._paces[host] = Pace(self._per_minute)
            self.calls[host] = self.calls.get(host, 0) + 1
            return self._paces[host]

    def get(self, url: str, params: Any = None) -> dict[str, Any]:
        """GET a JSON body. Retries 429/5xx and dropped connections; a 401 refreshes the token once."""
        refreshed = False
        for attempt in range(1, MAX_TRIES + 1):
            self._pace(url).wait()
            headers = {"Authorization": f"Bearer {self._access_token()}"}
            try:
                response = self._session.get(url, params=params, headers=headers, timeout=120)
            except (requests.ConnectionError, requests.Timeout) as exc:
                if attempt == MAX_TRIES:
                    raise
                self._backoff(attempt, url, type(exc).__name__)
                continue
            if response.status_code == 401 and not refreshed:
                refreshed = True
                self._access_token(force=True)
                continue
            if response.status_code in RETRY_STATUS and attempt < MAX_TRIES:
                self._backoff(attempt, url, f"HTTP {response.status_code}")
                continue
            if response.status_code >= 400:
                raise APIError(response.status_code, _message(response), url)
            if not response.content:
                return {}
            return response.json()
        raise AssertionError("unreachable")

    def _backoff(self, attempt: int, url: str, why: str) -> None:
        delay = min(MAX_BACKOFF, 2 ** attempt) + random.uniform(0, 1)  # noqa: S311
        log.info("retrying %s after %s (attempt %s, %.0f s)", urlparse(url).path, why, attempt, delay)
        self._sleep(delay)

    def pages(self, url: str, params: dict[str, Any] | list[tuple[str, Any]], items: str) -> Iterator[dict[str, Any]]:
        """Every item of a paged list (nextPageToken)."""
        token: str | None = None
        while True:
            query = list(params.items()) if isinstance(params, dict) else list(params)
            if token:
                query.append(("pageToken", token))
            body = self.get(url, query)
            yield from body.get(items) or []
            token = body.get("nextPageToken")
            if not token:
                return


def _message(response: requests.Response) -> str:
    try:
        body = response.json()
    except ValueError:
        return response.text[:200]
    error = body.get("error") if isinstance(body, dict) else None
    if isinstance(error, dict):
        return str(error.get("message") or error.get("status") or "")
    return str(error or body)[:200]
