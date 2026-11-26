"""GoogleBusinessProfile tap class."""

from __future__ import annotations

import logging
import threading
from typing import Any

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_googlebusinessprofile import streams
from tap_googlebusinessprofile.client import ACCOUNTS_API, INFO_API, GBPClient

log = logging.getLogger("tap-googlebusinessprofile")

# Location fields listed (readMask), the old connector's list.
READ_MASK = ",".join(streams.LOCATION_FIELDS)


def _id(value: Any) -> str:
    return str(value).strip().rsplit("/", 1)[-1]


class TapGoogleBusinessProfile(Tap):
    """Google Business Profile: accounts, locations, daily metrics, search keywords, reviews."""

    name = "tap-googlebusinessprofile"

    config_jsonschema = th.PropertiesList(
        th.Property("client_id", th.StringType, required=True, secret=True),
        th.Property("client_secret", th.StringType, required=True, secret=True),
        th.Property("refresh_token", th.StringType, required=True, secret=True),
        th.Property(
            "account_ids",
            th.ArrayType(th.StringType),
            description="Accounts whose locations sync (ids, or accounts/<id>). Empty: every account the sign-in sees.",
        ),
        th.Property(
            "location_ids",
            th.ArrayType(th.StringType),
            description="Only these locations (ids, or locations/<id>). Empty: every location of the accounts.",
        ),
        th.Property("start_date", th.DateType, required=True, description="First day of a location's first sync."),
        th.Property("end_date", th.DateType, description="Last day (default today, UTC)."),
        th.Property(
            "lookback_days",
            th.IntegerType,
            default=30,
            description="Days before each location's bookmark pulled again every run (Google restates recent days).",
        ),
        th.Property(
            "daily_metrics",
            th.ArrayType(th.StringType),
            description="DailyMetric values to pull. Default: the ten Growth Nirvana's connector syncs.",
        ),
        th.Property("max_workers", th.IntegerType, default=8, description="Locations fetched at once."),
        th.Property(
            "requests_per_minute",
            th.IntegerType,
            default=240,
            description="Most requests a run sends per minute to each Business Profile API (the project's quotas are 300-600).",
        ),
    ).to_dict()

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._gbp: GBPClient | None = None
        self._accounts: list[dict] | None = None
        self._locations: dict[str, tuple[str, dict]] | None = None
        self._lock = threading.Lock()
        super().__init__(*args, **kwargs)

    @property
    def gbp(self) -> GBPClient:
        if self._gbp is None:
            self._gbp = self.make_client()
        return self._gbp

    def make_client(self) -> GBPClient:
        rpm = self.config.get("requests_per_minute")
        return GBPClient(
            self.config["client_id"],
            self.config["client_secret"],
            self.config["refresh_token"],
            requests_per_minute=240 if rpm is None else int(rpm),
        )

    def accounts(self) -> list[dict]:
        """The run's accounts: account_ids (each read), else every account the sign-in lists."""
        with self._lock:
            if self._accounts is None:
                wanted = [_id(a) for a in self.config.get("account_ids") or []]
                if wanted:
                    self._accounts = [self.gbp.get(f"{ACCOUNTS_API}/accounts/{a}") for a in dict.fromkeys(wanted)]
                else:
                    self._accounts = list(self.gbp.pages(f"{ACCOUNTS_API}/accounts", {"pageSize": 20}, "accounts"))
            return self._accounts

    def locations(self) -> dict[str, tuple[str, dict]]:
        """locations/<id> -> (accounts/<id> it was listed under, the Location), in id order. A
        location reachable through two accounts is read once, under the first."""
        accounts = self.accounts()
        with self._lock:
            if self._locations is None:
                only = {_id(x) for x in self.config.get("location_ids") or []}
                found: dict[str, tuple[str, dict]] = {}
                for account in accounts:
                    url = f"{INFO_API}/{account['name']}/locations"
                    for loc in self.gbp.pages(url, {"pageSize": 100, "readMask": READ_MASK}, "locations"):
                        name = loc.get("name")
                        if name and (not only or _id(name) in only):
                            found.setdefault(name, (account["name"], loc))
                if only and (missing := sorted(only - {_id(n) for n in found})):
                    log.warning("%s locations not listed by the chosen accounts: %s", len(missing), ", ".join(missing[:20]))
                self._locations = dict(sorted(found.items(), key=lambda kv: kv[0]))
                log.info("%s accounts, %s locations", len(accounts), len(self._locations))
            return self._locations

    def discover_streams(self) -> list[streams.GBPStream]:
        return [cls(tap=self) for cls in streams.STREAMS]

    def sync_all(self) -> None:
        try:
            super().sync_all()
        finally:
            if self._gbp is not None:
                log.info("requests per API: %s", self._gbp.calls)


if __name__ == "__main__":
    TapGoogleBusinessProfile.cli()
