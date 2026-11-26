"""The streams: account, location, location_daily_metrics, location_search_keyword_impressions,
location_reviews. Tables and columns are those of Growth Nirvana's Business Profile connector
(growth-nirvana/business_profile_function), so its models read these as they read that one's.

Speed comes from the requests, never from less data:
- daily metrics: one fetchMultiDailyMetricsTimeSeries call per location for every metric and the
  whole window (the old connector made one call per location per metric);
- locations are fetched concurrently (max_workers), each API host paced (requests_per_minute);
- each location keeps its own bookmark, and a run re-pulls only lookback_days before it (daily
  metrics), the months those days touch (keywords), or reviews updated since (reviews).
"""

from __future__ import annotations

import calendar
import json
import logging
import re
import threading
import typing as t
from collections.abc import Callable, Iterable
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone

from singer_sdk import Stream
from singer_sdk import typing as th

from tap_googlebusinessprofile.client import (
    ACCOUNTS_API,
    PERFORMANCE_API,
    REVIEWS_API,
    APIError,
)

if t.TYPE_CHECKING:
    from tap_googlebusinessprofile.tap import TapGoogleBusinessProfile

log = logging.getLogger("tap-googlebusinessprofile")

# A location the sign-in can list but not read (unverified, suspended, removed since listing):
# skipped, as the old connector skipped 403s and 404s, and said so on stderr with this marker.
SKIP_MARKER = "GBP LOCATION SKIPPED"
SKIP_STATUS = {403, 404}

# The metrics Growth Nirvana's connector syncs (Client::LocationDailyMetrics::DAILY_METRICS).
DEFAULT_DAILY_METRICS = [
    "BUSINESS_BOOKINGS",
    "BUSINESS_CONVERSATIONS",
    "BUSINESS_DIRECTION_REQUESTS",
    "BUSINESS_FOOD_ORDERS",
    "BUSINESS_IMPRESSIONS_DESKTOP_MAPS",
    "BUSINESS_IMPRESSIONS_DESKTOP_SEARCH",
    "BUSINESS_IMPRESSIONS_MOBILE_MAPS",
    "BUSINESS_IMPRESSIONS_MOBILE_SEARCH",
    "CALL_CLICKS",
    "WEBSITE_CLICKS",
]

_CAMEL = re.compile(r"(?<=[a-z0-9])([A-Z])")


def snake(value: t.Any) -> t.Any:
    """Keys in snake_case, at every level: the old connector stored the Ruby client's to_h."""
    if isinstance(value, dict):
        return {_CAMEL.sub(r"_\1", k).lower(): snake(v) for k, v in value.items()}
    if isinstance(value, list):
        return [snake(v) for v in value]
    return value


def as_json(value: t.Any) -> str | None:
    return None if value is None else json.dumps(snake(value), separators=(",", ":"), sort_keys=True)


def as_int(value: t.Any) -> int | None:
    return None if value in (None, "") else int(value)


def parse_day(value: str) -> date:
    return date.fromisoformat(str(value)[:10])


def month_end(day: date) -> date:
    return day.replace(day=calendar.monthrange(day.year, day.month)[1])


def months(first: date, last: date) -> list[date]:
    out, day = [], first.replace(day=1)
    while day <= last:
        out.append(day)
        day = month_end(day) + timedelta(days=1)
    return out


class Prefetch:
    """Fetch locations ahead on a thread pool while the SDK syncs them one at a time, in order.

    At most `window` locations are held, so memory stays flat however many locations there are.
    """

    def __init__(self, names: list[str], fetch: Callable[[str], list[dict]], workers: int) -> None:
        self.names = names
        self.index = {n: i for i, n in enumerate(names)}
        self.fetch = fetch
        self.window = max(1, workers) * 2
        self.pool = ThreadPoolExecutor(max_workers=max(1, workers), thread_name_prefix="gbp")
        self.futures: dict[str, Future] = {}
        self.submitted = 0
        self.lock = threading.Lock()

    def result(self, name: str) -> list[dict]:
        position = self.index[name]
        with self.lock:
            while self.submitted < len(self.names) and self.submitted <= position + self.window:
                nxt = self.names[self.submitted]
                self.futures[nxt] = self.pool.submit(self.fetch, nxt)
                self.submitted += 1
            future = self.futures.pop(name)
        try:
            return future.result()
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        self.pool.shutdown(wait=False, cancel_futures=True)


class GBPStream(Stream):
    tap: TapGoogleBusinessProfile

    @property
    def gbp(self):  # noqa: ANN201
        return self._tap.gbp


class AccountStream(GBPStream):
    """The accounts the run reads (Account Management API accounts.list)."""

    name = "account"
    primary_keys: t.ClassVar[list[str]] = ["name"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("name", th.StringType, required=True),
        th.Property("account_name", th.StringType),
        th.Property("type", th.StringType),
        th.Property("verification_state", th.StringType),
        th.Property("vetted_state", th.StringType),
        th.Property("role", th.StringType),
        th.Property("permission_level", th.StringType),
        th.Property("primary_owner", th.StringType),
        th.Property("account_number", th.StringType),
        th.Property("organization_info", th.StringType),
    ).to_dict()

    def get_records(self, context: dict | None) -> Iterable[dict]:
        for account in self._tap.accounts():
            yield {
                "name": account.get("name"),
                "account_name": account.get("accountName"),
                "type": account.get("type"),
                "verification_state": account.get("verificationState"),
                "vetted_state": account.get("vettedState"),
                "role": account.get("role"),
                "permission_level": account.get("permissionLevel"),
                "primary_owner": account.get("primaryOwner"),
                "account_number": account.get("accountNumber"),
                "organization_info": as_json(account.get("organizationInfo")),
            }


# The Location fields the old connector read (Schemas::Location), in its column names. Objects
# are JSON text with snake_case keys, as it stored them.
LOCATION_FIELDS = [
    "name", "language_code", "store_code", "title", "phone_numbers", "categories", "storefront_address",
    "website_uri", "regular_hours", "special_hours", "service_area", "labels", "ad_words_location_extensions",
    "latlng", "open_info", "metadata", "profile", "relationship_data", "more_hours", "service_items",
]
LOCATION_TEXT = {"name", "language_code", "store_code", "title", "website_uri"}


class LocationStream(GBPStream):
    """Each location of the run (Business Information API accounts.locations.list)."""

    name = "location"
    primary_keys: t.ClassVar[list[str]] = ["name"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("name", th.StringType, required=True),
        th.Property("account_name", th.StringType, description="The account the location was listed under (accounts/<id>)."),
        *(
            th.Property(f, th.ArrayType(th.StringType) if f == "labels" else th.StringType)
            for f in LOCATION_FIELDS
            if f != "name"
        ),
    ).to_dict()

    def get_records(self, context: dict | None) -> Iterable[dict]:
        for name, (account, raw) in self._tap.locations().items():
            row: dict[str, t.Any] = {"name": name, "account_name": account}
            for field in LOCATION_FIELDS[1:]:
                value = raw.get(_camel(field))
                if field == "labels":
                    row[field] = value
                elif field in LOCATION_TEXT:
                    row[field] = value
                else:
                    row[field] = as_json(value)
            yield row


def _camel(field: str) -> str:
    head, *rest = field.split("_")
    return head + "".join(p.title() for p in rest)


class PerLocationStream(GBPStream):
    """A stream synced location by location (one SDK partition each, its own bookmark), fetched
    ahead on a thread pool."""

    state_partitioning_keys: t.ClassVar[list[str]] = ["location_name"]
    # Each run re-pulls days before the bookmark (lookback), so rows go back in time: the
    # bookmark moves when a location is done (progress markers), not row by row.
    is_sorted = False
    _prefetch: Prefetch | None = None

    @property
    def partitions(self) -> list[dict] | None:
        return [{"location_name": n} for n in self._tap.locations()]

    def fetch(self, location: str, start: str | None) -> list[dict]:
        raise NotImplementedError

    def get_records(self, context: dict | None) -> Iterable[dict]:
        assert context is not None
        if self._prefetch is None:
            names = [p["location_name"] for p in self.partitions or []]
            # Each location's own bookmark (None before its first sync: start_date, or every
            # review). Not get_starting_replication_key_value, which falls back to start_date.
            starts = {n: self.get_context_state({"location_name": n}).get("replication_key_value") for n in names}
            self._prefetch = Prefetch(names, lambda n: self._guarded(n, starts.get(n)), int(self.config.get("max_workers", 8)))
        rows = self._prefetch.result(context["location_name"])
        if context["location_name"] == self._prefetch.names[-1]:
            self._prefetch.close()
            self._prefetch = None
        yield from rows

    def _guarded(self, location: str, start: str | None) -> list[dict]:
        try:
            return self.fetch(location, start)
        except APIError as exc:
            if exc.status in SKIP_STATUS:
                # One line per location and stream, read by the connector (tap_notice_marker).
                log.warning("%s stream=%s location=%s status=%s %s", SKIP_MARKER, self.name, location, exc.status, exc)
                return []
            raise

    # --- the window -------------------------------------------------------------------------
    def end_day(self) -> date:
        end = self.config.get("end_date")
        return parse_day(end) if end else datetime.now(timezone.utc).date()

    def start_day(self, bookmark: str | None) -> date:
        first = parse_day(self.config["start_date"])
        if not bookmark:
            return first
        return max(first, parse_day(bookmark) - timedelta(days=int(self.config.get("lookback_days", 30))))


class LocationDailyMetricsStream(PerLocationStream):
    """Daily values of each metric (Performance API fetchMultiDailyMetricsTimeSeries): one call per
    location for all metrics. A day the API has no value for comes with a null value, as before."""

    name = "location_daily_metrics"
    primary_keys: t.ClassVar[list[str]] = ["location_name", "daily_metric", "date"]
    replication_key = "date"
    schema = th.PropertiesList(
        th.Property("date", th.DateType, required=True),
        th.Property("location_name", th.StringType, required=True),
        th.Property("daily_metric", th.StringType, required=True),
        th.Property("value", th.IntegerType),
    ).to_dict()

    def fetch(self, location: str, start: str | None) -> list[dict]:
        first, last = self.start_day(start), self.end_day()
        if first > last:
            return []
        metrics = list(self.config.get("daily_metrics") or DEFAULT_DAILY_METRICS)
        params: list[tuple[str, t.Any]] = [("dailyMetrics", m) for m in metrics]
        for edge, day in (("startDate", first), ("endDate", last)):
            params += [(f"dailyRange.{edge}.year", day.year), (f"dailyRange.{edge}.month", day.month), (f"dailyRange.{edge}.day", day.day)]
        body = self.gbp.get(f"{PERFORMANCE_API}/{location}:fetchMultiDailyMetricsTimeSeries", params)
        rows = []
        for multi in body.get("multiDailyMetricTimeSeries") or []:
            for series in multi.get("dailyMetricTimeSeries") or []:
                metric = series.get("dailyMetric")
                for dated in (series.get("timeSeries") or {}).get("datedValues") or []:
                    d = dated.get("date") or {}
                    rows.append(
                        {
                            "date": date(d["year"], d["month"], d["day"]).isoformat(),
                            "location_name": location,
                            "daily_metric": metric,
                            "value": as_int(dated.get("value")),
                        }
                    )
        rows.sort(key=lambda r: (r["date"], r["daily_metric"]))
        return rows


class LocationSearchKeywordImpressionsStream(PerLocationStream):
    """Monthly impressions per search keyword (Performance API searchkeywords.impressions.monthly).
    The API sums over the range asked for, so each month is its own call. start_date is the
    month's first day and end_date its last."""

    name = "location_search_keyword_impressions"
    primary_keys: t.ClassVar[list[str]] = ["location_name", "search_keyword", "start_date", "end_date"]
    replication_key = "start_date"
    schema = th.PropertiesList(
        th.Property("location_name", th.StringType, required=True),
        th.Property("search_keyword", th.StringType, required=True),
        th.Property("start_date", th.DateType, required=True),
        th.Property("end_date", th.DateType, required=True),
        th.Property("threshold", th.IntegerType),
        th.Property("value", th.IntegerType),
    ).to_dict()

    def fetch(self, location: str, start: str | None) -> list[dict]:
        rows = []
        for month in months(self.start_day(start), self.end_day()):
            params = {
                "monthlyRange.startMonth.year": month.year,
                "monthlyRange.startMonth.month": month.month,
                "monthlyRange.endMonth.year": month.year,
                "monthlyRange.endMonth.month": month.month,
                "pageSize": 100,
            }
            url = f"{PERFORMANCE_API}/{location}/searchkeywords/impressions/monthly"
            for count in self.gbp.pages(url, params, "searchKeywordsCounts"):
                insights = count.get("insightsValue") or {}
                rows.append(
                    {
                        "location_name": location,
                        "search_keyword": count.get("searchKeyword") or "",
                        "start_date": month.isoformat(),
                        "end_date": month_end(month).isoformat(),
                        "threshold": as_int(insights.get("threshold")),
                        "value": as_int(insights.get("value")),
                    }
                )
        return rows


class LocationReviewsStream(PerLocationStream):
    """Reviews (My Business API v4 accounts.locations.reviews.list), newest update first, read
    until one older than the bookmark less lookback_days: every review on a first sync, then the
    ones updated since (new replies too)."""

    name = "location_reviews"
    primary_keys: t.ClassVar[list[str]] = ["review_id"]
    replication_key = "update_time"
    schema = th.PropertiesList(
        th.Property("location_name", th.StringType, required=True),
        th.Property("account_id", th.StringType, required=True),
        th.Property("review_id", th.StringType, required=True),
        th.Property("reviewer", th.StringType),
        th.Property("star_rating", th.StringType),
        th.Property("comment", th.StringType),
        th.Property("create_time", th.StringType),
        th.Property("update_time", th.StringType),
        th.Property("review_reply", th.StringType),
        th.Property("name", th.StringType),
    ).to_dict()

    def fetch(self, location: str, start: str | None) -> list[dict]:
        account = self._tap.locations()[location][0].split("/")[-1]
        cutoff = (parse_day(start) - timedelta(days=int(self.config.get("lookback_days", 30)))).isoformat() if start else None
        url = f"{REVIEWS_API}/accounts/{account}/{location}/reviews"
        rows = []
        for review in self.gbp.pages(url, {"pageSize": 50, "orderBy": "updateTime desc"}, "reviews"):
            updated = review.get("updateTime") or ""
            if cutoff and updated and updated[:10] < cutoff:
                break
            rows.append(
                {
                    "location_name": location,
                    "account_id": account,
                    "review_id": review.get("reviewId"),
                    "reviewer": as_json(review.get("reviewer")),
                    "star_rating": review.get("starRating"),
                    "comment": review.get("comment"),
                    "create_time": review.get("createTime"),
                    "update_time": updated or None,
                    "review_reply": as_json(review.get("reviewReply")),
                    "name": review.get("name"),
                }
            )
        rows.sort(key=lambda r: r["update_time"] or "")
        return rows


STREAMS = [
    AccountStream,
    LocationStream,
    LocationDailyMetricsStream,
    LocationSearchKeywordImpressionsStream,
    LocationReviewsStream,
]

__all__ = ["ACCOUNTS_API", "DEFAULT_DAILY_METRICS", "STREAMS", "SKIP_MARKER"]
