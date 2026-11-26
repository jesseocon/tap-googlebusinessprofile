"""The tap against a fake Google: the requests it makes, the rows and the state it writes."""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import urlparse

import pytest

from tap_googlebusinessprofile import client as client_mod
from tap_googlebusinessprofile.client import GBPClient
from tap_googlebusinessprofile.streams import DEFAULT_DAILY_METRICS, SKIP_MARKER, months, snake
from tap_googlebusinessprofile.tap import TapGoogleBusinessProfile

CONFIG = {
    "client_id": "cid",
    "client_secret": "csecret",
    "refresh_token": "rtoken",
    "account_ids": ["111"],
    "start_date": "2026-09-29",
    "end_date": "2026-10-01",
    "max_workers": 3,
    "requests_per_minute": 0,
}


class Response:
    def __init__(self, status: int, body: Any) -> None:
        self.status_code = status
        self._body = body
        self.content = json.dumps(body).encode()
        self.text = self.content.decode()

    def json(self) -> Any:
        return self._body


class FakeGoogle:
    """Answers the Business Profile APIs for account 111: locations 1 and 2 (2 is unverified:
    Performance 403s), and 3 under account 222 (not chosen)."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, list[tuple[str, Any]]]] = []
        self.token_posts = 0
        self.fail_once: dict[str, int] = {}

    def post(self, url: str, data: dict, timeout: int) -> Response:
        assert url == client_mod.TOKEN_URL and data["refresh_token"] == "rtoken"
        self.token_posts += 1
        return Response(200, {"access_token": f"at{self.token_posts}", "expires_in": 3599})

    def get(self, url: str, params: Any, headers: dict, timeout: int) -> Response:
        assert headers["Authorization"].startswith("Bearer at")
        query = list(params.items()) if isinstance(params, dict) else list(params or [])
        self.calls.append((url, query))
        path = urlparse(url).path
        if self.fail_once.get(path):
            self.fail_once[path] -= 1
            return Response(429, {"error": {"message": "Quota exceeded", "status": "RESOURCE_EXHAUSTED"}})
        q = dict(query)
        if path == "/v1/accounts/111":
            return Response(200, {"name": "accounts/111", "accountName": "Brand", "type": "LOCATION_GROUP", "verificationState": "VERIFIED"})
        if path == "/v1/accounts/111/locations":
            assert "storefront_address" in q["readMask"] and q["pageSize"] == 100
            if q.get("pageToken") == "p2":
                return Response(200, {"locations": [{"name": "locations/1", "title": "One", "phoneNumbers": {"primaryPhone": "555"}}]})
            return Response(200, {"locations": [{"name": "locations/2", "title": "Two", "labels": ["a"]}], "nextPageToken": "p2"})
        if path == "/v1/locations/1:fetchMultiDailyMetricsTimeSeries":
            metrics = [v for k, v in query if k == "dailyMetrics"]
            assert metrics == DEFAULT_DAILY_METRICS
            return Response(200, {"multiDailyMetricTimeSeries": [{"dailyMetricTimeSeries": [
                {"dailyMetric": m, "timeSeries": {"datedValues": [
                    {"date": {"year": 2026, "month": 9, "day": 30}, "value": "7"},
                    {"date": {"year": 2026, "month": 9, "day": 29}},
                ]}} for m in metrics
            ]}]})
        if path.startswith("/v1/locations/2"):
            return Response(403, {"error": {"message": "The caller does not have permission", "status": "PERMISSION_DENIED"}})
        if path == "/v1/locations/1/searchkeywords/impressions/monthly":
            month = q["monthlyRange.startMonth.month"]
            assert q["monthlyRange.endMonth.month"] == month
            return Response(200, {"searchKeywordsCounts": [
                {"searchKeyword": f"pizza {month}", "insightsValue": {"value": "40"}},
                {"searchKeyword": "rare", "insightsValue": {"threshold": "15"}},
            ]})
        if path.endswith("/reviews"):
            assert path.startswith("/v4/accounts/111/locations/")
            assert q["orderBy"] == "updateTime desc"
            if path == "/v4/accounts/111/locations/2/reviews":
                return Response(200, {})
            if q.get("pageToken") == "r2":
                return Response(200, {"reviews": [{"reviewId": "old", "updateTime": "2026-01-01T00:00:00Z", "starRating": "ONE"}]})
            return Response(200, {"reviews": [
                {"reviewId": "new", "name": "accounts/111/locations/1/reviews/new", "updateTime": "2026-10-01T10:00:00Z",
                 "createTime": "2026-09-30T10:00:00Z", "starRating": "FIVE", "comment": "Great",
                 "reviewer": {"displayName": "Ann", "isAnonymous": False}, "reviewReply": {"comment": "Thanks", "updateTime": "2026-10-01T10:00:00Z"}},
            ], "nextPageToken": "r2"})
        raise AssertionError(f"unexpected GET {url}")


def run(monkeypatch, capsys, fake: FakeGoogle, config: dict | None = None, state: dict | None = None) -> list[dict]:
    def make_client(self):  # noqa: ANN001, ANN202
        c = self.config
        return GBPClient(c["client_id"], c["client_secret"], c["refresh_token"],
                         requests_per_minute=c["requests_per_minute"], session=fake, sleep=lambda s: None)

    monkeypatch.setattr(TapGoogleBusinessProfile, "make_client", make_client)
    tap = TapGoogleBusinessProfile(config={**CONFIG, **(config or {})}, state=state or {}, setup_mapper=True)
    tap.sync_all()
    return [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.startswith("{")]


def records(messages: list[dict], stream: str) -> list[dict]:
    return [m["record"] for m in messages if m["type"] == "RECORD" and m["stream"] == stream]


def test_full_sync(monkeypatch, capsys, caplog):
    fake = FakeGoogle()
    fake.fail_once["/v1/locations/1:fetchMultiDailyMetricsTimeSeries"] = 1
    caplog.set_level("WARNING")
    messages = run(monkeypatch, capsys, fake)

    assert records(messages, "account") == [{
        "name": "accounts/111", "account_name": "Brand", "type": "LOCATION_GROUP", "verification_state": "VERIFIED",
        "vetted_state": None, "role": None, "permission_level": None, "primary_owner": None, "account_number": None,
        "organization_info": None,
    }]
    locs = records(messages, "location")
    assert [r["name"] for r in locs] == ["locations/1", "locations/2"]
    assert locs[0]["phone_numbers"] == '{"primary_phone":"555"}' and locs[0]["account_name"] == "accounts/111"
    assert locs[1]["labels"] == ["a"] and locs[1]["title"] == "Two"

    daily = records(messages, "location_daily_metrics")
    # One call for all ten metrics (after one 429), every metric and day, nulls kept; nothing for 2.
    metric_calls = [u for u, _ in fake.calls if "fetchMulti" in u]
    assert metric_calls.count("https://businessprofileperformance.googleapis.com/v1/locations/1:fetchMultiDailyMetricsTimeSeries") == 2
    assert len(daily) == 20 and {r["location_name"] for r in daily} == {"locations/1"}
    assert {"date": "2026-09-30", "location_name": "locations/1", "daily_metric": "CALL_CLICKS", "value": 7} in daily
    assert {"date": "2026-09-29", "location_name": "locations/1", "daily_metric": "CALL_CLICKS", "value": None} in daily

    keywords = records(messages, "location_search_keyword_impressions")
    assert {"location_name": "locations/1", "search_keyword": "pizza 9", "start_date": "2026-09-01", "end_date": "2026-09-30", "threshold": None, "value": 40} in keywords
    assert {"location_name": "locations/1", "search_keyword": "rare", "start_date": "2026-10-01", "end_date": "2026-10-31", "threshold": 15, "value": None} in keywords
    assert len(keywords) == 4

    reviews = records(messages, "location_reviews")
    assert [r["review_id"] for r in reviews] == ["old", "new"]  # first sync: every page
    new = reviews[1]
    assert new["reviewer"] == '{"display_name":"Ann","is_anonymous":false}'
    assert new["review_reply"] == '{"comment":"Thanks","update_time":"2026-10-01T10:00:00Z"}'
    assert new["account_id"] == "111" and new["location_name"] == "locations/1" and new["star_rating"] == "FIVE"

    assert any(SKIP_MARKER in r.message and "locations/2" in r.message for r in caplog.records)
    assert fake.token_posts == 1

    state = [m for m in messages if m["type"] == "STATE"][-1]["value"]
    parts = {p["context"]["location_name"]: p for p in state["bookmarks"]["location_daily_metrics"]["partitions"]}
    assert parts["locations/1"]["replication_key_value"] == "2026-09-30"


def test_incremental_window_from_each_location_bookmark(monkeypatch, capsys):
    fake = FakeGoogle()
    state = {"bookmarks": {
        "location_daily_metrics": {"partitions": [{"context": {"location_name": "locations/1"}, "replication_key": "date", "replication_key_value": "2026-09-30"}]},
        "location_search_keyword_impressions": {"partitions": [{"context": {"location_name": "locations/1"}, "replication_key": "start_date", "replication_key_value": "2026-10-01"}]},
        "location_reviews": {"partitions": [{"context": {"location_name": "locations/1"}, "replication_key": "update_time", "replication_key_value": "2026-10-01T10:00:00Z"}]},
    }}
    messages = run(monkeypatch, capsys, fake, {"start_date": "2025-01-01", "lookback_days": 3}, state)
    daily_q = dict(next(q for u, q in fake.calls if u.endswith("locations/1:fetchMultiDailyMetricsTimeSeries")))
    assert (daily_q["dailyRange.startDate.month"], daily_q["dailyRange.startDate.day"]) == (9, 27)
    # Location 2 has no bookmark: from start_date.
    daily_q2 = dict(next(q for u, q in fake.calls if u.endswith("locations/2:fetchMultiDailyMetricsTimeSeries")))
    assert daily_q2["dailyRange.startDate.year"] == 2025
    kw_months = [dict(q)["monthlyRange.startMonth.month"] for u, q in fake.calls if u.endswith("locations/1/searchkeywords/impressions/monthly")]
    assert kw_months == [9, 10]
    # Reviews stop at the first one older than the bookmark less the lookback.
    assert [r["review_id"] for r in records(messages, "location_reviews")] == ["new"]


def test_location_ids_filter_and_auth_error(monkeypatch, capsys):
    fake = FakeGoogle()
    messages = run(monkeypatch, capsys, fake, {"location_ids": ["locations/2"]})
    assert [r["name"] for r in records(messages, "location")] == ["locations/2"]
    assert records(messages, "location_daily_metrics") == []

    class Revoked(FakeGoogle):
        def post(self, url, data, timeout):  # noqa: ANN001, ANN202
            return Response(400, {"error": "invalid_grant", "error_description": "Token has been expired or revoked."})

    with pytest.raises(client_mod.AuthError, match="invalid_grant"):
        run(monkeypatch, capsys, Revoked())


def test_helpers():
    assert snake({"storefrontAddress": {"addressLines": ["1 Main"], "postalCode": "1"}}) == {
        "storefront_address": {"address_lines": ["1 Main"], "postal_code": "1"}
    }
    from datetime import date

    assert months(date(2025, 11, 15), date(2026, 1, 2)) == [date(2025, 11, 1), date(2025, 12, 1), date(2026, 1, 1)]


def test_discover_needs_no_api():
    tap = TapGoogleBusinessProfile(config=CONFIG, setup_mapper=False)
    streams = {s["tap_stream_id"]: s for s in tap.catalog_dict["streams"]}
    assert set(streams) == {"account", "location", "location_daily_metrics", "location_search_keyword_impressions", "location_reviews"}
    assert streams["location_daily_metrics"]["key_properties"] == ["location_name", "daily_metric", "date"]
