# tap-googlebusinessprofile

Singer tap for Google Business Profile, built with the Meltano Singer SDK. It replaces Growth
Nirvana's Business Profile Cloud Function connector (growth-nirvana/business_profile_function)
with the same tables and columns, and runs on aria-runner through aria-singer-taps
(connector `google_business_profile`).

| Stream | API | Key | Incremental |
|---|---|---|---|
| `account` | Account Management v1 `accounts` | `name` | read whole |
| `location` | Business Information v1 `accounts.locations.list` (the old connector's read mask) | `name` | read whole |
| `location_daily_metrics` | Performance v1 `locations.fetchMultiDailyMetricsTimeSeries` (one call per location, all metrics) | `location_name`, `daily_metric`, `date` | per location: bookmark less `lookback_days` |
| `location_search_keyword_impressions` | Performance v1 `locations.searchkeywords.impressions.monthly` (one call per location-month: the API sums a range) | `location_name`, `search_keyword`, `start_date`, `end_date` | per location: the months touched by bookmark less `lookback_days` |
| `location_reviews` | My Business v4 `accounts.locations.reviews.list`, newest update first | `review_id` | per location: reviews updated since bookmark less `lookback_days` |

Objects (addresses, hours, categories, reviewer, reply) are JSON text with snake_case keys, as
the old connector stored them. A location the API refuses (403/404: unverified, suspended,
removed) is skipped with a `GBP LOCATION SKIPPED` line on stderr.

Locations are fetched `max_workers` at a time; each API is paced to `requests_per_minute`, and
429/5xx answers back off and retry.

## Config

| Key | |
|---|---|
| `client_id`, `client_secret`, `refresh_token` | Google OAuth (scope `https://www.googleapis.com/auth/business.manage`) |
| `account_ids` | accounts whose locations sync (empty: every account the sign-in sees) |
| `location_ids` | only these locations (empty: all of the accounts') |
| `start_date` | first day of a location's first sync |
| `end_date` | default today (UTC) |
| `lookback_days` | default 30 |
| `daily_metrics` | default the ten Growth Nirvana syncs |
| `max_workers` | default 8 |
| `requests_per_minute` | per API, default 240 |

## Development

```bash
uv sync
uv run pytest
```
