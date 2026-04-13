#!/usr/bin/env python3
"""
sdg-prime-outlier.py
━━━━━━━━━━━━━━━━━━━━
Synthetic Data Generator for the OpsPath Outlier Detection Workshop.

Generates realistic APM span data for a fictional microservices application
("OpsPath") with embedded, correlated anomaly signals that the DFA outlier
detection job can meaningfully surface.  Anomalies are not randomly injected —
they are structurally correlated across fields so the model has real multi-
dimensional patterns to learn from.

Service topology (5 microservices):
  api-gateway       → entry point, handles all inbound HTTP traffic
  order-service     → processes orders, makes DB calls + calls inventory
  inventory-service → manages stock levels, heavy DB reader
  payment-service   → payment processing, external HTTP calls
  notification-svc  → async notification dispatch, queue writer

Span types generated:
  • db          — database query spans (PostgreSQL, Redis)
  • external    — outbound HTTP to downstream services or third-parties
  • app         — internal application processing spans

Anomaly classes (~anomaly-pct % of total):
  ┌──────────────────────────┬──────────────────────────────────────────────────┐
  │ Type                     │ Correlated signals                               │
  ├──────────────────────────┼──────────────────────────────────────────────────┤
  │ slow_query               │ span.duration.us >> P99, span.type=db,           │
  │                          │ span.db.rows_affected very high                  │
  ├──────────────────────────┼──────────────────────────────────────────────────┤
  │ cascade_failure          │ span.duration.us >> P99 AND                      │
  │                          │ http.response.status_code 5xx,                   │
  │                          │ transaction.result=HTTP 5xx                      │
  ├──────────────────────────┼──────────────────────────────────────────────────┤
  │ connection_saturation    │ span.duration.us >> P99, span.subtype=redis,     │
  │                          │ span.action=connect (not query)                  │
  ├──────────────────────────┼──────────────────────────────────────────────────┤
  │ large_payload            │ http.request.body.bytes very high,               │
  │                          │ http.response.body.bytes very high,              │
  │                          │ span.duration.us moderately elevated             │
  └──────────────────────────┴──────────────────────────────────────────────────┘

Usage:
  python sdg-prime-outlier.py --days 30 --events-per-day 3000 --anomaly-pct 5
  python sdg-prime-outlier.py --backfill-only --days 7 --events-per-day 1000
  python sdg-prime-outlier.py --live-only --events-per-day 500 --anomaly-pct 5
  python sdg-prime-outlier.py --list-timezones

anomaly-pct guidance:
  DFA outlier detection works best when anomalies are genuinely rare (2–10%).
  5% is the recommended default. Values above 15% degrade outlier score
  quality — the algorithm treats anomalies as a second normal cluster rather
  than outliers.
"""

import argparse
import json
import math
import multiprocessing as _mp
import os
import random
import sys
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone, date
from queue import Queue, Empty

try:
    from elasticsearch import Elasticsearch
    from elasticsearch.helpers import parallel_bulk
except ImportError:
    print("ERROR: elasticsearch-py not installed. Run: pip install elasticsearch")
    sys.exit(1)

_HERE = os.path.dirname(os.path.realpath(os.path.abspath(__file__)))
sys.path.insert(0, _HERE)

try:
    from business_calendar import is_us_federal_holiday, is_business_day
    _HAS_CAL = True
except ImportError:
    _HAS_CAL = False

CONFIG_FILE = os.path.join(_HERE, "workshop-config-outlier.json")
# Also check the Instruqt workspace path where bootstrap writes the config
_WORKSPACE_CONFIG = "/workspace/workshop/elastic-ml-lifecycle-automation/workshop-config-outlier.json"

# ── Parallelism defaults (mirrors classification SDG) ────────────────────────

_CPU_CORES       = _mp.cpu_count() or 4
DEFAULT_WORKERS  = min(_CPU_CORES * 2, 32)
DEFAULT_PB_THREADS = min(4, max(2, _CPU_CORES // 4))
DEFAULT_BULK_SIZE  = 500
DEFAULT_PB_QUEUE   = 4

# ── Diurnal weights — business-hours peak (10:00–12:00) ──────────────────────

_WORKDAY_W = [
    0.00, 0.00, 0.00, 0.00, 0.00, 0.01,
    0.03, 0.15, 0.55, 0.80, 0.95, 1.00,
    0.98, 0.95, 0.85, 0.75, 0.60, 0.40,
    0.20, 0.08, 0.03, 0.01, 0.00, 0.00,
]
_WEEKEND_W = [
    0.00, 0.00, 0.00, 0.00, 0.00, 0.00,
    0.01, 0.02, 0.03, 0.04, 0.04, 0.04,
    0.04, 0.04, 0.03, 0.03, 0.02, 0.02,
    0.01, 0.01, 0.00, 0.00, 0.00, 0.00,
]

# ── Timezone helpers (identical pattern to classification SDG) ────────────────

def _local_tz():
    try:
        import tzlocal
        return tzlocal.get_localzone()
    except Exception:
        offset = -time.timezone if not time.daylight else -time.altzone
        return timezone(timedelta(seconds=offset))


def resolve_tz(name):
    if not name:
        return _local_tz()
    try:
        import zoneinfo
        return zoneinfo.ZoneInfo(name)
    except Exception:
        pass
    try:
        import pytz
        return pytz.timezone(name)
    except Exception:
        pass
    print(f"  ⚠ Unknown timezone {name!r} — using local")
    return _local_tz()


def tz_str(tz):
    return getattr(tz, "key", getattr(tz, "zone", str(tz)))


def list_timezones():
    try:
        import zoneinfo
        zones = sorted(zoneinfo.available_timezones())
    except ImportError:
        try:
            import pytz
            zones = sorted(pytz.all_timezones)
        except ImportError:
            print("Install zoneinfo or pytz to list timezones.")
            return
    cols, w = 3, 35
    print(f"\n{len(zones)} timezones:\n")
    for i in range(0, len(zones), cols):
        print("  " + "".join(f"{z:<{w}}" for z in zones[i : i + cols]))
    print()


# ── Calendar helpers (same pattern as classification SDG) ─────────────────────

def _is_reduced_day(d):
    if _HAS_CAL:
        return not is_business_day(d) or is_us_federal_holiday(d)
    return d.weekday() >= 5


def _day_factor(d):
    return 0.15 if _is_reduced_day(d) else 1.0


def _hour_weights(d):
    return _WEEKEND_W if _is_reduced_day(d) else _WORKDAY_W


def _hour_counts(target, weights):
    total_w = sum(weights)
    counts, allocated = [], 0
    for w in weights:
        n = round(target * w / total_w) if total_w > 0 else 0
        counts.append(n)
        allocated += n
    counts[11] += target - allocated   # adjust at peak hour
    return counts


# ── Timestamp helpers ─────────────────────────────────────────────────────────

def timestamps_for_day(day_dt, count, tz):
    """Yield ISO timestamp strings distributed by diurnal pattern."""
    weights  = _hour_weights(day_dt)
    counts   = _hour_counts(count, weights)
    day_start = datetime(day_dt.year, day_dt.month, day_dt.day, tzinfo=tz)
    for hour, n in enumerate(counts):
        if n <= 0:
            continue
        h_utc = (day_start + timedelta(hours=hour)).astimezone(timezone.utc)
        for _ in range(n):
            ts = h_utc + timedelta(seconds=random.uniform(0, 3599))
            yield ts.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"


def timestamps_from_now(count, tz):
    """Yield (ts_string, sleep_seconds) for live generation."""
    now_utc     = datetime.now(timezone.utc)
    today_local = now_utc.astimezone(tz).date()
    weights     = _hour_weights(today_local)

    remaining_weights = list(weights)
    current_hour = now_utc.astimezone(tz).hour
    for h in range(current_hour):
        remaining_weights[h] = 0

    total_w = sum(remaining_weights)
    if total_w == 0 or count <= 0:
        return

    counts    = _hour_counts(count, remaining_weights)
    day_start = datetime(today_local.year, today_local.month,
                         today_local.day, tzinfo=tz)
    events = []
    for hour, n in enumerate(counts):
        if n <= 0:
            continue
        h_utc = (day_start + timedelta(hours=hour)).astimezone(timezone.utc)
        for _ in range(n):
            ts = h_utc + timedelta(seconds=random.uniform(0, 3599))
            if ts < now_utc:
                ts = now_utc + timedelta(seconds=random.uniform(1, 30))
            events.append(ts)
    events.sort()

    prev = now_utc
    for ts in events:
        sleep_s = max(0.0, (ts - prev).total_seconds())
        ts_str  = (ts.strftime("%Y-%m-%dT%H:%M:%S.")
                   + f"{ts.microsecond // 1000:03d}Z")
        yield ts_str, sleep_s
        prev = ts


# ── Service / span constants ──────────────────────────────────────────────────

SERVICES = [
    "api-gateway",
    "order-service",
    "inventory-service",
    "payment-service",
    "notification-svc",
]

NORMAL_DURATIONS = {          # (mean_log, std_log) for log-normal sampling
    "db":       (math.log(4_500),   0.55),
    "external": (math.log(12_000),  0.65),
    "app":      (math.log(800),     0.50),
}

ANOMALY_DURATION_MULTIPLIERS = {
    "slow_query":            (35.0, 8.0),
    "cascade_failure":       (25.0, 6.0),
    "connection_saturation": (18.0, 5.0),
    "large_payload":         ( 4.0, 1.5),
}

ANOMALY_TYPES   = ["slow_query", "cascade_failure",
                   "connection_saturation", "large_payload"]
ANOMALY_WEIGHTS = [0.40, 0.30, 0.20, 0.10]

DB_SUBTYPES          = ["postgresql", "postgresql", "postgresql", "redis"]
HTTP_METHODS         = ["GET", "GET", "GET", "POST", "PUT", "DELETE"]
HTTP_STATUS_NORMAL   = [200, 200, 200, 200, 201, 204, 304]
HTTP_STATUS_ANOMALOUS = [500, 502, 503, 504, 500, 503]

TRANSACTION_NAMES = [
    "GET /api/orders",     "POST /api/orders",
    "GET /api/inventory",  "POST /api/payments",
    "GET /api/products",   "PUT /api/orders/{id}",
    "DELETE /api/orders/{id}", "GET /health",
]
TRANSACTION_TYPES = ["request", "request", "request", "background"]

SPAN_NAMES_DB = [
    "SELECT orders",        "INSERT orders",
    "UPDATE inventory",     "SELECT inventory",
    "SELECT products",      "DELETE sessions",
    "ZADD queue",           "GET cache:user",
    "SET cache:session",
]
SPAN_NAMES_EXTERNAL = [
    "GET payment-gateway/charge",   "POST stripe/v1/charges",
    "GET inventory-service/stock",  "POST notification/send",
    "GET order-service/orders",     "POST order-service/orders",
]


# ── Document builders ─────────────────────────────────────────────────────────

def _log_normal(mean_log: float, std_log: float, min_val: int = 100) -> int:
    return max(min_val, int(math.exp(random.gauss(mean_log, std_log))))


def _build_base_doc(ts: str, service: str, span_type: str,
                    duration_us: int, is_anomalous: bool,
                    anomaly_type: str | None) -> dict:
    transaction_duration_us = duration_us + _log_normal(math.log(2_000), 0.4, 200)
    doc: dict = {
        "@timestamp": ts,
        "ecs":   {"version": "8.11.0"},
        "event": {
            "kind":     "event",
            "category": ["process"],
            "type":     ["info"],
            "action":   "span",
            "outcome":  "success",
            "dataset":  "apm.spans",
            "duration": duration_us * 1000,
        },
        "service": {
            "name":        service,
            "environment": "production",
            "version":     "1.4.2",
        },
        "span": {
            "id":       uuid.uuid4().hex[:16],
            "name":     "",
            "type":     span_type,
            "subtype":  "",
            "action":   "",
            "duration": {"us": duration_us},
            "self_time": {"sum": {"us": int(duration_us * random.uniform(0.6, 0.95))}},
        },
        "transaction": {
            "id":       uuid.uuid4().hex[:16],
            "name":     random.choice(TRANSACTION_NAMES),
            "type":     random.choice(TRANSACTION_TYPES),
            "duration": {"us": transaction_duration_us},
            "result":   "HTTP 2xx",
            "sampled":  True,
            "span_count": {"started": random.randint(1, 12)},
        },
        "http": {
            "request":  {"method": random.choice(HTTP_METHODS),
                          "body":   {"bytes": random.randint(64, 8_192)}},
            "response": {"status_code": random.choice(HTTP_STATUS_NORMAL),
                          "body":        {"bytes": random.randint(128, 32_768)}},
            "version":  "1.1",
        },
        "url": {
            "original": f"http://{service}.opspath.internal/api/v1/resource",
            "domain":   f"{service}.opspath.internal",
            "path":     "/api/v1/resource",
            "port":     8080,
            "scheme":   "http",
        },
        "destination": {
            "address": "db.opspath.internal",
            "port":    5432 if span_type == "db" else 80,
        },
        "host": {
            "hostname": f"{service}-pod-{random.randint(1, 5):02d}",
            "name":     f"{service}-pod-{random.randint(1, 5):02d}",
        },
        "agent":   {"name": "python", "version": "6.20.0"},
        "process": {"pid": random.randint(1, 32767)},
        # Ground-truth labels injected by SDG — NOT in DFA analyzed_fields
        "labels": {
            "is_anomalous": is_anomalous,
            "anomaly_type": anomaly_type or "normal",
        },
    }

    # Fill type-specific span fields
    if span_type == "db":
        doc["span"]["subtype"] = random.choice(DB_SUBTYPES)
        doc["span"]["action"]  = random.choice(["query", "execute", "get", "set"])
        doc["span"]["name"]    = random.choice(SPAN_NAMES_DB)
        doc["span"]["db"] = {
            "rows_affected": random.randint(0, 250),
            "statement": random.choice([
                "SELECT id, status FROM orders WHERE user_id = ?",
                "UPDATE inventory SET quantity = quantity - ? WHERE sku = ?",
                "INSERT INTO events (type, payload) VALUES (?, ?)",
                "SELECT COUNT(*) FROM products WHERE category = ?",
                "GET cache:session:{id}",
                "ZADD pending_queue 1234567890 job:{id}",
            ]),
        }
        doc["destination"]["port"] = (
            5432 if doc["span"]["subtype"] in ("postgresql", "mysql") else 6379
        )
    elif span_type == "external":
        doc["span"]["subtype"] = "http"
        doc["span"]["action"]  = "request"
        doc["span"]["name"]    = random.choice(SPAN_NAMES_EXTERNAL)
    else:
        doc["span"]["subtype"] = "internal"
        doc["span"]["action"]  = "process"
        doc["span"]["name"]    = random.choice([
            "serialization", "cache_lookup", "auth_check",
            "validation", "business_logic",
        ])
    return doc


def make_normal_doc(ts: str, service: str) -> dict:
    span_type = random.choices(["db", "external", "app"],
                                weights=[0.45, 0.35, 0.20])[0]
    mean_log, std_log = NORMAL_DURATIONS[span_type]
    duration_us = _log_normal(mean_log, std_log, min_val=50)
    return _build_base_doc(ts, service, span_type, duration_us,
                           is_anomalous=False, anomaly_type=None)


def make_anomalous_doc(ts: str, service: str, anomaly_type: str) -> dict:
    if anomaly_type == "slow_query":
        span_type = "db"
    elif anomaly_type == "cascade_failure":
        span_type = random.choice(["external", "db"])
    elif anomaly_type == "connection_saturation":
        span_type = "db"
    else:
        span_type = "external"

    mean_log, std_log = NORMAL_DURATIONS[span_type]
    mult_mean, mult_std = ANOMALY_DURATION_MULTIPLIERS[anomaly_type]
    multiplier  = max(2.0, random.gauss(mult_mean, mult_std))
    duration_us = max(5_000, int(
        math.exp(mean_log + math.log(multiplier))
        * math.exp(random.gauss(0, std_log * 0.3))
    ))

    doc = _build_base_doc(ts, service, span_type, duration_us,
                          is_anomalous=True, anomaly_type=anomaly_type)

    # Anomaly-specific field mutations
    if anomaly_type == "slow_query":
        doc["span"]["db"] = {
            "rows_affected": random.randint(50_000, 2_000_000),
            "statement": random.choice([
                "SELECT * FROM orders WHERE created_at > '2020-01-01'",
                "SELECT * FROM inventory JOIN products ON ...",
                "UPDATE orders SET status = 'processed' WHERE ...",
            ]),
        }
        doc["span"]["subtype"] = random.choice(["postgresql", "postgresql", "mysql"])
        doc["span"]["action"]  = "query"

    elif anomaly_type == "cascade_failure":
        if span_type == "external":
            doc["http"]["response"]["status_code"] = random.choice(HTTP_STATUS_ANOMALOUS)
            doc["http"]["response"]["body"]        = {"bytes": random.randint(50, 512)}
        doc["transaction"]["result"] = random.choice(["HTTP 5xx", "HTTP 5xx", "error"])
        doc["event"]["outcome"]      = "failure"

    elif anomaly_type == "connection_saturation":
        doc["span"]["subtype"] = "redis"
        doc["span"]["action"]  = "connect"
        doc["span"]["name"]    = "CONNECT redis"

    elif anomaly_type == "large_payload":
        doc["http"]["request"]["body"]  = {"bytes": random.randint(2_000_000, 20_000_000)}
        doc["http"]["response"]["body"] = {"bytes": random.randint(5_000_000, 50_000_000)}
        doc["http"]["response"]["status_code"] = random.choice([200, 200, 206, 413])

    return doc


# ── Bulk action generator ─────────────────────────────────────────────────────

TARGET_INDEX = "traces-apm.spans-opspath"


def action_gen_day(day_dt, count, anomaly_pct, tz):
    """Yield bulk actions for one day's worth of spans."""
    anomaly_thresh = anomaly_pct / 100.0
    for ts in timestamps_for_day(day_dt, count, tz):
        service = random.choice(SERVICES)
        if random.random() < anomaly_thresh:
            atype = random.choices(ANOMALY_TYPES, weights=ANOMALY_WEIGHTS)[0]
            doc   = make_anomalous_doc(ts, service, atype)
        else:
            doc = make_normal_doc(ts, service)
        yield {"_op_type": "create", "_index": TARGET_INDEX, "_source": doc}


# ── Per-day worker (mirrors classification SDG day_worker) ───────────────────

def day_worker(es, day_dt, count, anomaly_pct,
               bulk_size, pb_threads, pb_queue, progress_q, tz):
    try:
        for ok, info in parallel_bulk(
            es,
            action_gen_day(day_dt, count, anomaly_pct, tz),
            thread_count=pb_threads,
            chunk_size=bulk_size,
            queue_size=pb_queue,
            raise_on_error=False,
            raise_on_exception=False,
            request_timeout=120,
        ):
            progress_q.put("OK" if ok else f"ERR:{info}")
    except Exception as e:
        progress_q.put(f"ERR:{TARGET_INDEX}:{day_dt}:{e}")


# ── Live generation ───────────────────────────────────────────────────────────

def live_generate(es, epd, anomaly_pct, backfill_days, tz,
                  bulk_size, pb_threads, pb_queue):
    tz_name       = tz_str(tz)
    anomaly_thresh = anomaly_pct / 100.0

    print(f"\n{'='*70}")
    print(f"  Backfill complete — {backfill_days} days indexed.")
    print(f"  Live generation: ~{epd:,} events/day  "
          f"({anomaly_pct}% anomalous).")
    print(f"  Timezone: {tz_name} | Press Ctrl-C to stop.")
    print(f"{'='*70}\n")

    indexed   = 0
    errors    = 0
    last_msg  = time.time()

    def _index_one(ts_str):
        service = random.choice(SERVICES)
        if random.random() < anomaly_thresh:
            atype = random.choices(ANOMALY_TYPES, weights=ANOMALY_WEIGHTS)[0]
            doc   = make_anomalous_doc(ts_str, service, atype)
        else:
            doc = make_normal_doc(ts_str, service)
        for ok, _ in parallel_bulk(
            es,
            [{"_op_type": "create", "_index": TARGET_INDEX, "_source": doc}],
            thread_count=1, chunk_size=1, queue_size=1,
            raise_on_error=False, raise_on_exception=False,
            request_timeout=30,
        ):
            return ok
        return False

    # ── Today's remaining events ──────────────────────────────────────────────
    today       = datetime.now(tz).date()
    day_target  = max(1, round(epd * _day_factor(today)))

    print(f"  Today ({today}): generating remaining ~{day_target:,} events…\n")

    for ts_str, sleep_s in timestamps_from_now(day_target, tz):
        if sleep_s > 0:
            time.sleep(min(sleep_s, 5.0))
        if _index_one(ts_str):
            indexed += 1
        else:
            errors += 1
        if time.time() - last_msg >= 30:
            print(f"  Live data going forward at {epd:,} events/day. "
                  f"[{indexed:,} indexed"
                  + (f", {errors} errors" if errors else "") + "]",
                  flush=True)
            last_msg = time.time()

    # ── Full days after midnight ──────────────────────────────────────────────
    print(f"\n  Today complete — entering continuous mode. Press Ctrl-C to stop.\n")
    try:
        while True:
            today      = datetime.now(tz).date()
            day_target = max(1, round(epd * _day_factor(today)))
            midnight   = (datetime(today.year, today.month, today.day, tzinfo=tz)
                          + timedelta(days=1))
            secs_left  = max(1, (midnight.astimezone(timezone.utc)
                                 - datetime.now(timezone.utc)).total_seconds())
            sleep_each = secs_left / (day_target or 1)

            while datetime.now(tz).date() == today:
                ts_str = (datetime.now(timezone.utc)
                          .strftime("%Y-%m-%dT%H:%M:%S.")
                          + f"{datetime.now(timezone.utc).microsecond // 1000:03d}Z")
                if _index_one(ts_str):
                    indexed += 1
                else:
                    errors += 1
                time.sleep(max(0.001, sleep_each))
                if time.time() - last_msg >= 30:
                    print(f"  Live data going forward at {epd:,} events/day. "
                          f"[{indexed:,} indexed"
                          + (f", {errors} errors" if errors else "") + "]",
                          flush=True)
                    last_msg = time.time()
    except KeyboardInterrupt:
        pass

    print(f"\n{'='*70}")
    print(f"  Live generation stopped. {indexed:,} docs indexed."
          + (f"  {errors} errors." if errors else ""))
    print(f"{'='*70}\n")


# ── Backfill ──────────────────────────────────────────────────────────────────

def backfill(es, host, days, epd, anomaly_pct, tz,
             workers, bulk_size, pb_threads, pb_queue):

    today     = datetime.now(tz).date()
    start_day = today - timedelta(days=days)

    # Build schedule (today excluded — same reasoning as classification SDG)
    schedule = []
    for d in range(days):
        day    = start_day + timedelta(days=d)
        factor = _day_factor(day)
        count  = max(1, round(epd * factor))
        schedule.append((day, count))

    total_docs = sum(c for _, c in schedule)
    tz_name    = tz_str(tz)

    print(f"\n{'='*70}")
    print(f"  SDG-Prime Outlier — OpsPath APM Span Generator")
    print(f"{'='*70}")
    print(f"  Stream      : {TARGET_INDEX}")
    print(f"  Days        : {days}")
    print(f"  Weekday epd : {epd:,} events/day")
    print(f"  Weekend epd : {round(epd * 0.15):,} events/day")
    print(f"  Anomaly %   : {anomaly_pct}%")
    print(f"  Timezone    : {tz_name}")
    print(f"  Window      : {start_day} → {today - timedelta(days=1)}")
    print(f"  ~Total docs : {total_docs:,}")
    print(f"  CPU cores   : {_CPU_CORES}  →  "
          f"workers={workers}, pb_threads={pb_threads}")

    if "utc" in tz_name.lower() or tz_name in ("UTC", "Etc/UTC", "GMT"):
        print()
        print("  ⚠ Timezone is UTC — if your Kibana browser is in a different")
        print("    timezone the diurnal peak will appear shifted.")
        print('    Use --timezone "America/New_York" to align with local time.')
    print()

    # Schedule preview
    print(f"  {'Date':<12} {'Day':<4} {'Type':<10} {'Docs':>8}")
    print(f"  {'-'*38}")
    for day, count in schedule:
        if _HAS_CAL and is_us_federal_holiday(day):
            dtype = "holiday"
        elif _is_reduced_day(day):
            dtype = "weekend"
        else:
            dtype = "workday"
        print(f"  {str(day):<12} {day.strftime('%a'):<4} {dtype:<10} {count:>8,}")
    print(f"  {'-'*38}")
    print(f"  {'TOTAL':<28} {total_docs:>8,}")
    print()

    # Verify data stream exists — distinguish real "not found" from
    # connection/auth/SSL errors so the message is actionable.
    print("  Checking data stream…")
    try:
        es.indices.get_data_stream(name=TARGET_INDEX)
        print(f"  ✓ {TARGET_INDEX}")
    except Exception as _ds_err:
        _msg = str(_ds_err)
        if any(x in _msg for x in ("404", "index_not_found",
                                    "no such index", "not found")):
            print(f"  ✗ {TARGET_INDEX} — data stream does not exist.")
            print( "    Run bootstrap-outlier.py first, then re-run.")
        elif any(x in _msg.lower() for x in ("401", "403",
                                               "unauthorized", "forbidden")):
            print(f"  ✗ Auth error — check --user / --password")
            print(f"    or workshop-config-outlier.json")
            print(f"    Detail: {_msg[:200]}")
        elif any(x in _msg.lower() for x in ("ssl", "certificate")):
            print(f"  ✗ SSL error — try adding --no-verify-ssl")
            print(f"    Detail: {_msg[:200]}")
        elif any(x in _msg.lower() for x in
                 ("connection", "refused", "timeout", "connect")):
            print(f"  ✗ Cannot reach Elasticsearch at: {host}")
            print( "    Check the host URL and that ES is running.")
            print(f"    Detail: {_msg[:200]}")
        else:
            print(f"  ✗ Unexpected error: {_msg[:300]}")
        sys.exit(1)
    print()

    progress_q    = Queue()
    indexed_total = 0
    error_count   = 0
    start_time    = time.time()

    def printer():
        nonlocal indexed_total, error_count
        last = time.time()
        while True:
            try:
                item = progress_q.get(timeout=1)
                if item is None:
                    break
                if isinstance(item, str) and item.startswith("ERR:"):
                    error_count += 1
                    if error_count <= 50:
                        print(f"\n  ✗ {item[4:]}")
                    elif error_count == 51:
                        print("\n  (further errors suppressed)")
                else:
                    indexed_total += 1
                    now = time.time()
                    if now - last >= 10:
                        elapsed = now - start_time
                        rate    = indexed_total / elapsed if elapsed > 0 else 0
                        pct     = min(indexed_total / total_docs * 100, 100) if total_docs else 0
                        eta     = (total_docs - indexed_total) / rate if rate > 0 else 0
                        print(f"  [{pct:5.1f}%] {indexed_total:>10,}/{total_docs:,}"
                              f"  |  {rate:>8,.0f} docs/sec"
                              f"  |  ETA {int(eta // 3600):02d}h"
                              f"{int((eta % 3600) // 60):02d}m"
                              + (f"  |  {error_count} errs" if error_count else ""))
                        last = now
            except Empty:
                continue

    t_print = threading.Thread(target=printer, daemon=True)
    t_print.start()

    # Work queue — one item per day
    work_q = Queue()
    for day, count in schedule:
        work_q.put((day, count))

    def worker():
        while True:
            try:
                day_dt, count = work_q.get_nowait()
            except Empty:
                return
            try:
                day_worker(es, day_dt, count, anomaly_pct,
                           bulk_size, pb_threads, pb_queue, progress_q, tz)
            except KeyboardInterrupt:
                return
            except Exception as e:
                progress_q.put(f"ERR:{TARGET_INDEX}:{day_dt}:{e}")
            finally:
                work_q.task_done()

    threads = [threading.Thread(target=worker, daemon=True)
               for _ in range(min(workers, days))]
    for t in threads:
        t.start()

    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        print("\n\n  Stopped early.")

    progress_q.put(None)
    t_print.join(timeout=10)

    elapsed = time.time() - start_time
    rate    = indexed_total / elapsed if elapsed > 0 else 0
    h, m, s = int(elapsed // 3600), int((elapsed % 3600) // 60), int(elapsed % 60)
    elapsed_str = f"{h:02d}h {m:02d}m {s:02d}s" if h else f"{m:02d}m {s:02d}s"

    print(f"\n{'='*70}")
    print(f"  Backfill complete — {days} days indexed in {elapsed_str}")
    print(f"  {'─'*66}")
    print(f"  Indexed : {indexed_total:,} docs"
          + (f"  [{error_count} errors]" if error_count else ""))
    print(f"  Rate    : {rate:,.0f} docs/sec")
    print(f"  Anomaly : ~{anomaly_pct}% (~"
          f"{round(total_docs * anomaly_pct / 100):,} anomalous spans)")
    print(f"{'='*70}")

    return days


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(
        description="SDG-Prime Outlier: APM span generator for the OpsPath "
                    "Outlier Detection Workshop",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Default — 30-day backfill then live generation:
  python sdg-prime-outlier.py

  # Custom parameters:
  python sdg-prime-outlier.py \\
    --days 30 --events-per-day 3000 --anomaly-pct 5 \\
    --timezone "America/New_York"

  # Backfill only:
  python sdg-prime-outlier.py --days 30 --backfill-only

  # Live only (after backfill has already run):
  python sdg-prime-outlier.py --live-only --events-per-day 3000

  # List timezones:
  python sdg-prime-outlier.py --list-timezones

anomaly-pct guidance:
  Outlier detection requires anomalies to be genuinely rare (2–10%).
  5% is the recommended default. Values above 15% degrade quality.
""",
    )
    p.add_argument("--host",           default="https://localhost:9200")
    p.add_argument("--user",           default="elastic")
    p.add_argument("--password",       default="changeme")
    p.add_argument("--no-verify-ssl",  action="store_true")
    p.add_argument("--days",           type=int,   default=30)
    p.add_argument("--events-per-day", type=int,   default=3000)
    p.add_argument("--anomaly-pct",    type=float, default=5.0)
    p.add_argument("--timezone",       default=None, metavar="TZ")
    p.add_argument("--workers",        type=int,   default=DEFAULT_WORKERS)
    p.add_argument("--bulk-size",      type=int,   default=DEFAULT_BULK_SIZE)
    p.add_argument("--pb-threads",     type=int,   default=DEFAULT_PB_THREADS)
    p.add_argument("--pb-queue",       type=int,   default=DEFAULT_PB_QUEUE)
    p.add_argument("--backfill-only",  action="store_true")
    p.add_argument("--live-only",      action="store_true")
    p.add_argument("--list-timezones", action="store_true")
    args = p.parse_args()

    if args.list_timezones:
        list_timezones()
        return

    if args.anomaly_pct > 20:
        print(f"  WARNING: --anomaly-pct {args.anomaly_pct} is very high.")
        print("  Recommended: 2–10%. Continuing anyway…\n")

    # ── Load config ───────────────────────────────────────────────────────────
    host, user, password, no_ssl = (
        args.host, args.user, args.password, args.no_verify_ssl
    )
    # Try the script-local config first, then the Instruqt workspace path
    cfg_file = CONFIG_FILE
    if not os.path.exists(cfg_file) and os.path.exists(_WORKSPACE_CONFIG):
        cfg_file = _WORKSPACE_CONFIG
    if os.path.exists(cfg_file):
        try:
            cfg      = json.load(open(cfg_file))
            host     = cfg.get("host",          host)
            user     = cfg.get("user",          user)
            password = cfg.get("password",      password)
            no_ssl   = cfg.get("no_verify_ssl", no_ssl)
            print(f"  ✓ Loaded config: {cfg_file}")
        except Exception as e:
            print(f"  ⚠ Could not read config: {e}")
    # CLI flags always win over saved config
    if args.host != "https://localhost:9200":
        host = args.host
    if args.user != "elastic":
        user = args.user
    if args.password != "changeme":
        password = args.password
    if args.no_verify_ssl:
        no_ssl = True

    verify_ssl = not no_ssl
    ssl_opts   = {"verify_certs": verify_ssl, "ssl_show_warn": False}
    if not verify_ssl:
        ssl_opts["ssl_assert_fingerprint"] = None

    es = Elasticsearch(host, basic_auth=(user, password), **ssl_opts)

    # Confirm the resolved connection settings so problems are visible
    print(f"  Host         : {host}")
    print(f"  User         : {user}")
    print(f"  Verify SSL   : {verify_ssl}")
    print(f"  Config file  : {cfg_file if os.path.exists(cfg_file) else '(not found — using CLI defaults)'}")

    tz      = resolve_tz(args.timezone)
    workers = args.workers or DEFAULT_WORKERS

    if args.live_only:
        live_generate(es, args.events_per_day, args.anomaly_pct,
                      0, tz, args.bulk_size, args.pb_threads, args.pb_queue)
        return

    backfill_days = backfill(
        es, host, args.days, args.events_per_day, args.anomaly_pct, tz,
        workers, args.bulk_size, args.pb_threads, args.pb_queue,
    )

    if not args.backfill_only:
        live_generate(es, args.events_per_day, args.anomaly_pct,
                      backfill_days, tz,
                      args.bulk_size, args.pb_threads, args.pb_queue)


if __name__ == "__main__":
    main()
