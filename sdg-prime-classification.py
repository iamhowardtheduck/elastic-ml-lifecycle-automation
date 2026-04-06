#!/usr/bin/env python3
"""
sdg-prime-classification.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━
Focused synthetic data generator for the Elastic ML Classification Workshop.
Generates ONLY the three data streams needed for:

  • mortgage-audit-classification
      → logs-mortgage.audit-default
  • mortgage-privileged-access-classification
      → logs-ping_one.audit-mortgage
      → logs-oracle.database_audit-mortgage

Key controls
────────────
  --days           Historical window in days          (default: 30)
  --events-per-day Max events per day (weekdays)      (default: 2000)
  --anomaly-pct    % of events that are anomalies     (default: 15)
  --timezone       Local timezone for diurnal pattern (auto-detected)
  --backfill-only  Stop after backfill, do not start live generation
  --live-only      Skip backfill entirely, start live generation immediately

Live generation
───────────────
After the historical backfill completes, the generator automatically transitions
to live mode:

  • Calculates how many events were already written for today during backfill
  • Generates only the remaining events for today at the correct real-time rate
  • After midnight rolls into full days, respecting the calendar and diurnal pattern
  • Live mode runs until Ctrl+C

Example: --events-per-day 100000, today is a weekday, backfill wrote 25,750 events
for today → live mode generates 74,250 more events spread across the remaining hours.

Anomaly embedding
─────────────────
For audit events:
  audit.is_suspicious = true  AND  audit.risk_score skewed HIGH (70-100)
  + correlated signals: audit.off_hours=true, audit.new_device=true,
    audit.mfa_used=false, high-risk event.action, foreign source.geo

For PingOne events:
  ping_one.audit.risk.level = HIGH  AND  ping_one.audit.risk.score skewed HIGH
  + correlated signals: event.outcome=failure, USER.MFA.BYPASS action,
    foreign geo

For Oracle events:
  oracle.database_audit.privilege = SYSDBA/DBA
  oracle.database_audit.action = DROP TABLE / DROP USER / GRANT / EXPORT
  event.outcome = success (success + high privilege = suspicious)

Usage
─────
  python sdg-prime-classification.py \\
      --host https://localhost:9200 \\
      --user elastic --password changeme \\
      --no-verify-ssl

  # Custom parameters:
  python sdg-prime-classification.py ... \\
      --days 60 --events-per-day 5000 --anomaly-pct 20

  # List timezones:
  python sdg-prime-classification.py --list-timezones
"""

import argparse
import ipaddress
import json
import os
import random
import sys
import time
import threading
import uuid
from datetime import datetime, timedelta, timezone, date
from queue import Queue, Empty

try:
    from elasticsearch import Elasticsearch
    from elasticsearch.helpers import parallel_bulk
except ImportError:
    print("ERROR: elasticsearch-py not installed.  Run: pip install elasticsearch")
    sys.exit(1)

_HERE = os.path.dirname(os.path.realpath(os.path.abspath(__file__)))
sys.path.insert(0, _HERE)

try:
    from business_calendar import is_us_federal_holiday, is_business_day
    _HAS_CAL = True
except ImportError:
    _HAS_CAL = False


# ── Constants ──────────────────────────────────────────────────────────────────

import multiprocessing as _mp

DEFAULT_DAYS        = 30
DEFAULT_EPD         = 2000     # events per weekday
DEFAULT_ANOMALY_PCT = 15       # percent of events that are anomalous

# Auto-detect CPU cores and derive sensible I/O-bound defaults.
# Bulk indexing is network/I/O bound so 2× cores for workers is efficient.
# pb_threads is per-worker thread count inside parallel_bulk — keep low
# to avoid overwhelming the ES node with too many concurrent connections.
_CPU_CORES    = _mp.cpu_count() or 4
DEFAULT_WORKERS    = min(_CPU_CORES * 2, 32)   # cap at 32
DEFAULT_PB_THREADS = min(4, max(2, _CPU_CORES // 4))
DEFAULT_BULK_SIZE  = 500
DEFAULT_PB_QUEUE   = 4

# Diurnal weights — business-hours peak (10:00–14:00)
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


# ── Timezone helpers ───────────────────────────────────────────────────────────

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
    return getattr(tz, 'key', getattr(tz, 'zone', str(tz)))

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
        print("  " + "".join(f"{z:<{w}}" for z in zones[i:i+cols]))
    print()


# ── Calendar helpers ───────────────────────────────────────────────────────────

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
    # Adjust at peak hour (11)
    counts[11] += target - allocated
    return counts


# ── Timestamp generator ────────────────────────────────────────────────────────

def timestamps_for_day(day_dt, count, tz):
    """Yield ISO timestamp strings distributed by diurnal pattern."""
    weights = _hour_weights(day_dt)
    counts  = _hour_counts(count, weights)
    day_start = datetime(day_dt.year, day_dt.month, day_dt.day, tzinfo=tz)
    for hour, n in enumerate(counts):
        if n <= 0:
            continue
        h_local = day_start + timedelta(hours=hour)
        h_utc   = h_local.astimezone(timezone.utc)
        for _ in range(n):
            ts = h_utc + timedelta(seconds=random.uniform(0, 3599))
            yield ts.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond//1000:03d}Z"


def timestamps_from_now(count, end_of_day_utc, tz):
    """Yield (ts_string, sleep_seconds) for live generation.

    Distributes `count` events between now and end_of_day_utc using the
    diurnal weights for today, but only considering hours from now onwards.
    Yields each timestamp with the real-time sleep interval before it should
    be indexed so the event stream feels live.
    """
    now_utc = datetime.now(timezone.utc)
    today_local = now_utc.astimezone(tz).date()
    weights = _hour_weights(today_local)

    # Only consider hours from current hour through end of day
    current_hour = now_utc.astimezone(tz).hour
    remaining_weights = list(weights)
    for h in range(current_hour):
        remaining_weights[h] = 0

    total_w = sum(remaining_weights)
    if total_w == 0 or count <= 0:
        return

    counts = _hour_counts(count, remaining_weights)

    # Build a sorted list of (utc_datetime, ) for each event
    events = []
    day_start = datetime(today_local.year, today_local.month, today_local.day, tzinfo=tz)
    for hour, n in enumerate(counts):
        if n <= 0:
            continue
        h_local = day_start + timedelta(hours=hour)
        h_utc   = h_local.astimezone(timezone.utc)
        for _ in range(n):
            ts = h_utc + timedelta(seconds=random.uniform(0, 3599))
            # Only generate timestamps in the future (or very recent past)
            if ts < now_utc:
                ts = now_utc + timedelta(seconds=random.uniform(1, 30))
            events.append(ts)

    events.sort()

    prev = now_utc
    for ts in events:
        sleep_s = max(0.0, (ts - prev).total_seconds())
        ts_str  = ts.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond//1000:03d}Z"
        yield ts_str, sleep_s
        prev = ts


# ── Value pools ────────────────────────────────────────────────────────────────

_FIRST_NAMES = ["James","Mary","Robert","Patricia","John","Jennifer","Michael","Linda",
                "William","Barbara","David","Susan","Richard","Jessica","Joseph","Sarah",
                "Thomas","Karen","Charles","Lisa","Christopher","Nancy","Daniel","Betty",
                "Matthew","Margaret","Anthony","Sandra","Mark","Ashley","Donald","Dorothy",
                "Steven","Kimberly","Paul","Emily","Andrew","Donna","Joshua","Michelle"]
_LAST_NAMES  = ["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis",
                "Rodriguez","Martinez","Hernandez","Lopez","Gonzalez","Wilson","Anderson",
                "Thomas","Taylor","Moore","Jackson","Martin","Lee","Perez","Thompson",
                "White","Harris","Sanchez","Clark","Ramirez","Lewis","Robinson","Walker"]

_US_CITIES   = [("New York","New York","US"),("Los Angeles","California","US"),
                ("Chicago","Illinois","US"),("Houston","Texas","US"),
                ("Phoenix","Arizona","US"),("Philadelphia","Pennsylvania","US"),
                ("San Antonio","Texas","US"),("San Diego","California","US"),
                ("Dallas","Texas","US"),("San Jose","California","US"),
                ("Austin","Texas","US"),("Indianapolis","Indiana","US")]
_FOREIGN_CITIES = [("Lagos","Lagos","NG"),("Moscow","Moscow","RU"),
                   ("Beijing","Beijing","CN"),("São Paulo","São Paulo","BR"),
                   ("Mexico City","CDMX","MX"),("Accra","Greater Accra","GH"),
                   ("Minsk","Minsk","BY"),("Bucharest","Bucharest","RO")]

_INTERNAL_IPS = ["10.0.0.0/8","172.16.0.0/12"]
_SUSPICIOUS_IPS = ["185.220.101.0/24","195.54.160.0/24","91.108.4.0/24"]

_USERS = [
    ("LO-001","loan_officer"),("LO-002","loan_officer"),("LO-003","loan_officer"),
    ("LO-004","loan_officer"),("LO-005","loan_officer"),("LO-006","loan_officer"),
    ("PROC-01","processor"),("PROC-02","processor"),
    ("UW-01","underwriter"),("UW-02","underwriter"),("UW-03","underwriter"),
    ("ADMIN-01","admin"),("SYS-BOT","system"),("COMPLIANCE-01","compliance"),
]
_NORMAL_ACTIONS = ["user_login","user_logout","document_accessed","mfa_success",
                   "mfa_challenge","permission_granted","credit_report_accessed"]
_SUSPICIOUS_ACTIONS = ["admin_impersonation","bulk_export","ssn_accessed",
                       "rate_overridden","loan_amount_modified","api_key_created",
                       "config_changed","fee_waived","permission_denied","account_locked"]

_PINGONE_NORMAL_ACTIONS  = ["USER.ACCESS_ALLOWED","USER.AUTHENTICATION.SUCCESS",
                             "USER.MFA.SUCCESS","FLOW.EXECUTION.SUCCESS"]
_PINGONE_ANOMALY_ACTIONS = ["USER.MFA.BYPASS","USER.AUTHENTICATION.FAILED",
                             "USER.ACCOUNT.LOCKED","ADMIN.USER.CREATED",
                             "ADMIN.POLICY.UPDATED","USER.ROLE.GRANTED"]

_ORACLE_NORMAL_ACTIONS    = ["LOGON","LOGOFF","SELECT","INSERT","UPDATE"]
_ORACLE_ANOMALY_ACTIONS   = ["DROP TABLE","DROP USER","CREATE USER","GRANT",
                              "EXPORT","CREATE OR REPLACE PROCEDURE","ALTER USER"]
_ORACLE_NORMAL_PRIVS  = ["NOT APPLICABLE","CREATE SESSION","SELECT ANY TABLE"]
_ORACLE_ANOMALY_PRIVS = ["SYSDBA","DBA","CREATE ANY PROCEDURE"]


def _rand_name():
    return f"{random.choice(_FIRST_NAMES)} {random.choice(_LAST_NAMES)}"

def _rand_email(name):
    return f"{name.lower().replace(' ','.')}{random.randint(1,99)}@lendpath.com"

def _rand_ip(cidr):
    net = ipaddress.IPv4Network(cidr, strict=False)
    return str(ipaddress.IPv4Address(
        random.randint(int(net.network_address)+1, int(net.broadcast_address)-1)
    ))

def _geo(anomaly=False):
    if anomaly and random.random() < 0.6:
        city, region, country = random.choice(_FOREIGN_CITIES)
    else:
        city, region, country = random.choice(_US_CITIES)
    return city, region, country

def _src_ip(anomaly=False):
    if anomaly and random.random() < 0.5:
        return _rand_ip(random.choice(_SUSPICIOUS_IPS))
    return _rand_ip(random.choice(_INTERNAL_IPS))


# ── Document builders ──────────────────────────────────────────────────────────

def make_audit_doc(ts, anomaly_pct):
    """Build a logs-mortgage.audit-default document."""
    is_anomaly = random.random() < (anomaly_pct / 100.0)
    user_id, role = random.choice(_USERS)
    name = _rand_name()
    city, region, country = _geo(anomaly=is_anomaly)

    if is_anomaly:
        risk_score  = round(random.uniform(65, 100), 2)
        action      = random.choice(_SUSPICIOUS_ACTIONS)
        off_hours   = random.random() < 0.75
        new_device  = random.random() < 0.70
        mfa_used    = random.random() < 0.20
        vpn         = random.random() < 0.50
        outcome     = random.choices(["success","failure","unknown"],
                                     weights=[40,50,10])[0]
    else:
        risk_score  = round(random.uniform(0, 50), 2)
        action      = random.choice(_NORMAL_ACTIONS)
        off_hours   = random.random() < 0.10
        new_device  = random.random() < 0.05
        mfa_used    = random.random() < 0.85
        vpn         = random.random() < 0.15
        outcome     = random.choices(["success","failure","unknown"],
                                     weights=[85,12,3])[0]

    return {
        "@timestamp": ts,
        "ecs":        {"version": "8.11.0"},
        "event": {
            "kind":     "event",
            "category": random.choice(["authentication","authorization","iam","file"]),
            "type":     random.choice(["start","end","change","access"]),
            "action":   action,
            "outcome":  outcome,
            "dataset":  "mortgage.audit",
        },
        "user": {
            "id":    user_id,
            "name":  name,
            "email": _rand_email(name),
            "roles": [role],
        },
        "source": {
            "ip": _src_ip(anomaly=is_anomaly),
            "geo": {
                "country_iso_code": country,
                "city_name":        city,
                "region_name":      region,
            },
        },
        "audit": {
            "risk_score":    risk_score,
            "is_suspicious": is_anomaly,
            "session_id":    str(uuid.uuid4()),
            "mfa_used":      mfa_used,
            "off_hours":     off_hours,
            "new_device":    new_device,
            "vpn_detected":  vpn,
        },
        "host": {"hostname": random.choice(["audit-svc-01","audit-svc-02"]),
                 "name":     random.choice(["audit-svc-01","audit-svc-02"])},
        "message": f"{'Suspicious' if is_anomaly else 'Normal'} audit event: {action}",
        "tags":    ["mortgage-audit"],
    }


def make_pingone_doc(ts, anomaly_pct):
    """Build a logs-ping_one.audit-mortgage document."""
    is_anomaly = random.random() < (anomaly_pct / 100.0)
    name = _rand_name()
    city, region, country = _geo(anomaly=is_anomaly)

    if is_anomaly:
        risk_score   = round(random.uniform(60, 100), 2)
        risk_level   = random.choices(["HIGH","MEDIUM"],weights=[70,30])[0]
        action       = random.choice(_PINGONE_ANOMALY_ACTIONS)
        result_status= random.choices(["FAILED","BLOCKED","REQUIRES_MFA"],
                                      weights=[50,30,20])[0]
        outcome      = random.choices(["failure","success"],weights=[65,35])[0]
    else:
        risk_score   = round(random.uniform(0, 40), 2)
        risk_level   = random.choices(["LOW","MEDIUM"],weights=[85,15])[0]
        action       = random.choice(_PINGONE_NORMAL_ACTIONS)
        result_status= "SUCCESS"
        outcome      = random.choices(["success","failure"],weights=[90,10])[0]

    user_uuid = f"user-{random.randint(1000,9999)}-uuid"
    return {
        "@timestamp": ts,
        "ecs":        {"version": "8.11.0"},
        "event": {
            "kind":     "event",
            "category": random.choice(["iam","authentication","configuration"]),
            "dataset":  "ping_one.audit",
            "action":   action,
            "outcome":  outcome,
            "type":     random.choice(["user","info","access"]),
        },
        "input":  {"type": "http_endpoint"},
        "user": {
            "id":    user_uuid,
            "email": _rand_email(name),
            "name":  name,
        },
        "client": {
            "user": {
                "id":   str(uuid.uuid4()),
                "name": _rand_name(),
            }
        },
        "source": {
            "ip": _src_ip(anomaly=is_anomaly),
            "geo": {
                "country_iso_code": country,
                "city_name":        city,
                "region_name":      region,
            },
        },
        "ping_one": {
            "audit": {
                "action":  {"type": random.choice(["AUTHENTICATION","AUTHORIZATION",
                                                   "ACCOUNT","MFA","ADMIN","SAML"])},
                "actors":  {"client":{"type":"WORKER"}, "user":{"type":"USER"}},
                "result":  {"status": result_status,
                            "description": f"Risk level: {risk_level}"},
                "risk":    {"score": risk_score, "level": risk_level},
            }
        },
        "tags": ["ping_one-audit"],
    }


def make_oracle_doc(ts, anomaly_pct):
    """Build a logs-oracle.database_audit-mortgage document."""
    is_anomaly = random.random() < (anomaly_pct / 100.0)
    db_user = random.choice(["lendpath_app","lendpath_ro","lendpath_batch",
                              "dba_admin","sys","system"])
    server  = random.choice(["db-primary-01.lendpath.internal",
                              "db-replica-01.lendpath.internal"])

    if is_anomaly:
        action    = random.choice(_ORACLE_ANOMALY_ACTIONS)
        privilege = random.choice(_ORACLE_ANOMALY_PRIVS)
        outcome   = random.choices(["success","failure"],weights=[60,40])[0]
        role      = random.choice(["SYSDBA","DBA","SYSOPER"])
        obj       = random.choice(["BORROWER_PII","CREDIT_SCORES","USERS",
                                   "ROLES","INTEREST_RATES","SYSTEM_CONFIG"])
        schema    = random.choice(["SYS","SYSTEM","DBA_ADMIN"])
        result_code = random.choice([0,1017,1031,1033])
        status    = "Success" if outcome == "success" else "Failure"
    else:
        action    = random.choice(_ORACLE_NORMAL_ACTIONS)
        privilege = random.choice(_ORACLE_NORMAL_PRIVS)
        outcome   = random.choices(["success","failure"],weights=[92,8])[0]
        role      = random.choice(["CONNECT","RESOURCE","SELECT_CATALOG_ROLE"])
        obj       = random.choice(["LOAN_APPLICATIONS","APPRAISALS",
                                   "CLOSING_DOCUMENTS","FEE_SCHEDULES"])
        schema    = "LENDPATH"
        result_code = 0
        status    = "Success"

    return {
        "@timestamp": ts,
        "ecs":        {"version": "8.11.0"},
        "event": {
            "kind":     "event",
            "category": "database",
            "action":   "database_audit",
            "dataset":  "oracle.database_audit",
            "outcome":  outcome,
            "type":     "access",
        },
        "client": {"user": {"name": db_user}},
        "user":   {"roles": [role]},
        "server": {"address": server, "domain": server},
        "process":{"pid": random.randint(1, 65535)},
        "oracle": {
            "database_audit": {
                "action":          action,
                "action_number":   random.randint(1, 300),
                "database":        {"user": db_user},
                "entry":           {"id": random.randint(1, 9999999)},
                "length":          random.randint(100, 4096),
                "obj":             {"name": obj, "schema": schema},
                "privilege":       privilege,
                "result_code":     result_code,
                "session_id":      random.randint(1, 9999999),
                "status":          status,
                "terminal":        random.choice(["pts/0","pts/1","console","UNKNOWN"]),
            }
        },
        "related": {"hosts": [server]},
        "tags":    ["oracle-database-audit"],
    }


# ── Bulk action generators ─────────────────────────────────────────────────────

_BUILDERS = {
    "logs-mortgage.audit-default":       (make_audit_doc,    "create"),
    "logs-ping_one.audit-mortgage":      (make_pingone_doc,  "create"),
    "logs-oracle.database_audit-mortgage":(make_oracle_doc,  "create"),
}

def action_gen_day(index, builder, day_dt, count, anomaly_pct, tz):
    for ts in timestamps_for_day(day_dt, count, tz):
        doc = builder(ts, anomaly_pct)
        yield {"_op_type": "create", "_index": index, "_source": doc}


# ── Worker ─────────────────────────────────────────────────────────────────────

def day_worker(es, index, builder, day_dt, count, anomaly_pct,
               bulk_size, pb_threads, pb_queue, progress_q, tz):
    try:
        for ok, info in parallel_bulk(
            es,
            action_gen_day(index, builder, day_dt, count, anomaly_pct, tz),
            thread_count=pb_threads, chunk_size=bulk_size,
            queue_size=pb_queue, raise_on_error=False,
            raise_on_exception=False, request_timeout=120,
        ):
            progress_q.put("OK" if ok else f"ERR:{info}")
    except Exception as e:
        progress_q.put(f"ERR:{index}:{day_dt}:{e}")


# ── Live generation ───────────────────────────────────────────────────────────

def live_generate(es, epd, anomaly_pct, backfill_days, backfill_today_count,
                  bulk_size, pb_threads, pb_queue, tz):
    """Continuous live event generation after backfill completes.

    Args:
        epd:                   Max events per day target (weekdays)
        backfill_days:         Number of days covered by the backfill
        backfill_today_count:  Events already written for today during backfill
        tz:                    Local timezone
    """
    tz_name = tz_str(tz)
    print(f"\n{'='*70}")
    print(f"  Backfill data completed {backfill_days} days.")
    print(f"  Data going forward is being generated at {epd:,} events per day.")
    print(f"  Timezone: {tz_name} | Press Ctrl+C to stop.\n")

    indexed_live  = 0
    error_count   = 0
    start_time    = time.time()
    _last_status  = time.time()
    current_day   = datetime.now(tz).date()

    # Calculate remaining quota for today
    today_target  = max(0, round(epd * _day_factor(current_day)) - backfill_today_count)
    end_of_today  = (datetime(current_day.year, current_day.month, current_day.day,
                               tzinfo=tz) + timedelta(days=1)).astimezone(timezone.utc)

    print(f"  Today ({current_day}):")
    print(f"    Backfill wrote:    {backfill_today_count:>8,} events per stream")
    print(f"    Remaining today:   {today_target:>8,} events per stream")
    print(f"    Day rolls at:      {end_of_today.strftime('%H:%M:%S UTC')}\n")

    def _index_doc(index, builder, ts):
        """Index a single live document — returns True on success."""
        doc = builder(ts, anomaly_pct)
        try:
            for ok, info in parallel_bulk(
                es,
                [{"_op_type": "create", "_index": index, "_source": doc}],
                thread_count=1, chunk_size=1,
                queue_size=1, raise_on_error=False,
                raise_on_exception=False, request_timeout=30,
            ):
                return ok
        except Exception:
            return False
        return False

    # ── Today's remaining events ──────────────────────────────────────────────
    if today_target > 0:
        # Generate sorted event timestamps for the rest of today for all 3 streams
        # and interleave them by time
        stream_events = {}
        for index, (builder, _) in _BUILDERS.items():
            gen = list(timestamps_from_now(today_target, end_of_today, tz))
            stream_events[index] = (builder, iter(gen))

        # Drain all three stream iterators, sleeping between events
        active = {k: v for k, v in stream_events.items()}
        # Prefetch first event from each stream
        nexts = {}
        for idx, (builder, it) in active.items():
            try:
                ts, sleep_s = next(it)
                nexts[idx] = (builder, it, ts, sleep_s)
            except StopIteration:
                pass

        while nexts:
            # Find the stream with the earliest next event
            earliest = min(nexts.items(), key=lambda x: x[1][2])
            idx, (builder, it, ts, sleep_s) = earliest
            if sleep_s > 0:
                time.sleep(min(sleep_s, 5.0))  # cap sleep to stay responsive
            ok = _index_doc(idx, builder, ts)
            if ok:
                indexed_live += 1
            else:
                error_count += 1
            # Advance this stream
            try:
                ts_next, sleep_next = next(it)
                nexts[idx] = (builder, it, ts_next, sleep_next)
            except StopIteration:
                del nexts[idx]
            # Progress every 500 docs
            if time.time() - _last_status >= 30:
                print(f"  Backfill data completed {backfill_days} days. "
                      f"Data going forward is being generated at {epd:,} events per day. "
                      f"[{indexed_live:,} live docs indexed"
                      + (f", {error_count} errors" if error_count else "") + "]")
                _last_status = time.time()

    # ── Full days after midnight ──────────────────────────────────────────────
    print(f"\n  Today's remaining events complete — entering continuous mode.")
    print(f"  Will generate full days as they roll. Press Ctrl+C to stop.\n")

    while True:
        now_local  = datetime.now(tz)
        today      = now_local.date()
        day_target = max(1, round(epd * _day_factor(today)))
        midnight   = (datetime(today.year, today.month, today.day, tzinfo=tz)
                      + timedelta(days=1))
        seconds_in_day   = 86400
        seconds_remaining = max(1, (midnight.astimezone(timezone.utc)
                                     - datetime.now(timezone.utc)).total_seconds())
        # Rate: spread day_target events across remaining seconds
        sleep_per_event = seconds_remaining / (day_target * len(_BUILDERS))

        day_indexed = 0
        try:
            while datetime.now(tz).date() == today:
                for index, (builder, _) in _BUILDERS.items():
                    ts = datetime.now(timezone.utc)
                    ts_str = ts.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond//1000:03d}Z"
                    ok = _index_doc(index, builder, ts_str)
                    if ok:
                        indexed_live += 1
                        day_indexed  += 1
                    else:
                        error_count += 1
                time.sleep(max(0.001, sleep_per_event))
                if time.time() - _last_status >= 30:
                    print(f"  Backfill data completed {backfill_days} days. "
                          f"Data going forward is being generated at {epd:,} events per day. "
                          f"[{indexed_live:,} live docs indexed"
                          + (f", {error_count} errors" if error_count else "") + "]")
                    _last_status = time.time()
        except KeyboardInterrupt:
            break

    elapsed = time.time() - start_time
    h = int(elapsed // 3600)
    m = int((elapsed % 3600) // 60)
    s = int(elapsed % 60)
    elapsed_str = f"{h:02d}h {m:02d}m {s:02d}s" if h else f"{m:02d}m {s:02d}s"
    print(f"\n{'='*70}")
    print(f"  Live generation stopped.")
    print(f"  Backfill: {backfill_days} days  |  Live docs indexed: {indexed_live:,}"
          + (f"  |  {error_count} errors" if error_count else ""))
    print(f"  Live elapsed: {elapsed_str}")
    print(f"{'='*70}\n")


# ── Main backfill ──────────────────────────────────────────────────────────────

def backfill(host, user, password, verify_ssl,
             days, epd, anomaly_pct, tz,
             workers, bulk_size, pb_threads, pb_queue):

    ssl_opts = {"verify_certs": verify_ssl, "ssl_show_warn": False}
    if not verify_ssl:
        ssl_opts["ssl_assert_fingerprint"] = None
    es = Elasticsearch(host, basic_auth=(user, password), **ssl_opts)

    today     = datetime.now(tz).date()
    start_day = today - timedelta(days=days - 1)
    tz_name   = tz_str(tz)

    # Build schedule
    schedule = []
    for d in range(days):
        day    = start_day + timedelta(days=d)
        factor = _day_factor(day)
        count  = max(1, round(epd * factor))
        schedule.append((day, count))

    # 3 streams × docs per day
    total_docs = sum(c for _, c in schedule) * 3

    # Print header
    print(f"\n{'='*70}")
    print(f"  SDG-Prime Classification — Focused Workshop Generator")
    print(f"{'='*70}")
    print(f"  Streams:        3 (audit, ping_one, oracle)")
    print(f"  Days:           {days}")
    print(f"  Weekday target: {epd:,} events/day per stream")
    print(f"  Weekend/holiday:{round(epd * 0.15):,} events/day per stream")
    print(f"  Anomaly %:      {anomaly_pct}%")
    print(f"  Timezone:       {tz_name}")
    print(f"  Window:         {start_day}  →  {today}")
    print(f"  ~Total docs:    {total_docs:,}")
    print(f"  CPU cores:      {_CPU_CORES}  →  workers={workers}, pb_threads={pb_threads}")
    print()

    if "utc" in tz_name.lower() or tz_name in ("UTC","Etc/UTC","GMT"):
        print("  ⚠ Timezone is UTC — if your Kibana browser is in a different")
        print("    timezone the diurnal peak will appear shifted.")
        print("    Use --timezone 'America/New_York' to align with local time.\n")

    # Schedule preview
    print(f"  {'Date':<12} {'Day':<4} {'Type':<10} {'Per stream':>12}  {'×3 total':>10}")
    print(f"  {'-'*54}")
    for day, count in schedule:
        if _HAS_CAL and is_us_federal_holiday(day):
            dtype = "holiday"
        elif _is_reduced_day(day):
            dtype = "weekend"
        else:
            dtype = "workday"
        print(f"  {str(day):<12} {day.strftime('%a'):<4} {dtype:<10} "
              f"{count:>12,}  {count*3:>10,}")
    print(f"  {'-'*54}")
    print(f"  {'TOTAL':<28} {sum(c for _,c in schedule):>12,}  "
          f"{sum(c for _,c in schedule)*3:>10,}")
    print(f"\n  Anomaly distribution:")
    print(f"    audit.is_suspicious = true:     ~{anomaly_pct}%")
    print(f"    ping_one.audit.risk.level = HIGH: ~{anomaly_pct}%")
    print(f"    Oracle SYSDBA/DBA actions:        ~{anomaly_pct}%")
    print(f"\n  Press Ctrl+C to stop.\n")

    progress_q    = Queue()
    start_time    = time.time()
    indexed_total = 0
    error_count   = 0

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
                          f"  |  ETA {int(eta//3600):02d}h{int((eta%3600)//60):02d}m"
                          + (f"  | {error_count} errs" if error_count else ""))
                    last = now
            except Empty:
                continue

    t_print = threading.Thread(target=printer, daemon=True)
    t_print.start()

    # Pre-flight: verify all three data streams exist
    print("  Checking data streams…")
    for index in _BUILDERS:
        try:
            info = es.indices.get_data_stream(name=index)
            print(f"    ✓ {index}")
        except Exception:
            print(f"    ✗ {index} — NOT FOUND. Run bootstrap-classification.py first.")

    # Work queue: one item per (stream, day)
    work_q = Queue()
    for index, (builder, _) in _BUILDERS.items():
        for day, count in schedule:
            work_q.put((index, builder, day, count))

    def worker():
        while True:
            try:
                index, builder, day_dt, count = work_q.get_nowait()
            except Empty:
                return
            try:
                day_worker(es, index, builder, day_dt, count, anomaly_pct,
                           bulk_size, pb_threads, pb_queue, progress_q, tz)
            except KeyboardInterrupt:
                return
            except Exception as e:
                progress_q.put(f"ERR:{index}:{day_dt}:{e}")
            finally:
                work_q.task_done()

    threads = [threading.Thread(target=worker, daemon=True)
               for _ in range(min(workers, len(_BUILDERS) * days))]
    for t in threads:
        t.start()
    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        print("\n\nStopped early.")

    progress_q.put(None)
    t_print.join(timeout=10)
    elapsed = time.time() - start_time
    rate    = indexed_total / elapsed if elapsed > 0 else 0

    print(f"\n{'='*70}")
    print(f"  Classification backfill complete.")
    print(f"  Indexed:  {indexed_total:,}   Errors: {error_count:,}")
    print(f"  Elapsed:  {int(elapsed//3600):02d}h"
          f"{int((elapsed%3600)//60):02d}m{int(elapsed%60):02d}s")
    print(f"  Rate:     {rate:,.0f} docs/sec")
    print()
    print(f"  Class balance written:")
    print(f"    audit.is_suspicious = true:      ~{anomaly_pct}%  "
          f"(~{round(sum(c for _,c in schedule)*anomaly_pct/100):,} docs)")
    print(f"    ping_one.audit.risk.level = HIGH: ~{anomaly_pct}%  "
          f"(~{round(sum(c for _,c in schedule)*anomaly_pct/100):,} docs)")
    print(f"    Oracle high-privilege actions:    ~{anomaly_pct}%  "
          f"(~{round(sum(c for _,c in schedule)*anomaly_pct/100):,} docs)")
    h = int(elapsed // 3600)
    m = int((elapsed % 3600) // 60)
    s = int(elapsed % 60)
    elapsed_str = f"{h:02d}h {m:02d}m {s:02d}s" if h else f"{m:02d}m {s:02d}s"
    print(f"\n{'='*70}")
    print(f"  Backfill complete — {days} days indexed in {elapsed_str}")
    print(f"  {'─'*66}")
    print(f"  Indexed:  {indexed_total:,} documents across 3 streams"
          + (f"   [{error_count} errors]" if error_count else ""))
    print(f"  Rate:     {rate:,.0f} docs/sec")
    print()
    print(f"  Class balance written:")
    print(f"    audit.is_suspicious = true:       ~{anomaly_pct}%  "
          f"(~{round(sum(c for _,c in schedule)*anomaly_pct/100):,} docs)")
    print(f"    ping_one.audit.risk.level = HIGH:  ~{anomaly_pct}%  "
          f"(~{round(sum(c for _,c in schedule)*anomaly_pct/100):,} docs)")
    print(f"    Oracle high-privilege actions:     ~{anomaly_pct}%  "
          f"(~{round(sum(c for _,c in schedule)*anomaly_pct/100):,} docs)")
    print(f"{'='*70}")

    # Return tuple so main() can pass all three values to live_generate
    today_entry = next((c for d, c in schedule if d == today), 0)
    return days, epd, today_entry


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(
        description="SDG-Prime Classification: focused generator for ML classification workshop",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Standard run — 30 days backfill then live generation:
  python sdg-prime-classification.py \\
      --host https://localhost:9200 \\
      --user elastic --password changeme --no-verify-ssl

  # High-volume with timezone (backfill + live):
  python sdg-prime-classification.py ... \\
      --days 30 --events-per-day 100000 --anomaly-pct 15 \\
      --timezone "America/New_York"

  # Backfill only — no live generation:
  python sdg-prime-classification.py ... \\
      --days 30 --events-per-day 2000 --backfill-only

  # Live only — skip backfill, generate new events going forward:
  python sdg-prime-classification.py ... \\
      --events-per-day 2000 --live-only

  # List timezones:
  python sdg-prime-classification.py --list-timezones

Live generation notes:
  After backfill completes the generator automatically calculates how many
  events remain for today and generates them in real time. For example:
    --events-per-day 100000, today backfill wrote 25,750 events
    → live generates the remaining 74,250 events across today's hours
  After today is complete it transitions to continuous full-day generation.
  Press Ctrl+C at any time to stop.
        """
    )
    p.add_argument("--host",           default="https://localhost:9200")
    p.add_argument("--user",           default="elastic")
    p.add_argument("--password",       default="changeme")
    p.add_argument("--no-verify-ssl",  action="store_true")
    p.add_argument("--days",           type=int,   default=DEFAULT_DAYS,
                   help=f"Historical window in days (default: {DEFAULT_DAYS})")
    p.add_argument("--events-per-day", "--epd", type=int, default=DEFAULT_EPD,
                   help=f"Max events per day per stream on weekdays "
                        f"(default: {DEFAULT_EPD:,})")
    p.add_argument("--anomaly-pct",    type=float, default=DEFAULT_ANOMALY_PCT,
                   help=f"Percentage of events that are anomalies "
                        f"(default: {DEFAULT_ANOMALY_PCT}). "
                        f"Affects audit.is_suspicious, ping_one.audit.risk.level, "
                        f"and Oracle privilege fields.")
    p.add_argument("--workers",  "-w", type=int,   default=DEFAULT_WORKERS,
                   help=f"Worker threads for backfill (default: {DEFAULT_WORKERS} "
                        f"= {_CPU_CORES} CPU cores × 2, capped at 32). "
                        f"Tasks are I/O bound so 2× cores is efficient.")
    p.add_argument("--bulk-size","-b", type=int,   default=DEFAULT_BULK_SIZE,
                   help=f"Documents per bulk request (default: {DEFAULT_BULK_SIZE})")
    p.add_argument("--pb-threads",     type=int,   default=DEFAULT_PB_THREADS,
                   help=f"parallel_bulk threads per worker (default: {DEFAULT_PB_THREADS} "
                        f"= cores // 4, min 2, max 4)")
    p.add_argument("--pb-queue",       type=int,   default=DEFAULT_PB_QUEUE,
                   help=f"parallel_bulk queue depth (default: {DEFAULT_PB_QUEUE})")
    p.add_argument("--timezone",       default=None, metavar="TZ",
                   help="Timezone for timestamps (default: system local).")
    p.add_argument("--list-timezones", action="store_true")
    p.add_argument("--backfill-only",  action="store_true",
                   help="Stop after backfill completes — do not start live generation.")
    p.add_argument("--live-only",      action="store_true",
                   help="Skip backfill entirely and start live generation immediately. "
                        "Useful when data already exists and you only want new events "
                        "going forward. Sets backfill_today_count=0 so the full day "
                        "target is generated for today.")

    # Read workshop-config.json if present
    _cfg = {}
    _cfg_file = os.path.join(_HERE, "workshop-config.json")
    if os.path.exists(_cfg_file):
        try:
            _cfg = json.load(open(_cfg_file))
            print(f"  ✓ Loaded config: {_cfg_file}")
        except Exception:
            pass

    args = p.parse_args()

    if args.list_timezones:
        list_timezones()
        sys.exit(0)

    # Back-fill from saved config where CLI left defaults
    _def = {"host": "https://localhost:9200", "user": "elastic", "password": "changeme"}
    if args.host     == _def["host"]     and _cfg.get("host"):     args.host     = _cfg["host"]
    if args.user     == _def["user"]     and _cfg.get("user"):     args.user     = _cfg["user"]
    if args.password == _def["password"] and _cfg.get("password"): args.password = _cfg["password"]
    if not args.no_verify_ssl            and _cfg.get("no_verify_ssl"):
        args.no_verify_ssl = _cfg["no_verify_ssl"]
    if not args.timezone                 and _cfg.get("timezone"):
        args.timezone = _cfg["timezone"]

    if args.anomaly_pct < 1 or args.anomaly_pct > 49:
        print(f"ERROR: --anomaly-pct must be between 1 and 49 "
              f"(got {args.anomaly_pct}). Values outside this range "
              f"produce unbalanced training data.")
        sys.exit(1)

    tz = resolve_tz(args.timezone)

    ssl_opts = {"verify_certs": not args.no_verify_ssl, "ssl_show_warn": False}
    if args.no_verify_ssl:
        ssl_opts["ssl_assert_fingerprint"] = None
    es = Elasticsearch(args.host, basic_auth=(args.user, args.password), **ssl_opts)

    if args.live_only:
        # Skip backfill — jump straight to live generation.
        # backfill_today_count=0 means the full day target is generated for today.
        print("\n  --live-only: skipping backfill, starting live generation immediately.")
        live_generate(
            es                   = es,
            epd                  = args.events_per_day,
            anomaly_pct          = args.anomaly_pct,
            backfill_days        = 0,
            backfill_today_count = 0,
            bulk_size            = args.bulk_size,
            pb_threads           = args.pb_threads,
            pb_queue             = args.pb_queue,
            tz                   = tz,
        )
    else:
        backfill_days, backfill_epd, backfill_today_count = backfill(
            host        = args.host,
            user        = args.user,
            password    = args.password,
            verify_ssl  = not args.no_verify_ssl,
            days        = args.days,
            epd         = args.events_per_day,
            anomaly_pct = args.anomaly_pct,
            tz          = tz,
            workers     = args.workers,
            bulk_size   = args.bulk_size,
            pb_threads  = args.pb_threads,
            pb_queue    = args.pb_queue,
        )

        if not args.backfill_only:
            live_generate(
                es                   = es,
                epd                  = args.events_per_day,
                anomaly_pct          = args.anomaly_pct,
                backfill_days        = backfill_days,
                backfill_today_count = backfill_today_count,
                bulk_size            = args.bulk_size,
                pb_threads           = args.pb_threads,
                pb_queue             = args.pb_queue,
                tz                   = tz,
            )


if __name__ == "__main__":
    main()
