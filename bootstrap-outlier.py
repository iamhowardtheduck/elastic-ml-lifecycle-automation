#!/usr/bin/env python3
"""
bootstrap-outlier.py
━━━━━━━━━━━━━━━━━━━━
Bootstrap for the Elastic ML Outlier Detection Workshop.

Scenario: "OpsPath" — a fictional SRE team monitoring a microservices
application. Students use unsupervised outlier detection on APM span data
to surface anomalous service calls without any labels.

What this does:
  1. Creates index template for traces-apm.spans-opspath with full ECS
     APM mappings (span, transaction, service, http, db, url fields)
  2. Creates the data stream explicitly
  3. Patches any existing backing index mappings
  4. Creates Kibana data views for source and destination indices
  5. Saves workshop-config-outlier.json for use by sdg-prime-outlier.py

DFA job is NOT pre-created by bootstrap:
  • opspath-span-outliers → student creates via Dev Tools in Chapter 1

Usage:
  python bootstrap-outlier.py \\
    --host https://your-es-host:9200 \\
    --kibana-host http://your-kibana:5601 \\
    --user elastic --password changeme \\
    --no-verify-ssl

  # Purge and reset:
  python bootstrap-outlier.py ... --purge [--force]

  # Skip Kibana:
  python bootstrap-outlier.py ... --skip-kibana
"""

import argparse
import base64
import json
import os
import ssl
import urllib.error
import urllib.request

_HERE = os.path.dirname(os.path.realpath(os.path.abspath(__file__)))


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def _request(url, method, body, auth, verify_ssl):
    ctx = (ssl.create_default_context() if verify_ssl
           else ssl._create_unverified_context())
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", auth)
    req.add_header("Content-Type", "application/json")
    req.add_header("kbn-xsrf", "true")
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=30) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode())
        except Exception:
            return e.code, {}
    except Exception as e:
        return 0, {"error": str(e)}


def _es_put(host, path, body, auth, verify_ssl, label):
    status, resp = _request(f"{host}{path}", "PUT", body, auth, verify_ssl)
    if status in (200, 201):
        print(f"  ✓ [{status}] {label}")
    elif status == 400 and "already_exists" in str(resp).lower():
        print(f"  ~ [exists] {label}")
    else:
        print(f"  ✗ [{status}] {label}")
        msg = resp.get("error", {})
        if isinstance(msg, dict):
            print(f"    {msg.get('reason', '')[:120]}")
    return status, resp


def _es_post(host, path, body, auth, verify_ssl, label):
    status, resp = _request(f"{host}{path}", "POST", body, auth, verify_ssl)
    if status in (200, 201):
        print(f"  ✓ [{status}] {label}")
    else:
        print(f"  ~ [{status}] {label}")
    return status, resp


def _es_delete(host, path, auth, verify_ssl, label):
    status, resp = _request(f"{host}{path}", "DELETE", None, auth, verify_ssl)
    if status in (200, 201):
        print(f"  ✓ [{status}] Deleted: {label}")
    elif status == 404:
        print(f"  ~ [404] Not found (skipped): {label}")
    else:
        print(f"  ✗ [{status}] Failed: {label}")
    return status, resp


# ── Index template ────────────────────────────────────────────────────────────

def create_template(host, auth, verify_ssl):
    print("\n▸ Creating index template…")

    # Check whether the built-in 'logs' ILM policy exists on this cluster.
    # Serverless and minimal self-managed clusters often don't have it.
    # If absent we simply omit the lifecycle setting — the data stream will
    # still be created correctly, just without automatic rollover policy.
    ilm_status, _ = _request(f"{host}/_ilm/policy/logs",
                              "GET", None, auth, verify_ssl)
    if ilm_status == 200:
        index_settings = {
            "lifecycle": {"name": "logs"},
            "number_of_replicas": 0,
        }
        print("  ✓ ILM policy 'logs' found — lifecycle setting included")
    else:
        index_settings = {"number_of_replicas": 0}
        print("  ~ ILM policy 'logs' not found — skipping lifecycle setting")

    _es_put(host, "/_index_template/traces-apm.spans-opspath", {
        "index_patterns": ["traces-apm.spans-opspath*"],
        "data_stream": {},
        "priority": 300,
        "template": {
            "settings": {"index": index_settings},
            "mappings": {
                "properties": {

                    # ── ECS base ──────────────────────────────────────────
                    "@timestamp":  {"type": "date"},
                    "ecs":         {"properties": {"version": {"type": "keyword"}}},
                    "tags":        {"type": "keyword"},
                    "message":     {"type": "text"},

                    # ── event ─────────────────────────────────────────────
                    # All subfields explicit — DFA analyzed_fields excludes
                    # reference these names directly.
                    "event": {"properties": {
                        "kind":     {"type": "keyword"},
                        "category": {"type": "keyword"},
                        "type":     {"type": "keyword"},
                        "action":   {"type": "keyword"},
                        "outcome":  {"type": "keyword"},
                        "dataset":  {"type": "keyword"},
                        "duration": {"type": "long"},     # nanoseconds
                    }},

                    # ── service ───────────────────────────────────────────
                    # Core identity fields for the emitting microservice.
                    # service.name is NOT analyzed — DFA should not learn
                    # per-service identity (same reason as user.id in
                    # classification). Excluded in the DFA job definition.
                    "service": {"properties": {
                        "name":        {"type": "keyword"},
                        "environment": {"type": "keyword"},
                        "version":     {"type": "keyword"},
                        "node":        {"properties": {
                            "name": {"type": "keyword"}
                        }},
                    }},

                    # ── span ──────────────────────────────────────────────
                    # The primary analyzed fields for outlier detection.
                    # span.duration.us is the most important numeric signal.
                    "span": {"properties": {
                        "id":       {"type": "keyword"},
                        "name":     {"type": "keyword"},
                        "type":     {"type": "keyword"},   # db, external, app
                        "subtype":  {"type": "keyword"},   # postgresql, redis, http
                        "action":   {"type": "keyword"},   # query, execute, connect
                        "duration": {"properties": {
                            "us": {"type": "long"},        # microseconds — ANALYZED
                        }},
                        "db": {"properties": {
                            "rows_affected": {"type": "integer"},
                            "statement":     {
                                "type": "text",
                                "fields": {"keyword": {"type": "keyword",
                                                       "ignore_above": 512}}
                            },
                        }},
                        "self_time": {"properties": {
                            "sum": {"properties": {
                                "us": {"type": "long"}
                            }}
                        }},
                    }},

                    # ── transaction ───────────────────────────────────────
                    # Parent transaction context. transaction.duration.us
                    # is a strong co-feature with span.duration.us.
                    "transaction": {"properties": {
                        "id":   {"type": "keyword"},
                        "name": {"type": "keyword"},
                        "type": {"type": "keyword"},       # request, background
                        "duration": {"properties": {
                            "us": {"type": "long"},        # microseconds — ANALYZED
                        }},
                        "result":    {"type": "keyword"},  # HTTP 2xx/4xx/5xx
                        "sampled":   {"type": "boolean"},
                        "span_count": {"properties": {
                            "started": {"type": "integer"},
                        }},
                    }},

                    # ── http ──────────────────────────────────────────────
                    # HTTP context for outbound calls made within a span.
                    # http.response.status_code is a key categorical feature.
                    "http": {"properties": {
                        "request": {"properties": {
                            "method":       {"type": "keyword"},
                            "body":         {"properties": {
                                "bytes": {"type": "integer"}
                            }},
                        }},
                        "response": {"properties": {
                            "status_code":   {"type": "integer"},  # ANALYZED
                            "body":          {"properties": {
                                "bytes": {"type": "integer"}       # ANALYZED
                            }},
                        }},
                        "version": {"type": "keyword"},
                    }},

                    # ── url ───────────────────────────────────────────────
                    "url": {"properties": {
                        "original": {
                            "type": "text",
                            "fields": {"keyword": {"type": "keyword",
                                                   "ignore_above": 1024}}
                        },
                        "domain": {"type": "keyword"},
                        "path":   {"type": "keyword"},
                        "port":   {"type": "integer"},
                        "scheme": {"type": "keyword"},
                    }},

                    # ── destination ───────────────────────────────────────
                    "destination": {"properties": {
                        "address": {"type": "keyword"},
                        "ip":      {"type": "ip"},
                        "port":    {"type": "integer"},
                    }},

                    # ── host ──────────────────────────────────────────────
                    "host": {"properties": {
                        "hostname": {"type": "keyword"},
                        "name":     {"type": "keyword"},
                        "ip":       {"type": "ip"},
                        "os":       {"properties": {
                            "platform": {"type": "keyword"},
                        }},
                    }},

                    # ── agent ─────────────────────────────────────────────
                    "agent": {"properties": {
                        "name":    {"type": "keyword"},
                        "version": {"type": "keyword"},
                    }},

                    # ── process ───────────────────────────────────────────
                    "process": {"properties": {
                        "pid": {"type": "integer"},
                    }},

                    # ── labels ────────────────────────────────────────────
                    # Custom labels injected by the SDG to support workshop
                    # discussions. NOT analyzed by DFA.
                    "labels": {"properties": {
                        "is_anomalous": {"type": "boolean"},  # ground truth for discussion
                        "anomaly_type": {"type": "keyword"},  # slow_query, cascade_failure, etc.
                    }},

                }
            }
        }
    }, auth, verify_ssl, "Template: traces-apm.spans-opspath")



# ── Template verification ─────────────────────────────────────────────────────

def _verify_template(host, auth, verify_ssl):
    """Confirm the index template was actually created before trying to use it."""
    status, resp = _request(
        f"{host}/_index_template/traces-apm.spans-opspath",
        "GET", None, auth, verify_ssl
    )
    if status == 200:
        templates = resp.get("index_templates", [])
        if templates:
            print(f"  ✓ Template verified: traces-apm.spans-opspath")
            return
    # Template missing — print everything we know and stop
    print(f"\n  ✗ [{status}] Template traces-apm.spans-opspath not found after creation.")
    print(f"    This means the PUT _index_template call above returned an error.")
    print(f"    Full response: {str(resp)[:300]}")
    print()
    print("  Run this in Dev Tools to check for conflicting templates:")
    print("    GET _index_template/traces-apm*")
    print()
    print("  Bootstrap cannot continue without the template.")
    import sys
    sys.exit(1)


# ── Data stream ───────────────────────────────────────────────────────────────

def create_data_stream(host, auth, verify_ssl):
    print("\n▸ Creating data stream…")
    ds = "traces-apm.spans-opspath"
    status, resp = _request(f"{host}/_data_stream/{ds}",
                            "PUT", None, auth, verify_ssl)
    if status in (200, 201):
        print(f"  ✓ [{status}] Data stream: {ds}")
        return
    elif status == 400 and "already_exists" in str(resp).lower():
        print(f"  ~ [exists] Data stream: {ds}")
        return

    # Any other status — do a GET to see if it exists already anyway
    chk, _ = _request(f"{host}/_data_stream/{ds}", "GET", None, auth, verify_ssl)
    if chk == 200:
        print(f"  ~ [exists] Data stream: {ds}")
        return

    # Truly failed — print the full error and stop so the SDG doesn't run
    # against a missing stream
    reason = resp.get("error", {}).get("reason", str(resp))[:200]
    print(f"\n  ✗ [{status}] Failed to create data stream: {ds}")
    print(f"    Reason : {reason}")
    print()
    print("  Most likely causes:")
    print("  1. The index lifecycle policy 'logs' does not exist on this cluster.")
    print("     Fix: remove the lifecycle setting from the template, or create")
    print("     the policy first:  PUT _ilm/policy/logs {{ ... }}")
    print("  2. The cluster has a conflicting index template at higher priority.")
    print("     Check: GET _index_template/traces-apm*")
    print("  3. SSL/auth error — check --host, --user, --password, --no-verify-ssl")
    print()
    print("  Bootstrap cannot continue. Fix the error above and re-run.")
    import sys
    sys.exit(1)


# ── Patch existing backing index mappings ─────────────────────────────────────

def patch_existing_mappings(host, auth, verify_ssl):
    print("\n▸ Patching existing backing index mappings…")

    # The core analyzed fields must have explicit float/long/integer types
    # on any already-existing backing index. Without this, DFA rejects the
    # job with a "field used in analysis has wrong type" error.
    patch = {
        "properties": {
            "span":        {"properties": {"duration": {"properties": {"us": {"type": "long"}}}}},
            "transaction": {"properties": {"duration": {"properties": {"us": {"type": "long"}}}}},
            "http":        {"properties": {"response": {"properties": {"status_code": {"type": "integer"}}}}},
            "event":       {"properties": {"duration": {"type": "long"}}},
        }
    }

    status, resp = _request(
        f"{host}/_data_stream/traces-apm.spans-opspath",
        "GET", None, auth, verify_ssl
    )
    if status != 200:
        print("  ~ Data stream not found — skipping patch")
        return

    for ds in resp.get("data_streams", []):
        for backing in ds.get("indices", []):
            idx = backing.get("index_name", "")
            if not idx:
                continue
            s, r = _request(f"{host}/{idx}/_mapping", "PUT", patch, auth, verify_ssl)
            if s == 200:
                print(f"  ✓ Patched: {idx}")
            else:
                reason = r.get("error", {}).get("reason", "")[:80]
                print(f"  ✗ Failed: {idx} {reason}")


# ── Kibana data views ─────────────────────────────────────────────────────────

def create_data_views(kibana_host, auth, verify_ssl):
    print("\n▸ Creating Kibana data views…")

    views = [
        # Source data stream — student explores this first
        ("traces-apm.spans-opspath",
         "traces-apm.spans-opspath*"),
        # DFA destination index — created after job runs
        ("opspath-span-outliers",
         "opspath-span-outliers"),
    ]

    for name, pattern in views:
        status, resp = _request(
            f"{kibana_host}/api/data_views/data_view",
            "POST",
            {"data_view": {
                "title":         pattern,
                "name":          name,
                "timeFieldName": "@timestamp",
            }},
            auth, verify_ssl
        )
        if status in (200, 201):
            print(f"  ✓ [{status}] Data view: {name}")
        elif status == 400 and "already exists" in str(resp).lower():
            print(f"  ~ [exists] Data view: {name}")
        else:
            print(f"  ✗ [{status}] Data view: {name}")



# ── Cluster state verification ────────────────────────────────────────────────

def _cluster_verify(host, auth, verify_ssl):
    """Check what bootstrap resources exist on the cluster right now."""
    print("\n▸ Verifying cluster state…\n")

    # Template
    s, r = _request(f"{host}/_index_template/traces-apm.spans-opspath",
                    "GET", None, auth, verify_ssl)
    if s == 200 and r.get("index_templates"):
        print("  ✓ Index template  : traces-apm.spans-opspath")
    else:
        print("  ✗ Index template  : traces-apm.spans-opspath — NOT FOUND")

    # Data stream
    s, r = _request(f"{host}/_data_stream/traces-apm.spans-opspath",
                    "GET", None, auth, verify_ssl)
    if s == 200:
        ds_list = r.get("data_streams", [])
        if ds_list:
            backing = [i["index_name"] for i in ds_list[0].get("indices", [])]
            print(f"  ✓ Data stream     : traces-apm.spans-opspath")
            print(f"    Backing indices : {backing}")
        else:
            print("  ✗ Data stream     : traces-apm.spans-opspath — NOT FOUND")
    else:
        print(f"  ✗ Data stream     : traces-apm.spans-opspath — [{s}]")

    # Document count
    s, r = _request(f"{host}/traces-apm.spans-opspath/_count",
                    "GET", None, auth, verify_ssl)
    if s == 200:
        print(f"  ✓ Document count  : {r.get('count', 0):,}")
    else:
        print(f"  ~ Document count  : unavailable [{s}]")

    # Config file
    import os
    cfg = os.path.join(os.path.dirname(os.path.realpath(os.path.abspath(__file__))),
                       "workshop-config-outlier.json")
    ws_cfg = "/workspace/workshop/elastic-ml-lifecycle-automation/workshop-config-outlier.json"
    for path in [cfg, ws_cfg]:
        if os.path.exists(path):
            print(f"  ✓ Config file     : {path}")
            break
    else:
        print("  ✗ Config file     : workshop-config-outlier.json — NOT FOUND")
        print("    The SDG will fall back to localhost:9200 without this file.")

    print()


# ── Purge ─────────────────────────────────────────────────────────────────────

def purge(host, auth, verify_ssl, kibana_host, force):
    print("\n=== OpsPath Outlier Workshop — Purge ===\n")
    if not force:
        answer = input(
            "  Type YES to confirm deletion of all workshop resources: "
        ).strip()
        if answer != "YES":
            print("  Aborted.")
            return
    print()

    print("▸ Stopping and deleting DFA job…")
    _es_post(host, "/ml/data_frame/analytics/opspath-span-outliers/_stop?force=true",
             {}, auth, verify_ssl, "Stop: opspath-span-outliers")
    _es_delete(host, "/_ml/data_frame/analytics/opspath-span-outliers",
               auth, verify_ssl, "Job: opspath-span-outliers")

    print("\n▸ Deleting destination index…")
    _es_delete(host, "/opspath-span-outliers",
               auth, verify_ssl, "Index: opspath-span-outliers")

    print("\n▸ Deleting data stream…")
    _es_delete(host, "/_data_stream/traces-apm.spans-opspath",
               auth, verify_ssl, "Stream: traces-apm.spans-opspath")

    print("\n▸ Deleting index template…")
    _es_delete(host, "/_index_template/traces-apm.spans-opspath",
               auth, verify_ssl, "Template: traces-apm.spans-opspath")

    if kibana_host:
        print("\n▸ Deleting Kibana data views…")
        for dv_name in ["traces-apm.spans-opspath", "opspath-span-outliers"]:
            s, r = _request(f"{kibana_host}/api/data_views/data_view",
                            "GET", None, auth, verify_ssl)
            if s == 200:
                items = r.get("data_view", [])
                if isinstance(items, dict):
                    items = [items]
                for item in items:
                    if item.get("name") == dv_name:
                        _id = item.get("id", "")
                        if _id:
                            _es_delete(kibana_host,
                                       f"/api/data_views/data_view/{_id}",
                                       auth, verify_ssl, f"Data view: {dv_name}")

    print("\n✓ Purge complete.\n")


# ── Config save ───────────────────────────────────────────────────────────────

def save_config(args):
    cfg = {
        "host":          args.host,
        "user":          args.user,
        "password":      args.password,
        "no_verify_ssl": args.no_verify_ssl,
        "kibana_host":   args.kibana_host,
        "timezone":      getattr(args, "timezone", None),
        "workshop":      "outlier",
    }
    path = os.path.join(_HERE, "workshop-config-outlier.json")
    try:
        with open(path, "w") as f:
            json.dump(cfg, f, indent=2)
        print(f"  ✓ Config saved → {path}")
    except Exception as e:
        print(f"  ⚠ Could not save config: {e}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(
        description="Bootstrap for the OpsPath Outlier Detection Workshop",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full setup:
  python bootstrap-outlier.py \\
    --host https://localhost:9200 \\
    --kibana-host http://localhost:5601 \\
    --user elastic --password changeme \\
    --no-verify-ssl

  # Purge everything:
  python bootstrap-outlier.py ... --purge

  # Purge without confirmation prompt:
  python bootstrap-outlier.py ... --purge --force

  # Skip Kibana asset creation:
  python bootstrap-outlier.py ... --skip-kibana
""",
    )
    p.add_argument("--host",          default="https://localhost:9200")
    p.add_argument("--user",          default="elastic")
    p.add_argument("--password",      default="changeme")
    p.add_argument("--no-verify-ssl", action="store_true")
    p.add_argument("--kibana-host",   default="http://localhost:5601")
    p.add_argument("--skip-kibana",   action="store_true")
    p.add_argument("--timezone",      default=None, metavar="TZ")
    p.add_argument("--verify",        action="store_true",
                   help="Check what exists on the cluster without creating anything")
    p.add_argument("--purge",         action="store_true")
    p.add_argument("--force",         action="store_true",
                   help="Skip confirmation prompt with --purge")
    args = p.parse_args()

    verify_ssl = not args.no_verify_ssl
    creds = base64.b64encode(f"{args.user}:{args.password}".encode()).decode()
    auth  = f"Basic {creds}"

    print("\n=== OpsPath Outlier Detection Workshop — Bootstrap ===")
    print(f"  Elasticsearch : {args.host}")
    if not args.skip_kibana:
        print(f"  Kibana        : {args.kibana_host}")
    print()

    if args.verify:
        _cluster_verify(args.host, auth, verify_ssl)
        return

    if args.purge:
        purge(args.host, auth, verify_ssl,
              None if args.skip_kibana else args.kibana_host,
              args.force)
        return

    save_config(args)
    create_template(args.host, auth, verify_ssl)
    _verify_template(args.host, auth, verify_ssl)
    create_data_stream(args.host, auth, verify_ssl)
    patch_existing_mappings(args.host, auth, verify_ssl)

    if not args.skip_kibana:
        create_data_views(args.kibana_host, auth, verify_ssl)

    print(f"\n{'='*56}")
    print("✓ Bootstrap complete.")
    print()
    print("  DFA job — created by student, not bootstrap:")
    print("  • opspath-span-outliers")
    print("    Chapter 1: create via Dev Tools with analyzed_fields includes/excludes")
    print()
    print("  Next steps:")
    print("    1. Generate data:")
    print("       python sdg-prime-outlier.py \\")
    print("         --days 30 --events-per-day 3000 --anomaly-pct 5")
    print("    2. Follow instruqt-chapter-0-setup.md")
    print(f"{'='*56}\n")


if __name__ == "__main__":
    main()
