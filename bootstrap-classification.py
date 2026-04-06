#!/usr/bin/env python3
"""
bootstrap-classification.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━
Focused bootstrap for the Elastic ML Classification Workshop.
Creates ONLY what is needed for the two classification jobs:

  • mortgage-audit-classification
  • mortgage-privileged-access-classification

What this does:
  1. Creates index templates for the 3 data streams with cross-index consistent
     mappings (prevents DFA merge failures when using multiple source indices)
  2. Creates the 3 data streams explicitly
  3. Patches existing backing index mappings to match templates
  4. Creates Kibana data views for source and destination indices
  5. Saves workshop-config.json for use by sdg-prime-classification.py

  DFA jobs are NOT created by bootstrap:
  • mortgage-privileged-access-classification → student creates via Dev Tools (Part 1)
  • mortgage-audit-classification             → Elastic Workflow creates automatically (Part 2)

Usage:
  python bootstrap-classification.py \\
      --host https://localhost:9200 \\
      --kibana-host http://localhost:5601 \\
      --user elastic --password changeme \\
      --no-verify-ssl

  # Purge everything and start over:
  python bootstrap-classification.py ... --purge

  # Skip Kibana asset creation:
  python bootstrap-classification.py ... --skip-kibana
"""

import argparse
import base64
import json
import os
import sys
import time

try:
    import urllib.request
    import urllib.error
    import ssl
except ImportError:
    pass

_HERE = os.path.dirname(os.path.realpath(os.path.abspath(__file__)))

# ── HTTP helpers ───────────────────────────────────────────────────────────────

def _request(url, method, body, auth, verify_ssl):
    ctx = ssl.create_default_context() if verify_ssl else ssl._create_unverified_context()
    data = json.dumps(body).encode() if body is not None else None
    req  = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", auth)
    req.add_header("Content-Type",  "application/json")
    req.add_header("kbn-xsrf",      "true")
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

def es_put(host, path, body, auth, verify_ssl, label):
    status, resp = _request(f"{host}{path}", "PUT", body, auth, verify_ssl)
    if status in (200, 201):
        print(f"  ✓ [{status}] {label}")
    elif status == 400 and "already_exists" in str(resp).lower():
        print(f"  ~ [exists] {label}")
    else:
        print(f"  ✗ [{status}] {label}")
        if status not in (200, 201):
            msg = resp.get("error", {})
            if isinstance(msg, dict):
                print(f"         {msg.get('reason','')[:120]}")
    return status, resp

def es_post(host, path, body, auth, verify_ssl, label):
    status, resp = _request(f"{host}{path}", "POST", body, auth, verify_ssl)
    if status in (200, 201):
        print(f"  ✓ [{status}] {label}")
    else:
        print(f"  ~ [{status}] {label}")
    return status, resp

def es_delete(host, path, auth, verify_ssl, label):
    status, resp = _request(f"{host}{path}", "DELETE", None, auth, verify_ssl)
    if status in (200, 201):
        print(f"  ✓ [{status}] Deleted: {label}")
    elif status == 404:
        print(f"  ~ [404] Not found (skipped): {label}")
    else:
        print(f"  ✗ [{status}] Failed: {label}")
    return status, resp

def kib_put(host, path, body, auth, verify_ssl, label):
    return es_put(host, path, body, auth, verify_ssl, label)


# ── Index templates ────────────────────────────────────────────────────────────

def create_templates(host, auth, verify_ssl):
    print("\n▸ Creating index templates…")

    # Shared lifecycle helper
    def _common_settings(itype):
        return {"index": {"lifecycle": {"name": itype}}}

    # ── logs-mortgage.audit-default ─────────────────────────────────────────
    es_put(host, "/_index_template/logs-mortgage.audit", {
        "index_patterns": ["logs-mortgage.audit-*"],
        "data_stream": {},
        "priority": 300,
        "template": {
            "settings": _common_settings("logs"),
            "mappings": {
                "properties": {
                    # ── ECS base ──────────────────────────────────────────────
                    "ecs":     {"properties": {"version": {"type": "keyword"}}},
                    "tags":    {"type": "keyword"},
                    "message": {"type": "text"},
                    # ── event ─────────────────────────────────────────────────
                    "event": {"properties": {
                        "kind":     {"type": "keyword"},
                        "category": {"type": "keyword"},
                        "type":     {"type": "keyword"},
                        "action":   {"type": "keyword"},
                        "outcome":  {"type": "keyword"},
                        "dataset":  {"type": "keyword"},
                    }},
                    # ── user — all subfields explicit so excludes work cleanly
                    "user": {"properties": {
                        "id":       {"type": "keyword"},
                        "name":     {"type": "keyword"},
                        "email":    {"type": "keyword"},
                        "roles":    {"type": "keyword"},
                        "full_name":{"type": "keyword"},
                    }},
                    # ── source ────────────────────────────────────────────────
                    "source": {"properties": {
                        "ip": {"type": "ip"},
                        "geo": {"properties": {
                            "country_iso_code": {"type": "keyword"},
                            "city_name":        {"type": "keyword"},
                            "region_name":      {"type": "keyword"},
                            "location":         {"type": "geo_point"},
                        }},
                    }},
                    # ── host ──────────────────────────────────────────────────
                    "host": {"properties": {
                        "hostname": {"type": "keyword"},
                        "name":     {"type": "keyword"},
                    }},
                    # ── audit ML features — dependent variable + all features ─
                    "audit": {"properties": {
                        "risk_score":    {"type": "float"},
                        "is_suspicious": {"type": "boolean"},
                        "session_id":    {"type": "keyword"},
                        "mfa_used":      {"type": "boolean"},
                        "off_hours":     {"type": "boolean"},
                        "new_device":    {"type": "boolean"},
                        "vpn_detected":  {"type": "boolean"},
                    }},
                }
            }
        }
    }, auth, verify_ssl, "Template: logs-mortgage.audit")

    # ── logs-ping_one.audit-mortgage ────────────────────────────────────────
    es_put(host, "/_index_template/logs-ping_one.audit", {
        "index_patterns": ["logs-ping_one.audit-*"],
        "data_stream": {},
        "priority": 300,
        "template": {
            "settings": _common_settings("logs"),
            "mappings": {
                "properties": {
                    # ── ECS base ──────────────────────────────────────────────
                    "ecs":  {"properties": {"version": {"type": "keyword"}}},
                    "tags": {"type": "keyword"},
                    "input":{"properties": {"type": {"type": "keyword"}}},
                    # ── event — must match Oracle template exactly ─────────────
                    "event": {"properties": {
                        "kind":     {"type": "keyword"},
                        "category": {"type": "keyword"},
                        "type":     {"type": "keyword"},
                        "action":   {"type": "keyword"},
                        "outcome":  {"type": "keyword"},
                        "dataset":  {"type": "keyword"},
                    }},
                    # ── user — must match Oracle template exactly ──────────────
                    # All four subfields declared in both templates so the DFA
                    # cross-index merge never sees a structural difference.
                    # user.id and user.name are in the DFA excludes list;
                    # they must still be mapped or the excludes directive errors.
                    "user": {"properties": {
                        "id":    {"type": "keyword"},
                        "name":  {"type": "keyword"},
                        "email": {"type": "keyword"},
                        "roles": {"type": "keyword"},
                    }},
                    # ── client.user — must match Oracle template exactly ────────
                    "client": {"properties": {
                        "user": {"properties": {
                            "id":   {"type": "keyword"},
                            "name": {"type": "keyword"},
                        }}
                    }},
                    # ── source — must match Oracle template exactly ─────────────
                    "source": {"properties": {
                        "ip": {"type": "ip"},
                        "geo": {"properties": {
                            "country_iso_code": {"type": "keyword"},
                            "city_name":        {"type": "keyword"},
                            "region_name":      {"type": "keyword"},
                            "location":         {"type": "geo_point"},
                        }},
                    }},
                    # ── PingOne-specific fields ────────────────────────────────
                    # These are only in PingOne documents; Oracle docs will have
                    # null values for these fields — no merge conflict.
                    "ping_one": {"properties": {
                        "audit": {"properties": {
                            "action":  {"properties": {"type": {"type": "keyword"}}},
                            "actors":  {"properties": {
                                "client": {"properties": {"type": {"type": "keyword"}}},
                                "user":   {"properties": {"type": {"type": "keyword"}}},
                            }},
                            "result": {"properties": {
                                "status":      {"type": "keyword"},
                                "description": {"type": "keyword"},
                            }},
                            "risk": {"properties": {
                                "score": {"type": "float"},
                                "level": {"type": "keyword"},
                            }},
                        }}
                    }},
                }
            }
        }
    }, auth, verify_ssl, "Template: logs-ping_one.audit")

    # ── logs-oracle.database_audit-mortgage ─────────────────────────────────
    es_put(host, "/_index_template/logs-oracle.database_audit", {
        "index_patterns": ["logs-oracle.database_audit-*"],
        "data_stream": {},
        "priority": 300,
        "template": {
            "settings": _common_settings("logs"),
            "mappings": {
                "properties": {
                    # ── ECS base ──────────────────────────────────────────────
                    "ecs":  {"properties": {"version": {"type": "keyword"}}},
                    "tags": {"type": "keyword"},
                    # ── event — must match PingOne template exactly ─────────────
                    "event": {"properties": {
                        "kind":     {"type": "keyword"},
                        "category": {"type": "keyword"},
                        "type":     {"type": "keyword"},
                        "action":   {"type": "keyword"},
                        "outcome":  {"type": "keyword"},
                        "dataset":  {"type": "keyword"},
                    }},
                    # ── user — must match PingOne template exactly ──────────────
                    # All four subfields declared so the DFA merge never sees a
                    # structural difference between this and the PingOne template.
                    # user.id and user.name must exist here even though most Oracle
                    # documents don't populate them — presence in mapping is what
                    # the merge check requires, not presence in documents.
                    "user": {"properties": {
                        "id":    {"type": "keyword"},
                        "name":  {"type": "keyword"},
                        "email": {"type": "keyword"},
                        "roles": {"type": "keyword"},
                    }},
                    # ── client.user — must match PingOne template exactly ────────
                    "client": {"properties": {
                        "user": {"properties": {
                            "id":   {"type": "keyword"},
                            "name": {"type": "keyword"},
                        }}
                    }},
                    # ── source — must match PingOne template exactly ─────────────
                    # Oracle documents don't have source.ip or source.geo but the
                    # field must be defined identically for the merge to succeed.
                    "source": {"properties": {
                        "ip": {"type": "ip"},
                        "geo": {"properties": {
                            "country_iso_code": {"type": "keyword"},
                            "city_name":        {"type": "keyword"},
                            "region_name":      {"type": "keyword"},
                            "location":         {"type": "geo_point"},
                        }},
                    }},
                    # ── Oracle-specific fields ─────────────────────────────────
                    # These are only in Oracle documents; PingOne docs will have
                    # null values for these fields — no merge conflict.
                    "server": {"properties": {
                        "address": {"type": "keyword"},
                        "domain":  {"type": "keyword"},
                    }},
                    "process": {"properties": {"pid": {"type": "integer"}}},
                    "related": {"properties": {"hosts": {"type": "keyword"}}},
                    "oracle": {"properties": {
                        "database_audit": {"properties": {
                            "action":         {"type": "keyword"},
                            "action_number":  {"type": "integer"},
                            "database":       {"properties": {"user": {"type": "keyword"}}},
                            "entry":          {"properties": {"id":   {"type": "long"}}},
                            "length":         {"type": "integer"},
                            "obj": {"properties": {
                                "name":   {"type": "keyword"},
                                "schema": {"type": "keyword"},
                            }},
                            "privilege":   {"type": "keyword"},
                            "result_code": {"type": "integer"},
                            "session_id":  {"type": "long"},
                            "status":      {"type": "keyword"},
                            "terminal":    {"type": "keyword"},
                        }}
                    }},
                }
            }
        }
    }, auth, verify_ssl, "Template: logs-oracle.database_audit")


# ── Data streams ──────────────────────────────────────────────────────────────

def create_data_streams(host, auth, verify_ssl):
    print("\n▸ Creating data streams…")
    for ds in [
        "logs-mortgage.audit-default",
        "logs-ping_one.audit-mortgage",
        "logs-oracle.database_audit-mortgage",
    ]:
        status, resp = _request(f"{host}/_data_stream/{ds}", "PUT", None, auth, verify_ssl)
        if status in (200, 201):
            print(f"  ✓ [{status}] Data stream: {ds}")
        elif status == 400 and "already_exists" in str(resp).lower():
            print(f"  ~ [exists] Data stream: {ds}")
        else:
            # May already exist from prior run — check
            chk, _ = _request(f"{host}/_data_stream/{ds}", "GET", None, auth, verify_ssl)
            if chk == 200:
                print(f"  ~ [exists] Data stream: {ds}")
            else:
                print(f"  ✗ [{status}] Data stream: {ds}  {resp.get('error',{}).get('reason','')[:80]}")


# ── Patch existing backing index mappings ─────────────────────────────────────
# Called after templates are created so that any already-existing backing indices
# receive the same mapping updates the new template would apply to future indices.
# This prevents DFA cross-index merge failures caused by structural differences
# between PingOne and Oracle backing indices.

def patch_existing_mappings(host, auth, verify_ssl):
    print("\n▸ Patching existing backing index mappings…")

    # Shared fields that MUST be identical in both PingOne and Oracle indices
    # for the DFA cross-index merge to succeed. We PUT these on all existing
    # backing indices for both data streams — safe to re-apply, mapping updates
    # are additive and never remove or change existing field types.

    shared_mapping = {
        "properties": {
            "event": {"properties": {
                "kind":     {"type": "keyword"},
                "category": {"type": "keyword"},
                "type":     {"type": "keyword"},
                "action":   {"type": "keyword"},
                "outcome":  {"type": "keyword"},
                "dataset":  {"type": "keyword"},
            }},
            # All four user subfields must exist in both indices.
            # user.id and user.name appear in the DFA excludes list —
            # they must be mapped even if most Oracle docs don't have them.
            "user": {"properties": {
                "id":    {"type": "keyword"},
                "name":  {"type": "keyword"},
                "email": {"type": "keyword"},
                "roles": {"type": "keyword"},
            }},
            # client.user.id was missing from Oracle in earlier bootstrap versions.
            "client": {"properties": {
                "user": {"properties": {
                    "id":   {"type": "keyword"},
                    "name": {"type": "keyword"},
                }}
            }},
            # source was missing from Oracle entirely in earlier versions.
            "source": {"properties": {
                "ip": {"type": "ip"},
                "geo": {"properties": {
                    "country_iso_code": {"type": "keyword"},
                    "city_name":        {"type": "keyword"},
                    "region_name":      {"type": "keyword"},
                    "location":         {"type": "geo_point"},
                }},
            }},
        }
    }

    # Discover all backing indices for PingOne and Oracle data streams
    target_streams = [
        "logs-ping_one.audit-mortgage",
        "logs-oracle.database_audit-mortgage",
    ]

    for stream in target_streams:
        # GET the data stream to find its backing indices
        status, resp = _request(
            f"{host}/_data_stream/{stream}", "GET", None, auth, verify_ssl
        )
        if status != 200:
            print(f"  ~ [{status}] {stream} not found — skipping")
            continue

        data_streams = resp.get("data_streams", [])
        if not data_streams:
            print(f"  ~ {stream}: no backing indices found")
            continue

        for ds in data_streams:
            for backing in ds.get("indices", []):
                index_name = backing.get("index_name", "")
                if not index_name:
                    continue
                s, r = _request(
                    f"{host}/{index_name}/_mapping",
                    "PUT", shared_mapping, auth, verify_ssl
                )
                if s == 200:
                    print(f"  ✓ [{s}] Patched: {index_name}")
                else:
                    reason = r.get("error", {}).get("reason", "")[:80]
                    print(f"  ✗ [{s}] Failed:  {index_name}  {reason}")


# ── DFA job definitions ────────────────────────────────────────────────────────

def create_dfa_jobs(host, auth, verify_ssl):
    print("\n▸ DFA jobs — intentionally not pre-created:")
    print("  ℹ  mortgage-privileged-access-classification")
    print("       → Created by student in Part 1 via Dev Tools (includes/excludes)")
    print("  ℹ  mortgage-audit-classification")
    print("       → Created by Elastic Workflow in Part 2 (automated)")


# ── Kibana data views ──────────────────────────────────────────────────────────

def create_data_views(kibana_host, auth, verify_ssl):
    print("\n▸ Creating Kibana data views…")

    views = [
        ("logs-mortgage.audit-default",             "logs-mortgage.audit-*"),
        ("logs-ping_one.audit-mortgage",             "logs-ping_one.audit-*"),
        ("logs-oracle.database_audit-mortgage",      "logs-oracle.database_audit-*"),
        ("mortgage-audit-classification",            "mortgage-audit-classification"),
        ("mortgage-privileged-access-classification","mortgage-privileged-access-classification"),
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


# ── Purge ──────────────────────────────────────────────────────────────────────

def purge(host, auth, verify_ssl, kibana_host, force):
    print("\n=== Classification Workshop — Purge ===\n")
    if not force:
        answer = input("  Type YES to confirm deletion of all workshop resources: ").strip()
        if answer != "YES":
            print("  Aborted.")
            return
        print()

    print("▸ Stopping and deleting DFA jobs…")
    for job in ["mortgage-audit-classification",
                "mortgage-privileged-access-classification"]:
        es_post(host, f"/_ml/data_frame/analytics/{job}/_stop?force=true",
                {}, auth, verify_ssl, f"Stop: {job}")
        es_delete(host, f"/_ml/data_frame/analytics/{job}",
                  auth, verify_ssl, f"Job: {job}")

    print("\n▸ Deleting destination indices…")
    for idx in ["mortgage-audit-classification",
                "mortgage-privileged-access-classification"]:
        es_delete(host, f"/{idx}", auth, verify_ssl, f"Index: {idx}")

    print("\n▸ Deleting data streams…")
    for ds in ["logs-mortgage.audit-default",
               "logs-ping_one.audit-mortgage",
               "logs-oracle.database_audit-mortgage"]:
        es_delete(host, f"/_data_stream/{ds}", auth, verify_ssl, f"Stream: {ds}")

    print("\n▸ Deleting index templates…")
    for tmpl in ["logs-mortgage.audit",
                 "logs-ping_one.audit",
                 "logs-oracle.database_audit"]:
        es_delete(host, f"/_index_template/{tmpl}", auth, verify_ssl, f"Template: {tmpl}")

    if kibana_host:
        print("\n▸ Deleting Kibana data views…")
        for dv in ["logs-mortgage.audit-default",
                   "logs-ping_one.audit-mortgage",
                   "logs-oracle.database_audit-mortgage",
                   "mortgage-audit-classification",
                   "mortgage-privileged-access-classification"]:
            # Find and delete by title search
            s, r = _request(
                f"{kibana_host}/api/data_views/data_view",
                "GET", None, auth, verify_ssl
            )
            # Best effort — skip if not found
            if s == 200:
                for item in r.get("data_view", []) if isinstance(r.get("data_view"), list) \
                        else [r.get("data_view", {})]:
                    if item.get("name") == dv or item.get("title","").startswith(dv.split("-")[0]):
                        _id = item.get("id","")
                        if _id:
                            es_delete(kibana_host, f"/api/data_views/data_view/{_id}",
                                      auth, verify_ssl, f"Data view: {dv}")

    print("\n✓ Purge complete.\n")


# ── Config save ────────────────────────────────────────────────────────────────

def save_config(args):
    cfg = {
        "host":          args.host,
        "user":          args.user,
        "password":      args.password,
        "no_verify_ssl": args.no_verify_ssl,
        "kibana_host":   args.kibana_host,
        "timezone":      getattr(args, "timezone", None),
    }
    path = os.path.join(_HERE, "workshop-config.json")
    try:
        with open(path, "w") as f:
            json.dump(cfg, f, indent=2)
        print(f"  ✓ Config saved → {path}")
    except Exception as e:
        print(f"  ⚠ Could not save config: {e}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(
        description="Bootstrap for the focused Elastic ML Classification Workshop",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full setup:
  python bootstrap-classification.py \\
      --host https://localhost:9200 \\
      --kibana-host http://localhost:5601 \\
      --user elastic --password changeme \\
      --no-verify-ssl

  # Purge everything:
  python bootstrap-classification.py ... --purge

  # Purge without confirmation:
  python bootstrap-classification.py ... --purge --force

  # Skip Kibana:
  python bootstrap-classification.py ... --skip-kibana
        """
    )
    p.add_argument("--host",          default="https://localhost:9200")
    p.add_argument("--user",          default="elastic")
    p.add_argument("--password",      default="changeme")
    p.add_argument("--no-verify-ssl", action="store_true")
    p.add_argument("--kibana-host",   default="http://localhost:5601")
    p.add_argument("--skip-kibana",   action="store_true")
    p.add_argument("--timezone",      default=None, metavar="TZ")
    p.add_argument("--purge",         action="store_true")
    p.add_argument("--force",         action="store_true",
                   help="Skip confirmation prompt with --purge")
    args = p.parse_args()

    verify_ssl = not args.no_verify_ssl
    creds = base64.b64encode(f"{args.user}:{args.password}".encode()).decode()
    auth  = f"Basic {creds}"

    print("\n=== LendPath Classification Workshop — Bootstrap ===")
    print(f"    Elasticsearch: {args.host}")
    if not args.skip_kibana:
        print(f"    Kibana:        {args.kibana_host}")
    print()

    if args.purge:
        purge(args.host, auth, verify_ssl,
              None if args.skip_kibana else args.kibana_host,
              args.force)
        return

    save_config(args)

    create_templates(args.host, auth, verify_ssl)
    create_data_streams(args.host, auth, verify_ssl)
    patch_existing_mappings(args.host, auth, verify_ssl)
    create_dfa_jobs(args.host, auth, verify_ssl)

    if not args.skip_kibana:
        create_data_views(args.kibana_host, auth, verify_ssl)

    print(f"\n{'='*56}")
    print(f"✓ Bootstrap complete.")
    print()
    print(f"  DFA jobs — created by students, not bootstrap:")
    print(f"    • mortgage-privileged-access-classification")
    print(f"       Part 1: create via Dev Tools with analyzed_fields includes/excludes")
    print(f"    • mortgage-audit-classification")
    print(f"       Part 2: Elastic Workflow creates this automatically")
    print()
    print(f"  Next steps:")
    print(f"    1. Generate data:")
    print(f"       python sdg-prime-classification.py \\")
    print(f"           --days 30 --events-per-day 2000 --anomaly-pct 15")
    print(f"    2. Follow WORKSHOP_FOCUSED.md — Part 1 starts with Dev Tools job creation")
    print(f"{'='*56}\n")


if __name__ == "__main__":
    main()
