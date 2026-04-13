#!/usr/bin/env python3
"""
bootstrap-outlier-classifier.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Bootstrap for the OpsPath Outlier → Classification Bridge Workshop.

This script extends the existing outlier workshop setup.  It assumes
bootstrap-outlier.py has already been run and the data stream
traces-apm.spans-opspath already exists.

What this adds:
  1. Patches the existing index template to add ml.outlier_score (float)
     and is_anomalous (boolean) mappings — required so DFA can use
     is_anomalous as a dependent_variable without a type conflict.
  2. Patches all existing backing indices with the same mappings.
  3. Creates the labelling ingest pipeline that thresholds ml.outlier_score
     into is_anomalous = true/false (threshold configurable, default 0.5).
  4. Creates the chained inference pipeline that runs outlier scoring
     THEN label assignment in a single pipeline — so new spans get both
     fields at write time.
  5. Creates Kibana data views for the classification destination index.
  6. Saves workshop-config-bridge.json for sdg-prime-outlier.py live mode.

DFA classification job is NOT pre-created:
  • opspath-span-classifier → student creates via Dev Tools in Chapter 2

Usage:
  python bootstrap-outlier-classifier.py \\
    --host https://your-es-host:9200 \\
    --kibana-host http://your-kibana:5601 \\
    --user elastic --password changeme \\
    --no-verify-ssl \\
    --outlier-model-id opspath-span-outliers-1234567890

  # Override threshold (default 0.5):
  python bootstrap-outlier-classifier.py ... --threshold 0.6

  # Purge bridge resources only (leaves outlier workshop intact):
  python bootstrap-outlier-classifier.py ... --purge [--force]
"""

import argparse
import base64
import json
import os
import ssl
import urllib.error
import urllib.request

_HERE = os.path.dirname(os.path.realpath(os.path.abspath(__file__)))
OUTLIER_CONFIG = os.path.join(_HERE, "workshop-config-outlier.json")
BRIDGE_CONFIG  = os.path.join(_HERE, "workshop-config-bridge.json")


# ── HTTP helpers (identical to bootstrap-outlier.py) ─────────────────────────

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


def _put(host, path, body, auth, verify_ssl, label):
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


def _post(host, path, body, auth, verify_ssl, label):
    status, resp = _request(f"{host}{path}", "POST", body, auth, verify_ssl)
    if status in (200, 201):
        print(f"  ✓ [{status}] {label}")
    else:
        print(f"  ~ [{status}] {label}")
    return status, resp


def _delete(host, path, auth, verify_ssl, label):
    status, resp = _request(f"{host}{path}", "DELETE", None, auth, verify_ssl)
    if status in (200, 201):
        print(f"  ✓ [{status}] Deleted: {label}")
    elif status == 404:
        print(f"  ~ [404]     Not found (skipped): {label}")
    else:
        print(f"  ✗ [{status}] Failed: {label}")
    return status, resp


# ── Resolve outlier model ID ──────────────────────────────────────────────────

def resolve_model_id(host, auth, verify_ssl, provided_id):
    """
    If the user passed --outlier-model-id explicitly, use it.
    Otherwise query the trained models API to find the most recent model
    tagged with 'opspath-span-outliers'.
    """
    if provided_id:
        return provided_id

    print("  Auto-detecting outlier model ID…")
    status, resp = _request(
        f"{host}/_ml/trained_models"
        "?tags=opspath-span-outliers&human=true&size=10",
        "GET", None, auth, verify_ssl
    )
    if status != 200:
        print(f"  ✗ Could not query trained models [{status}]. "
              "Pass --outlier-model-id explicitly.")
        return None

    models = resp.get("trained_model_configs", [])
    if not models:
        print("  ✗ No trained model found with tag 'opspath-span-outliers'.")
        print("    Run the outlier DFA job first (Chapter 1), then re-run this script.")
        return None

    # Sort by create_time descending — use the most recent
    models.sort(key=lambda m: m.get("create_time", 0), reverse=True)
    model_id = models[0]["model_id"]
    print(f"  ✓ Found model: {model_id}")
    return model_id


# ── Patch template and backing indices ───────────────────────────────────────

def patch_template_and_indices(host, auth, verify_ssl):
    """
    Add ml.outlier_score (float) and is_anomalous (boolean) to the existing
    traces-apm.spans-opspath template AND to all current backing indices.

    Without this:
      - The DFA job fails with "field [is_anomalous] has no mapping" when it
        tries to use it as dependent_variable.
      - The labelling pipeline would auto-map is_anomalous as keyword (from
        the script processor output) instead of boolean, breaking DFA.
    """
    print("\n▸ Patching index template with bridge fields…")

    # Retrieve current template component count so we can add to it
    # (We can't read the full template body via the standard GET without
    # expanding it, so we just PUT a targeted mapping update instead.)

    new_props = {
        "properties": {
            # Written by the outlier inference pipeline
            "ml": {"properties": {
                "outlier_score": {"type": "float"},
                "inference_error": {"type": "keyword"},
                # Feature influence sub-fields — dot-in-name fields
                # must be declared with enabled:false at the parent or
                # as individual keyword fields. We declare the parent
                # as an object with dynamic:true so new sub-fields work.
                "feature_influence": {"type": "object", "dynamic": True},
            }},
            # Written by the labelling pipeline — MUST be boolean for DFA
            "is_anomalous": {"type": "boolean"},
        }
    }

    # Patch the index template (component update via simulate is not needed —
    # PUT on the template path with just the new mapping is additive)
    _put(
        host,
        "/_index_template/traces-apm.spans-opspath",
        {
            "index_patterns": ["traces-apm.spans-opspath*"],
            "data_stream": {},
            "priority": 300,
            "template": {
                "settings": {"index": {"lifecycle": {"name": "logs"},
                                        "number_of_replicas": 0}},
                "mappings": new_props,
            },
        },
        auth, verify_ssl,
        "Template patch: ml.outlier_score + is_anomalous"
    )

    # Patch all existing backing indices
    print("\n▸ Patching existing backing indices…")
    status, resp = _request(
        f"{host}/_data_stream/traces-apm.spans-opspath",
        "GET", None, auth, verify_ssl
    )
    if status != 200:
        print("  ~ Data stream not found — skipping backing index patch")
        return

    for ds in resp.get("data_streams", []):
        for backing in ds.get("indices", []):
            idx = backing.get("index_name", "")
            if not idx:
                continue
            s, r = _request(
                f"{host}/{idx}/_mapping", "PUT", new_props,
                auth, verify_ssl
            )
            if s == 200:
                print(f"  ✓ Patched: {idx}")
            else:
                reason = r.get("error", {}).get("reason", "")[:80]
                print(f"  ✗ Failed : {idx}  {reason}")


# ── Create labelling pipeline ─────────────────────────────────────────────────

def create_labelling_pipeline(host, auth, verify_ssl, threshold: float):
    """
    opspath-label-anomalous
    ────────────────────────
    A single Painless script processor that reads ml.outlier_score from the
    document (already written by the outlier inference pipeline) and sets
    is_anomalous = true if the score meets the threshold.

    This pipeline is NOT wired to the data stream directly — it is called
    as a second processor in the chained pipeline (opspath-full-ml).
    Students can also run it as a one-shot reindex against historical data.
    """
    print("\n▸ Creating labelling pipeline…")
    _put(
        host,
        "/_ingest/pipeline/opspath-label-anomalous",
        {
            "description": (
                f"Threshold ml.outlier_score >= {threshold} into "
                "is_anomalous boolean field"
            ),
            "processors": [
                {
                    "script": {
                        "description": "Threshold outlier score into boolean label",
                        "lang": "painless",
                        "source": f"""
double score = 0.0;
if (ctx.containsKey('ml') && ctx['ml'] != null
    && ctx['ml'].containsKey('outlier_score')
    && ctx['ml']['outlier_score'] != null) {{
  score = ((Number) ctx['ml']['outlier_score']).doubleValue();
}}
ctx['is_anomalous'] = (score >= {threshold});
""".strip(),
                    }
                }
            ],
        },
        auth, verify_ssl,
        f"Pipeline: opspath-label-anomalous (threshold={threshold})"
    )


# ── Create chained inference + labelling pipeline ────────────────────────────

def create_chained_pipeline(host, auth, verify_ssl,
                             model_id: str, threshold: float):
    """
    opspath-full-ml
    ────────────────
    Chains two processors:
      1. Outlier inference  → writes ml.outlier_score
      2. Label script       → writes is_anomalous

    This is the pipeline wired to the data stream. Every new span that
    arrives gets both fields at write time with no extra indexing step.
    """
    print("\n▸ Creating chained inference + labelling pipeline…")
    _put(
        host,
        "/_ingest/pipeline/opspath-full-ml",
        {
            "description": (
                "Chain: outlier inference → outlier score → "
                "is_anomalous boolean label"
            ),
            "processors": [
                # ── Step 1: outlier model ────────────────────────────────
                {
                    "inference": {
                        "description": "Score span with trained outlier model",
                        "model_id": model_id,
                        "target_field": "ml",
                        "field_mappings": {},
                        "inference_config": {
                            "regression": {"results_field": "outlier_score"}
                        },
                        "on_failure": [
                            {
                                "set": {
                                    "field": "ml.inference_error",
                                    "value": "{{ _ingest.on_failure_message }}",
                                }
                            }
                        ],
                    }
                },
                # ── Step 2: threshold script ─────────────────────────────
                {
                    "script": {
                        "description": (
                            f"Threshold ml.outlier_score >= {threshold} "
                            "into is_anomalous boolean"
                        ),
                        "lang": "painless",
                        "source": f"""
double score = 0.0;
if (ctx.containsKey('ml') && ctx['ml'] != null
    && ctx['ml'].containsKey('outlier_score')
    && ctx['ml']['outlier_score'] != null) {{
  score = ((Number) ctx['ml']['outlier_score']).doubleValue();
}}
ctx['is_anomalous'] = (score >= {threshold});
""".strip(),
                    }
                },
            ],
        },
        auth, verify_ssl,
        f"Pipeline: opspath-full-ml (model={model_id}, threshold={threshold})"
    )


# ── Wire chained pipeline to data stream ─────────────────────────────────────

def wire_pipeline(host, auth, verify_ssl):
    """
    Apply opspath-full-ml as the default_pipeline on the data stream.
    Replaces the opspath-span-outlier-ml pipeline set in Chapter 1.
    """
    print("\n▸ Wiring chained pipeline to data stream…")
    _put(
        host,
        "/traces-apm.spans-opspath/_settings",
        {"index": {"default_pipeline": "opspath-full-ml"}},
        auth, verify_ssl,
        "default_pipeline = opspath-full-ml on traces-apm.spans-opspath"
    )


# ── Backfill is_anomalous on historical data via reindex ─────────────────────

def backfill_labels(host, auth, verify_ssl):
    """
    Reindex all existing documents in opspath-span-outliers (the DFA dest
    index, which already has ml.outlier_score) back into the source data
    stream via the labelling pipeline so historical docs also get is_anomalous.

    This is optional — students can do it manually in the workshop — but the
    bootstrap offers it as a one-shot operation so the classification job
    has a full 30-day labelled dataset immediately.
    """
    print("\n▸ Backfilling is_anomalous on historical data…")
    print("  (reindex from opspath-span-outliers → traces-apm.spans-opspath"
          " via opspath-label-anomalous)")
    print("  This may take 1–3 minutes depending on document count…")

    status, resp = _post(
        host,
        "/_reindex?wait_for_completion=false",
        {
            "source": {
                "index": "opspath-span-outliers",
                "_source": True,
            },
            "dest": {
                "index": "traces-apm.spans-opspath",
                "op_type": "index",
                "pipeline": "opspath-label-anomalous",
            },
        },
        auth, verify_ssl,
        "Reindex: opspath-span-outliers → traces-apm.spans-opspath"
    )

    if status in (200, 201):
        task_id = resp.get("task", "unknown")
        print(f"\n  Reindex task started: {task_id}")
        print(f"  Monitor with: GET _tasks/{task_id}")
        print("  When complete, new documents in traces-apm.spans-opspath"
              " will have is_anomalous populated.")
    else:
        print(f"\n  ✗ Reindex failed [{status}]: "
              f"{resp.get('error', {}).get('reason', '')[:120]}")
        print("  You can backfill manually via the workshop instructions.")


# ── Kibana data views ─────────────────────────────────────────────────────────

def create_data_views(kibana_host, auth, verify_ssl):
    print("\n▸ Creating Kibana data views…")

    views = [
        # Classification destination — created after the DFA job runs
        ("opspath-span-classifier",  "opspath-span-classifier"),
        # Labelled source — useful for visualising class balance before training
        ("traces-apm.spans-opspath-labelled",
         "traces-apm.spans-opspath*"),
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


# ── Purge ─────────────────────────────────────────────────────────────────────

def purge(host, auth, verify_ssl, kibana_host, force):
    print("\n=== OpsPath Bridge — Purge (bridge resources only) ===\n")
    if not force:
        answer = input(
            "  Type YES to confirm deletion of bridge pipelines and "
            "classification job: "
        ).strip()
        if answer != "YES":
            print("  Aborted.")
            return
    print()

    print("▸ Stopping and deleting classification DFA job…")
    _post(host,
          "/_ml/data_frame/analytics/opspath-span-classifier/_stop?force=true",
          {}, auth, verify_ssl, "Stop: opspath-span-classifier")
    _delete(host, "/_ml/data_frame/analytics/opspath-span-classifier",
            auth, verify_ssl, "Job: opspath-span-classifier")

    print("\n▸ Deleting classification destination index…")
    _delete(host, "/opspath-span-classifier",
            auth, verify_ssl, "Index: opspath-span-classifier")

    print("\n▸ Deleting bridge pipelines…")
    for p in ["opspath-full-ml", "opspath-label-anomalous"]:
        _delete(host, f"/_ingest/pipeline/{p}", auth, verify_ssl,
                f"Pipeline: {p}")

    print("\n▸ Resetting data stream to outlier-only pipeline…")
    _put(host, "/traces-apm.spans-opspath/_settings",
         {"index": {"default_pipeline": "opspath-span-outlier-ml"}},
         auth, verify_ssl,
         "Reset default_pipeline to opspath-span-outlier-ml")

    if kibana_host:
        print("\n▸ Deleting bridge Kibana data views…")
        for dv_name in ["opspath-span-classifier",
                         "traces-apm.spans-opspath-labelled"]:
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
                            _delete(kibana_host,
                                    f"/api/data_views/data_view/{_id}",
                                    auth, verify_ssl,
                                    f"Data view: {dv_name}")

    print("\n✓ Bridge purge complete. Outlier workshop resources untouched.\n")


# ── Config save ───────────────────────────────────────────────────────────────

def save_config(args, model_id: str):
    # Load outlier config as base, extend with bridge fields
    cfg = {}
    if os.path.exists(OUTLIER_CONFIG):
        with open(OUTLIER_CONFIG) as f:
            cfg = json.load(f)

    cfg.update({
        "workshop":       "bridge",
        "outlier_model_id": model_id,
        "label_threshold":  args.threshold,
    })
    # Override connection if explicitly passed
    if args.host and args.host != "auto":
        cfg["host"]          = args.host
        cfg["user"]          = args.user
        cfg["password"]      = args.password
        cfg["no_verify_ssl"] = args.no_verify_ssl
        cfg["kibana_host"]   = args.kibana_host

    try:
        with open(BRIDGE_CONFIG, "w") as f:
            json.dump(cfg, f, indent=2)
        print(f"  ✓ Config saved → {BRIDGE_CONFIG}")
    except Exception as e:
        print(f"  ⚠ Could not save config: {e}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(
        description="Bootstrap for the OpsPath Outlier → Classification Bridge",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Prerequisites:
  bootstrap-outlier.py must have been run and the outlier DFA job
  (opspath-span-outliers) must have completed training.

Examples:
  # Auto-detect model ID from trained models API:
  python bootstrap-outlier-classifier.py \\
    --host https://localhost:9200 \\
    --kibana-host http://localhost:5601 \\
    --user elastic --password changeme \\
    --no-verify-ssl

  # Explicit model ID:
  python bootstrap-outlier-classifier.py ... \\
    --outlier-model-id opspath-span-outliers-1712345678901

  # Custom threshold (default 0.5):
  python bootstrap-outlier-classifier.py ... --threshold 0.6

  # Purge bridge resources only (outlier workshop stays intact):
  python bootstrap-outlier-classifier.py ... --purge --force
""",
    )
    p.add_argument("--host",             default="auto")
    p.add_argument("--user",             default="elastic")
    p.add_argument("--password",         default="changeme")
    p.add_argument("--no-verify-ssl",    action="store_true")
    p.add_argument("--kibana-host",      default="http://localhost:5601")
    p.add_argument("--skip-kibana",      action="store_true")
    p.add_argument("--outlier-model-id", default=None,
                   help="Trained model ID from the outlier DFA job. "
                        "Auto-detected if omitted.")
    p.add_argument("--threshold",        type=float, default=0.5,
                   help="Outlier score threshold for is_anomalous=true "
                        "(default: 0.5)")
    p.add_argument("--skip-backfill",    action="store_true",
                   help="Skip reindexing historical docs with is_anomalous")
    p.add_argument("--purge",            action="store_true")
    p.add_argument("--force",            action="store_true")
    args = p.parse_args()

    # Resolve connection
    host, user, password, no_ssl, kibana_host = (
        args.host, args.user, args.password, args.no_verify_ssl, args.kibana_host
    )
    if host == "auto" and os.path.exists(OUTLIER_CONFIG):
        with open(OUTLIER_CONFIG) as f:
            cfg = json.load(f)
        host        = cfg["host"]
        user        = cfg["user"]
        password    = cfg["password"]
        no_ssl      = cfg.get("no_verify_ssl", False)
        kibana_host = cfg.get("kibana_host", kibana_host)
        print(f"  Using outlier config: {OUTLIER_CONFIG}")
    elif host == "auto":
        print("ERROR: --host not provided and workshop-config-outlier.json not found.")
        print("       Run bootstrap-outlier.py first, or pass --host explicitly.")
        raise SystemExit(1)

    verify_ssl = not no_ssl
    creds = base64.b64encode(f"{user}:{password}".encode()).decode()
    auth  = f"Basic {creds}"

    print("\n=== OpsPath Outlier → Classification Bridge — Bootstrap ===")
    print(f"  Elasticsearch : {host}")
    if not args.skip_kibana:
        print(f"  Kibana        : {kibana_host}")
    print(f"  Threshold     : {args.threshold}")
    print()

    if args.purge:
        purge(host, auth, verify_ssl,
              None if args.skip_kibana else kibana_host,
              args.force)
        return

    # Resolve the outlier model ID before doing anything else
    model_id = resolve_model_id(host, auth, verify_ssl, args.outlier_model_id)
    if not model_id:
        raise SystemExit(1)

    save_config(args, model_id)
    patch_template_and_indices(host, auth, verify_ssl)
    create_labelling_pipeline(host, auth, verify_ssl, args.threshold)
    create_chained_pipeline(host, auth, verify_ssl, model_id, args.threshold)
    wire_pipeline(host, auth, verify_ssl)

    if not args.skip_backfill:
        backfill_labels(host, auth, verify_ssl)

    if not args.skip_kibana:
        create_data_views(kibana_host, auth, verify_ssl)

    print(f"\n{'='*60}")
    print("✓ Bridge bootstrap complete.")
    print()
    print("  Pipelines created:")
    print("  • opspath-label-anomalous  (threshold script only)")
    print("  • opspath-full-ml          (outlier inference + label — wired to stream)")
    print()
    print("  Classification DFA job — created by student, not bootstrap:")
    print("  • opspath-span-classifier")
    print("    Chapter 2: create via Dev Tools once is_anomalous is populated")
    print()
    print("  Recommended next steps:")
    print("    1. Verify is_anomalous is populating:")
    print("       GET traces-apm.spans-opspath/_search")
    print('         { "query": { "exists": { "field": "is_anomalous" } } }')
    print("    2. Check class balance:")
    print("       FROM traces-apm.spans-opspath")
    print("       | STATS COUNT(*) BY is_anomalous")
    print("    3. Follow instruqt-chapter-bridge.md")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
