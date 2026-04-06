# LendPath ML Classification Workshop
## Elastic Machine Learning — Focused Edition

> *Learn how a fictional mortgage lender's data tells a story of risk — and how Elastic
> Machine Learning turns that story into real-time predictions, AI-assisted analysis,
> and fully automated model creation.*

---

## What You'll Build

Four chapters. Three data streams. Two classification models. One end-to-end automation arc.

| | Chapter 1 | Chapter 2 |
|---|---|---|
| **Goal** | Build, train, and deploy a DFA classification job manually | Use AI + Workflows to discover data, assess ML readiness, and automate job creation |
| **Data** | PingOne IAM + Oracle DB audit | LendPath internal audit log |
| **ML Job** | `mortgage-privileged-access-classification` | `mortgage-audit-classification` |
| **Key tools** | DFA · Dev Tools · Ingest Pipelines · Discover | Agent Builder · Elastic Workflows · DFA |

---

## Workshop Structure

```
Chapter 0 — Setup & Environment Review
  Verify data, explore streams, understand the LendPath scenario

Chapter 1 — Build and Train a Classification Job
  1.1  Explore source data in Discover
  1.2  Check class balance with ES|QL
  1.3  Create the DFA job via Dev Tools (includes, excludes, hyperparameters)
  1.4  Start and monitor training
  1.5  Explore results: confusion matrix, feature importance, hyperparameter stats
  1.6  Confirm the trained model
  1.7  Deploy as an ingest pipeline
  1.8  Wire the pipeline to both source indices
  1.9  Observe live predictions in Discover

Chapter 2 — AI Agent Analysis and Workflow Automation
  2.1  Enable Workflows
  2.2  Build the ML Readiness Analyst agent (built-in platform tools only)
  2.3  Chat with the agent — schema discovery, class balance, feature analysis
  2.4  Build the parameterised Automation Workflow
  2.5  Review workflow execution log
  2.6  Start the job and monitor training
  2.7  Deploy as inference pipeline

Chapter 3 — Final Thoughts & Take-Aways
  The three automation levels
  Student challenge: apply to a network traffic dataset
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     LendPath Mortgage Platform                       │
├──────────────────────┬──────────────────────┬───────────────────────┤
│  logs-ping_one       │  logs-oracle         │  logs-mortgage        │
│  .audit-mortgage     │  .database_audit     │  .audit-default       │
│                      │  -mortgage           │                       │
│  IAM events          │  DB audit events     │  Internal audit log   │
│  • risk.score        │  • action            │  • risk_score         │
│  • action.type       │  • privilege         │  • is_suspicious ◄─── label
│  • result.status     │  • event.outcome     │  • mfa_used           │
│  • risk.level ◄──────┼── label              │  • off_hours          │
└──────────┬───────────┴──────────┬───────────┴──────────┬────────────┘
           │                     │                       │
           └──────────┬──────────┘                       │
                      ▼                                   ▼
       ┌──────────────────────────┐       ┌──────────────────────────┐
       │  CHAPTER 1               │       │  CHAPTER 2               │
       │  mortgage-privileged-    │       │  Agent Builder           │
       │  access-classification   │       │  → discovers schema      │
       │                          │       │  → assesses ML readiness │
       │  Classes: LOW/MED/HIGH   │       │  → triggers Workflow     │
       │  Source: PingOne+Oracle  │       │                          │
       └─────────────┬────────────┘       │  Elastic Workflow        │
                     │                    │  → checks data readiness │
                     ▼                    │  → creates DFA job       │
       ┌──────────────────────────┐       │  mortgage-audit-         │
       │  Ingest Pipeline         │       │  classification          │
       │  mortgage-privileged-    │       └─────────────┬────────────┘
       │  access-ml               │                     │
       │                          │                     ▼
       │  ml.risk_level_prediction│       ┌──────────────────────────┐
       │  on every new event      │       │  Ingest Pipeline         │
       └──────────────────────────┘       │  mortgage-audit-ml       │
                                          │                          │
                                          │  ml.is_suspicious_       │
                                          │  prediction on every     │
                                          │  new audit event         │
                                          └──────────────────────────┘
```

---

## Files in This Workshop

| File | Purpose |
|---|---|
| `bootstrap-classification.py` | Creates index templates, data streams, patches mappings, Kibana data views |
| `sdg-prime-classification.py` | Generates synthetic data — backfill + live generation |
| `classification-workshop.yml` | SDG field schema reference |
| `business_calendar.py` | US Federal holiday + diurnal pattern calendar |
| `WORKSHOP_FOCUSED.md` | Full instructor/student workshop guide |
| `WORKSHOP_FOCUSED.html` | HTML version with copy buttons on all code blocks |
| `WORKSHOP_FOCUSED.pdf` | Printable version |

---

## Quick Start

### Prerequisites

- Elastic Stack 9.2+ or Serverless
- Workflows feature enabled in Advanced Settings
- LLM connector configured (OpenAI, Azure OpenAI, Bedrock, or local model)
- Python 3.9+ with dependencies:

```bash
pip install elasticsearch faker
```

### Step 1 — Bootstrap

Creates index templates with cross-index consistent field mappings, explicit data
streams, patches any existing backing index mappings, and creates Kibana data views.
Saves `workshop-config.json` so subsequent scripts pick up connection settings automatically.

```bash
python bootstrap-classification.py \
    --host https://your-es-host:9200 \
    --kibana-host http://your-kibana:5601 \
    --user elastic \
    --password changeme \
    --no-verify-ssl
```

### Step 2 — Generate Data

Backfills historical data across all three streams then automatically transitions
to live generation, producing new events in real time until stopped.

```bash
python sdg-prime-classification.py \
    --days 30 \
    --events-per-day 2000 \
    --anomaly-pct 15 \
    --timezone "America/New_York"
```

**Generation modes:**

| Mode | Command | Behaviour |
|---|---|---|
| Default | *(no flag)* | Backfill N days, then live generation |
| Backfill only | `--backfill-only` | Stop after backfill, no live generation |
| Live only | `--live-only` | Skip backfill, generate new events from now |

**Live generation behaviour:** After backfill completes, the generator calculates how
many events were already written for today and generates only the remaining quota.
Example: `--events-per-day 100000`, backfill wrote 25,750 events today →
live mode generates the remaining 74,250 across today's remaining hours.

**Full parameter reference:**

| Parameter | Default | Notes |
|---|---|---|
| `--days` | 30 | Historical backfill window |
| `--events-per-day` | 2000 | Per stream, per weekday. Weekends = 15% |
| `--anomaly-pct` | 15 | % anomalous events. Keep 10–30% for good class balance |
| `--timezone` | System local | Must match Kibana browser timezone |
| `--workers` | CPU cores × 2 | Backfill parallelism. Auto-detected, cap 32 |
| `--backfill-only` | off | Stop after backfill |
| `--live-only` | off | Skip backfill, live events only |

```bash
# List available timezones
python sdg-prime-classification.py --list-timezones
```

### Step 3 — Follow the Workshop Guide

Open `WORKSHOP_FOCUSED.html` or `WORKSHOP_FOCUSED.pdf` and begin at Chapter 0.

The DFA jobs are **not** pre-created by bootstrap:

- `mortgage-privileged-access-classification` → created by the student in Chapter 1 via Dev Tools
- `mortgage-audit-classification` → created automatically by the Elastic Workflow in Chapter 2

### Step 4 — Purge and Reset

```bash
# With confirmation prompt
python bootstrap-classification.py \
    --host https://your-es-host:9200 \
    --kibana-host http://your-kibana:5601 \
    --user elastic --password changeme \
    --no-verify-ssl \
    --purge

# Skip the YES prompt
python bootstrap-classification.py ... --purge --force
```

Purge removes both DFA jobs, destination indices, all three data streams (and their
backing indices), index templates, and Kibana data views. Trained models are **not**
purged — delete those manually in Kibana under Machine Learning → Trained Models.

---

## Data Generation Details

### Three streams, three purposes

| Stream | Label field | Used in |
|---|---|---|
| `logs-ping_one.audit-mortgage` | `ping_one.audit.risk.level` (LOW/MEDIUM/HIGH) | Chapter 1 — source for `mortgage-privileged-access-classification` |
| `logs-oracle.database_audit-mortgage` | *(no label — provides cross-source features)* | Chapter 1 — second source for same job |
| `logs-mortgage.audit-default` | `audit.is_suspicious` (boolean) | Chapter 2 — source for `mortgage-audit-classification` |

### Anomaly embedding

The generator doesn't randomly flip labels — it correlates signals so the model
has real patterns to learn from.

**Audit stream — `audit.is_suspicious = true` (~anomaly-pct % of events)**

```
risk_score     → 65–100      (normal: 0–50)
off_hours      → 75% true    (normal: 10%)
new_device     → 70% true    (normal: 5%)
mfa_used       → 20% true    (normal: 85%)
event.action   → bulk_export, admin_impersonation, ssn_accessed, rate_overridden
source.geo     → 60% foreign (normal: domestic)
```

**PingOne stream — `ping_one.audit.risk.level = HIGH` (~anomaly-pct % of events)**

```
risk.score     → 60–100      (normal: 0–40)
result.status  → FAILED / BLOCKED / REQUIRES_MFA
event.action   → USER.MFA.BYPASS, USER.AUTHENTICATION.FAILED, USER.ACCOUNT.LOCKED
event.outcome  → failure (65%)
```

**Oracle stream — high-privilege actions (~anomaly-pct % of events)**

```
privilege      → SYSDBA / DBA / CREATE ANY PROCEDURE
action         → DROP TABLE, DROP USER, GRANT, EXPORT, CREATE OR REPLACE PROCEDURE
event.outcome  → success + high privilege = most suspicious combination
```

### Calendar and diurnal pattern

| Day type | Volume |
|---|---|
| Weekday (non-holiday) | 100% of `--events-per-day` |
| Weekend | 15% of `--events-per-day` |
| US Federal holiday | 15% of `--events-per-day` |

Peak hours: 10:00–12:00 local time. Near-zero: 22:00–05:00.

---

## Cross-Index Mapping Consistency

The `mortgage-privileged-access-classification` job sources from two separate indices.
Elasticsearch requires that any field shared across both indices has **identical
mapping definitions** — otherwise the DFA job fails to start with a merge error.

Bootstrap ensures consistency across four shared field groups:

| Field group | PingOne template | Oracle template |
|---|---|---|
| `event.*` | `kind, category, type, action, outcome, dataset` | identical |
| `user.*` | `id, name, email, roles` | identical |
| `client.user.*` | `id, name` | identical |
| `source.*` | `ip, geo.*` | identical |

`patch_existing_mappings()` applies these to all current backing indices at runtime,
so re-running bootstrap on a populated cluster fixes mapping gaps without purging data.

---

## Troubleshooting

**DFA job fails to start — mapping merge error**

```
cannot merge [properties] mappings because of differences for field [client]
```

Re-run bootstrap. `patch_existing_mappings()` will apply the correct shared mapping
to all current backing indices. If the problem persists, apply manually:

```
PUT /logs-oracle.database_audit-mortgage/_settings
{ ... }   ← see Chapter 0 in WORKSHOP_FOCUSED.md for the full payload
```

**`No field [audit.is_suspicious] could be detected`**

Bootstrap must run before the generator. The index template sets the boolean mapping.
Without it, Elasticsearch maps the field as keyword and DFA rejects it.

**`No known trained model with model_id [mortgage-privileged-access-classification]`**

The job must complete all training phases including `inference`. Get the full
timestamped model ID:

```
GET /_ml/trained_models?tags=mortgage-privileged-access-classification&human=true
```

**Pipeline not scoring new documents**

Apply the pipeline directly to the data stream — this propagates to the current
backing index and all future rollovers:

```
PUT /logs-ping_one.audit-mortgage/_settings
{ "index": { "default_pipeline": "mortgage-privileged-access-ml" } }

PUT /logs-oracle.database_audit-mortgage/_settings
{ "index": { "default_pipeline": "mortgage-privileged-access-ml" } }
```

**Agent Builder not visible in Kibana**

Requires Elastic Stack 9.2+ or Serverless. Enable via:
`Stack Management → Advanced Settings → Enable Agent Builder`

**Workflows not in navigation**

`Stack Management → Advanced Settings → Enable Elastic Workflows`
then refresh — Workflows appears under Analytics in the left nav.

**Agent hangs on unfamiliar clusters without generating output**

The system prompt instructs the agent to call `list_indices` as its mandatory first
action. If your agent was created before this was added, update the system prompt —
see the ML Readiness Analyst system prompt in Chapter 2 of WORKSHOP_FOCUSED.md.

**Diurnal peak appears shifted in Kibana dashboards**

The generator used UTC while your browser is in a different timezone. Purge and
re-run with the correct timezone flag:

```bash
python sdg-prime-classification.py ... --timezone "America/New_York"
```

---

## License

Workshop content and synthetic data generators are provided for educational use.
LendPath is a fictional company. All data is synthetically generated.
