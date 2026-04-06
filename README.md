# LendPath ML Classification Workshop
## Elastic Machine Learning — Focused Edition

> *Learn how a fictional mortgage lender's data tells a story of risk — and how Elastic Machine Learning turns that story into real-time anomaly alerts, behavioral outliers, and predictive models that flag problems before they become losses.*

---

## What You'll Build

This workshop has two parts. Each part uses different Elastic ML capabilities against real synthetic mortgage platform data — no toy datasets.

| | Part 1 | Part 2 |
|---|---|---|
| **Goal** | Deploy a trained model as a live inference pipeline | Use AI + Automation to build and train a new model |
| **Data** | PingOne IAM + Oracle DB audit | LendPath internal audit log |
| **ML Job** | `mortgage-privileged-access-classification` | `mortgage-audit-classification` |
| **Key tools** | DFA · Ingest Pipelines · Discover | Agent Builder · Elastic Workflows · DFA |

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
│  • action.type       │  • privilege         │  • is_suspicious      │
│  • result.status     │  • event.outcome     │  • mfa_used           │
│  • risk.level ←label │                      │  • off_hours          │
└──────────┬───────────┴──────────┬───────────┴──────────┬────────────┘
           │                     │                       │
           └──────────┬──────────┘                       │
                      ▼                                   ▼
        ┌─────────────────────────┐        ┌─────────────────────────┐
        │  PART 1                 │        │  PART 2                 │
        │  DFA Classification     │        │  Agent Builder          │
        │  mortgage-privileged-   │        │  analyzes data, then    │
        │  access-classification  │        │  Workflow creates and   │
        │                         │        │  configures DFA job     │
        │  Predicts: LOW/MED/HIGH │        │  mortgage-audit-        │
        └──────────┬──────────────┘        │  classification         │
                   │                       └──────────┬──────────────┘
                   ▼                                  ▼
        ┌─────────────────────────┐        ┌─────────────────────────┐
        │  Ingest Pipeline        │        │  Ingest Pipeline        │
        │  mortgage-privileged-   │        │  mortgage-audit-ml      │
        │  access-ml              │        │                         │
        │                         │        │  ml.is_suspicious_      │
        │  ml.risk_level_         │        │  prediction on every    │
        │  prediction on every    │        │  new audit event        │
        │  new IAM/DB event       │        └─────────────────────────┘
        └─────────────────────────┘
```

---

## Files in This Workshop

| File | Purpose |
|---|---|
| `bootstrap-classification.py` | Creates index templates, DFA jobs, Kibana data views |
| `sdg-prime-classification.py` | Generates synthetic data for all 3 streams |
| `classification-workshop.yml` | SDG field schema reference |
| `business_calendar.py` | US Federal holiday + diurnal pattern calendar |
| `WORKSHOP_FOCUSED.md` | Full instructor guide |
| `WORKSHOP_FOCUSED.pdf` | Printable version |

---

## Quick Start

### Prerequisites

- Elastic Stack 9.2+ or Serverless
- Workflows feature enabled
- LLM connector configured (OpenAI, Azure OpenAI, Bedrock, or local)
- Python 3.9+ with `elasticsearch` and `faker` packages

```bash
pip install elasticsearch faker
```

### Step 1 — Bootstrap

Creates index templates, DFA job definitions, and Kibana data views.
Saves `workshop-config.json` so subsequent scripts need no connection args.

```bash
python bootstrap-classification.py \
    --host https://your-es-host:9200 \
    --kibana-host http://your-kibana:5601 \
    --user elastic \
    --password changeme \
    --no-verify-ssl
```

### Step 2 — Generate Data

Generates 30 days of synthetic data across all 3 streams.
Reads connection settings from `workshop-config.json` automatically.

```bash
python sdg-prime-classification.py \
    --days 30 \
    --events-per-day 2000 \
    --anomaly-pct 15 \
    --timezone "America/New_York"
```

**Parameter guide:**

| Parameter | Default | Notes |
|---|---|---|
| `--days` | 30 | Historical window. 30 days gives the model good baseline coverage. |
| `--events-per-day` | 2000 | Per stream per weekday. Weekends auto-reduced to 15%. |
| `--anomaly-pct` | 15 | % of events labeled as anomalous. Keep between 10–30% for a balanced model. |
| `--timezone` | System local | Must match your Kibana browser timezone or the diurnal peak will appear shifted. |
| `--backfill-only` | off | Stop after backfill completes — skip live generation. |

After the backfill completes the generator **automatically transitions to live mode**:
it calculates how many events were already written for today, generates only the
remaining events for the rest of the day at real-time rate, then rolls into
continuous full-day generation until you press Ctrl+C.

Example: `--events-per-day 100000`, backfill wrote 25,750 events for today →
live mode generates the remaining 74,250 events spread across today's remaining hours.

**List available timezones:**
```bash
python sdg-prime-classification.py --list-timezones
```

**Backfill only (no live generation):**
```bash
python sdg-prime-classification.py ... --backfill-only
```

### Step 3 — Start Part 1

Start the `mortgage-privileged-access-classification` DFA job in Kibana and follow the Part 1 instructions in `WORKSHOP_FOCUSED.md`.

**Kibana → Machine Learning → Data Frame Analytics → mortgage-privileged-access-classification → ▶**

### Step 4 — Start Part 2

Enable Workflows in Stack Management, then follow the Part 2 instructions to build the AI Agent and automation Workflow.

**Kibana → Stack Management → Advanced Settings → Enable Elastic Workflows**

### Purge and Reset

```bash
python bootstrap-classification.py \
    --host https://your-es-host:9200 \
    --kibana-host http://your-kibana:5601 \
    --user elastic --password changeme \
    --no-verify-ssl \
    --purge

# Skip confirmation prompt:
python bootstrap-classification.py ... --purge --force
```

---

## Part 1 — Privileged Access Pipeline

### What it does

Trains a classification model on cross-source data combining PingOne IAM signals with Oracle database privilege data, then deploys it as an ingest pipeline so every new event is scored automatically.

### Data flow

```
  logs-ping_one.audit-mortgage          logs-oracle.database_audit-mortgage
  ┌─────────────────────────────┐       ┌────────────────────────────────────┐
  │ ping_one.audit.risk.score   │       │ oracle.database_audit.action       │
  │ ping_one.audit.action.type  │       │ oracle.database_audit.privilege    │
  │ ping_one.audit.result.status│       │ event.outcome                      │
  │ event.outcome               │       │                                    │
  │ ping_one.audit.risk.level   │       │  (Oracle docs: PingOne fields null)│
  │ (dependent variable) ◄──────┼───────┼──► Model handles nulls: trained on │
  └──────────────┬──────────────┘       │   same mixed-source dataset        │
                 │                      └─────────────────┬──────────────────┘
                 └──────────────────┬───────────────────┘
                                    ▼
                    ┌───────────────────────────────┐
                    │  DFA Classification Job        │
                    │  mortgage-privileged-access-   │
                    │  classification                │
                    │                               │
                    │  Training: 80%  Test: 20%     │
                    │  Classes: LOW / MEDIUM / HIGH  │
                    │  Objective: maximize_minimum_  │
                    │            recall              │
                    └──────────────┬────────────────┘
                                   │  Trained model →
                                   ▼
                    ┌───────────────────────────────┐
                    │  Ingest Pipeline               │
                    │  mortgage-privileged-access-ml │
                    │                               │
                    │  Applied to BOTH indices       │
                    │  PingOne fields null for       │
                    │  Oracle docs → handled         │
                    │                               │
                    │  Writes to each document:      │
                    │  ml.risk_level_prediction      │
                    │  ml.top_classes[].probability  │
                    │  ml.inference_error (on fail)  │
                    └───────────────────────────────┘
```

### Steps summary

1. Confirm trained model exists: `GET /_ml/trained_models?tags=mortgage-privileged-access-classification`
2. Create ingest pipeline `mortgage-privileged-access-ml` with the inference processor
3. Simulate against PingOne doc and Oracle doc to verify null handling
4. Apply pipeline to current backing indices for both streams
5. Update both index templates so future rollovers inherit the pipeline
6. Open Discover → filter on `ml.risk_level_prediction: HIGH`

### Key insight

> A PingOne document has null Oracle fields. An Oracle document has null PingOne fields. The model was trained on the same mixed dataset so it already knows how to score either document type with only half the features present. No special handling needed — `on_failure` catches the edge cases.

---

## Part 2 — AI Agent + Workflow Automation

### What it does

Uses Elastic Agent Builder to analyze historical audit data and surface ML readiness signals, then uses Elastic Workflows to automatically create and configure the `mortgage-audit-classification` DFA job — replacing what would otherwise be a manual multi-step process.

### Exercise flow

```
  ┌─────────────────────────────────────────────────────────────────┐
  │                    logs-mortgage.audit-default                   │
  │   audit.risk_score · audit.is_suspicious · audit.mfa_used       │
  │   audit.off_hours · audit.new_device · user.roles               │
  │   event.action · source.geo.country_iso_code                    │
  └────────────────────────────┬────────────────────────────────────┘
                               │
              ┌────────────────▼────────────────┐
              │         AI AGENT                │
              │  "LendPath Audit ML Analyst"    │
              │                                 │
              │  Tool 1: Audit Risk Analysis    │
              │  → ES|QL: stats by user.roles   │
              │                                 │
              │  Tool 2: Suspicious Details     │
              │  → ES|QL: top risky events      │
              │                                 │
              │  Tool 3: Feature Distribution   │
              │  → ES|QL: class balance check   │
              │                                 │
              │  Output: "15.2% suspicious,     │
              │  data is suitable for training, │
              │  top features: off_hours,        │
              │  new_device, risk_score"        │
              └────────────────┬────────────────┘
                               │  Human reviews recommendation
                               ▼
              ┌────────────────────────────────────────────────────┐
              │              ELASTIC WORKFLOW YAML                  │
              │  Trigger: manual                                    │
              │                                                     │
              │  Step 1: count_total_docs ────► check ≥ 5,000 docs │
              │  Step 2: count_suspicious  ─┐                      │
              │  Step 3: count_not_suspicious┘► log class balance  │
              │  Step 4: check_job_exists ────► skip if exists     │
              │  Step 5: create_dfa_job ──────► PUT to ML API      │
              │  Step 6: final_summary ───────► log all counts     │
              └────────────────┬───────────────────────────────────┘
                               │  Job created — human reviews config
                               ▼
              ┌─────────────────────────────────┐
              │  DFA Classification Job          │
              │  mortgage-audit-classification  │
              │                                 │
              │  Source: audit-default          │
              │  Label:  audit.is_suspicious    │
              │  Train:  80%  Test: 20%         │
              │  ▶ Started manually in Kibana   │
              └────────────────┬────────────────┘
                               │  Training complete →
                               ▼
              ┌─────────────────────────────────┐
              │  Ingest Pipeline                 │
              │  mortgage-audit-ml              │
              │                                 │
              │  ml.is_suspicious_prediction    │
              │  ml.prediction_probability      │
              │  ml.top_classes                 │
              └─────────────────────────────────┘
```

### The three automation levels

```
  LEVEL 1 — MANUAL                    You configure everything by hand.
  ────────────────────────────────    Full control. Takes 20–30 minutes.
  Create pipeline → wire indices

  LEVEL 2 — AI ASSISTED               Agent analyzes your actual data
  ────────────────────────────────    and tells you what to configure.
  Agent Builder → chat → insight      Takes 5 minutes of conversation.

  LEVEL 3 — AUTOMATED                 Workflow does the work.
  ────────────────────────────────    Data check → job create → summary.
  Elastic Workflows → YAML → run      Takes 30 seconds.
```

### Why the Workflow intentionally doesn't auto-start the job

The Workflow creates the DFA job but stops short of starting it. In a mortgage compliance environment, an analyst reviews the job configuration before training begins — the model will score real audit events in production, so a human checkpoint before training is the right call. To add auto-start, add one step to the YAML after `create_dfa_job`:

```yaml
- name: start_dfa_job
  type: elasticsearch.request
  with:
    method: POST
    path: "/_ml/data_frame/analytics/{{ consts.jobId }}/_start"
```

---

## Data Generation Details

### Anomaly embedding logic

The generator doesn't randomly flip a label — it correlates suspicious signals so the model has real patterns to learn:

**Audit anomalies (`audit.is_suspicious = true`)**

```
  risk_score       → 65–100  (vs 0–50 for normal)
  off_hours        → 75% true  (vs 10%)
  new_device       → 70% true  (vs 5%)
  mfa_used         → 20% true  (vs 85%)
  event.action     → bulk_export, admin_impersonation,
                     ssn_accessed, rate_overridden ...
  source.geo       → 60% foreign country  (vs domestic)
```

**PingOne anomalies (`ping_one.audit.risk.level = HIGH`)**

```
  risk.score       → 60–100  (vs 0–40 for normal)
  result.status    → FAILED / BLOCKED / REQUIRES_MFA
  event.action     → USER.MFA.BYPASS, USER.AUTHENTICATION.FAILED,
                     USER.ACCOUNT.LOCKED, ADMIN.POLICY.UPDATED ...
  event.outcome    → failure  (65%)
```

**Oracle anomalies (high-privilege actions)**

```
  privilege        → SYSDBA / DBA / CREATE ANY PROCEDURE
  action           → DROP TABLE, DROP USER, GRANT,
                     EXPORT, CREATE OR REPLACE PROCEDURE ...
  event.outcome    → success + high privilege = most suspicious
```

### Calendar behavior

| Day type | Volume | Source |
|---|---|---|
| Weekday (non-holiday) | 100% of `--events-per-day` | `business_calendar.py` |
| Weekend | 15% of `--events-per-day` | `business_calendar.py` |
| US Federal holiday | 15% of `--events-per-day` | `business_calendar.py` |
| Fallback (no calendar) | weekday/weekend only | `d.weekday() >= 5` |

Diurnal pattern peaks at 10:00–12:00 local time, near-zero 22:00–05:00.

---

## Troubleshooting

**`No field [audit.is_suspicious] could be detected`**
Bootstrap must run before the generator. The index template creates the boolean mapping — without it, Elasticsearch maps `audit.is_suspicious` as keyword and DFA rejects it.

**`No known trained model with model_id [mortgage-privileged-access-classification]`**
Outlier detection jobs don't produce trained models. Classification jobs do. Confirm the DFA job has completed and is in the `inference` phase, then get the full timestamped model ID:
```
GET /_ml/trained_models?tags=mortgage-privileged-access-classification&human=true
```

**Pipeline not scoring new documents**
The index template update only affects future rollovers. Apply the pipeline to the current backing index directly:
```
GET /_cat/indices/.ds-logs-ping_one.audit-mortgage*?v&h=index
PUT /.ds-logs-ping_one.audit-mortgage-<date>-000001/_settings
{ "index": { "default_pipeline": "mortgage-privileged-access-ml" } }
```

**Agent Builder not visible in Kibana**
Requires Elastic Stack 9.2+ or Serverless. On self-managed, enable via:
```
Stack Management → Advanced Settings → Enable Agent Builder
```

**Workflows not in navigation**
```
Stack Management → Advanced Settings → Enable Elastic Workflows
```
Then refresh Kibana — Workflows appears under Analytics in the left nav.

**Diurnal peak appears shifted in dashboards**
The generator used UTC while your Kibana browser is in a different timezone. Purge and re-run with the correct timezone:
```bash
python sdg-prime-classification.py ... --timezone "America/New_York"
```

---

## License

Workshop content and synthetic data generators are provided for educational use.
LendPath is a fictional company. All data is synthetically generated.
