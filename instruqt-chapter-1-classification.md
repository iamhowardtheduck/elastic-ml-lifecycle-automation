# Chapter 1 — Build and Train a Classification Job

> **Objective:** Create, train, and deploy the `mortgage-privileged-access-classification` Data Frame Analytics job from scratch — exploring the Kibana wizard, recreating the job via Dev Tools to add excludes and hyperparameters, evaluating results, and wiring an inference pipeline so every new event is scored in real time.

**Objective:** Create a multi-class Data Frame Analytics classification job from scratch
using the Kibana UI. Configure it to predict whether a privileged access event is
`LOW`, `MEDIUM`, or `HIGH` risk by learning from two data sources simultaneously —
PingOne IAM audit events and Oracle database audit events.

By the end of Part 1 you will have:
- A trained classification model ready for deployment
- Understood how DFA handles cross-source datasets with null fields
- Deployed the model as an ingest pipeline scoring new events in real time
- Seen live predictions appear in Discover

---


## 1.1 — Explore the Source Data
===

Before building the job, understand what you are training on.

Set your time-picker to 30 days ago:
![Apr-06-2026_at_16.07.42-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/e2570bc213c5efd5eab8918cb711b710/assets/Apr-06-2026_at_16.07.42-image.png)

Our data generator automatically filled in peeks for standard work hours and weekends:
![Apr-06-2026_at_16.08.29-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/0f2e99dcaa8b4e05ef32673c4576fd30/assets/Apr-06-2026_at_16.08.29-image.png)

### PingOne audit data

Select the `logs-ping_one.audit-mortgage` data view.
![Apr-06-2026_at_16.11.03-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/6c583d933afb5845ebcdc234abc7f57b/assets/Apr-06-2026_at_16.11.03-image.png)

Add these columns:

- `ping_one.audit.risk.level` — this is the **label** the model will learn to predict
- `ping_one.audit.risk.score` — numeric risk signal 0–100
- `ping_one.audit.action.type` — IAM action category
- `ping_one.audit.result.status` — SUCCESS, FAILED, BLOCKED, REQUIRES_MFA
- `event.outcome` — success or failure

Look at a few documents. Notice that `LOW` risk events dominate, `HIGH` risk events are
rarer, and `MEDIUM` sits between them. This is the class distribution the model will
learn from.

![Apr-06-2026_at_16.12.13-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/7bd5f86204a470e20f188439287c10a1/assets/Apr-06-2026_at_16.12.13-image.png)

### Oracle audit data

Switch to the `logs-oracle.database_audit-mortgage` data view.
![Apr-06-2026_at_16.12.50-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/dd5b9ef930d7abfc3fc326a929460068/assets/Apr-06-2026_at_16.12.50-image.png)

Add these columns:

- `oracle.database_audit.action` — SQL action: SELECT, DROP TABLE, GRANT, EXPORT, etc.
- `oracle.database_audit.privilege` — SYSDBA, DBA, CREATE SESSION, NOT APPLICABLE
- `event.outcome` — success or failure

Notice these documents have **no PingOne fields** — `ping_one.audit.risk.level` is
absent. The model will be trained on this mixed dataset. When an Oracle document is
scored it will have null PingOne features, and when a PingOne document is scored it
will have null Oracle features. DFA handles this naturally — missing fields are treated
as absent features, not errors.

![Apr-06-2026_at_16.14.00-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/61d98299f06e510d248c13998f57a97b/assets/Apr-06-2026_at_16.14.00-image.png)

**Discussion:** Why would you want to train a single model on both sources rather than
two separate models? What does it gain you to know that a `SYSDBA` privilege escalation
event from Oracle carries the same risk signature as a `USER.MFA.BYPASS` from PingOne?

<details>
<summary><ins>Answers</ins></summary>

## Why train one model on both sources rather than two separate models?

The core reason is that the threat you're trying to detect - (a privileged access risk event) - doesn't respect system boundaries. An attacker compromising the mortgage platform doesn't announce themselves in just one log source. The pattern that actually signals a high-risk session is often a sequence across systems: a PingOne authentication succeeds (possibly bypassing MFA), followed shortly by an Oracle SYSDBA session from the same user doing destructive or exfiltration actions. If you train two separate models, each one only sees half the picture and can only tell you "this individual event looks a bit unusual." Neither can learn that the combination of signals across both sources is what elevates the risk.
A single cross-source model sees the full feature space for each document, even when most features from the other source are null. It learns that the presence of oracle.database_audit.privilege = SYSDBA combined with event.outcome = success carries predictive weight regardless of whether the PingOne fields are present — because those combinations showed up near risk.level = HIGH in training. The model essentially becomes a unified risk scoring function for privileged access activity across your IAM and database layers, rather than two siloed detectors that have to be manually correlated after the fact.
There's also a practical data volume argument. HIGH-risk events are rare by design - that's what makes them anomalous. If you split your already-imbalanced dataset across two models, each model sees even fewer positive-class examples, making it harder to learn reliable decision boundaries for the rare cases you actually care about.

## What does it gain you to know that SYSDBA privilege escalation and USER.MFA.BYPASS carry the same risk signature?

It gives you normalization across heterogeneous systems - a single unified risk score that means the same thing regardless of where the event originated. Without this, a security analyst has to mentally translate between "PingOne severity 8" and "Oracle privilege level SYSDBA" and decide whether those are equivalent. The model has already done that translation during training, anchored to the ground truth of what events were actually labeled HIGH risk.
More practically, it means your inference pipeline produces ml.risk_level_prediction = HIGH on events from either source, and a dashboard or alert rule can treat them identically. You can build a single Discover filter, a single alert threshold, a single case creation rule. The downstream operations don't need to know whether the event came from PingOne or Oracle — the model has abstracted that away into a common risk language.
It also gives you better generalization. If a novel attack pattern appears in Oracle that the Oracle-only features wouldn't recognise as risky, but it shares feature combinations with MFA bypass patterns the model learned from PingOne data, the cross-trained model has a better chance of catching it. The feature space is richer, the decision boundary is informed by more signal, and the model is harder to evade by staying within one system's "normal" range.
</details>

---


## 1.2 — Check Class Balance
===

Before creating the job, verify you have enough of each class to train on.

Run this ES|QL query:

![Apr-06-2026_at_16.15.18-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/52062565b412c41e1a04919e825fffb3/assets/Apr-06-2026_at_16.15.18-image.png)

<pre><code>
FROM logs-ping_one.audit-mortgage
| WHERE @timestamp > NOW() - 30 days
| STATS doc_count = COUNT(*)
        BY ping_one.audit.risk.level
| SORT doc_count DESC
</code></pre>

This groups all PingOne documents by their `risk.level` value and returns the count
for each. You should see three rows — `LOW`, `MEDIUM`, and `HIGH` — with `LOW`
dominating and `HIGH` being the rarest class.

![Apr-06-2026_at_16.17.29-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/d448d3e13911dbbed7f8f70a63048655/assets/Apr-06-2026_at_16.17.29-image.png)

You need at least several hundred documents in each class for the model to learn
reliably. If `HIGH` is very sparse, the `maximize_minimum_recall` objective you will
set in the next step specifically addresses this by penalising the model for missing
rare classes.

> **ES|QL conditional counting : `COUNT` vs `SUM(CASE(...))`**
>
> `COUNT(*)` counts all rows. `COUNT(field)` counts rows where that field is
> non-null - it does **not** filter on a condition. If you were to use `COUNT(audit.risk_score > 75)`
> then this returns the same value as `COUNT(*)` because the boolean expression evaluates
> to `true` or `false`, both non-null, so every row is counted.
>
> `The `SUM(CASE(condition, 1, 0))` - scores each row 1 if the
> condition is true and 0 otherwise, and sums the result. The queries in this
> workshop all use `SUM(CASE(...))` for this reason.

Also verify the Oracle source has data, so please run this ES|QL query::

<pre><code>
FROM logs-oracle.database_audit-mortgage
| WHERE @timestamp > NOW() - 30 days
| STATS total        = COUNT(*),
        sysdba_count = SUM(CASE(oracle.database_audit.privilege == "SYSDBA", 1, 0)),
        dba_count    = SUM(CASE(oracle.database_audit.privilege == "DBA", 1, 0)),
        drop_count   = SUM(CASE(oracle.database_audit.action == "DROP TABLE", 1, 0))
</code></pre>

Oracle documents don't carry `ping_one.audit.risk.level` — they will have null for
that field. The model will train on both sources simultaneously and learn to predict
risk level from the Oracle features (`action`, `privilege`, `event.outcome`) when the
PingOne risk score is absent.

![Apr-06-2026_at_16.19.57-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/2706ab31b8134d8931d172ad94449864/assets/Apr-06-2026_at_16.19.57-image.png)

---


## 1.3 — Create the DFA Job
===

The Kibana DFA wizard is a good starting point for configuration, but it does not
expose the `analyzed_fields` excludes list — you can only select fields to include,
not fields to explicitly exclude. For this job, excluding `@timestamp`, `user.id`,
and `user.name` is essential: without it DFA will include every mapped field and the
model will overfit to individual users and timestamps rather than learning behavioral
patterns.

The approach is therefore split: use the Kibana UI to configure everything it supports
well, then use Dev Tools to create the job with the full `analyzed_fields` specification.

---

### Part A — Explore the wizard 🧙‍♂️

Navigate to **Data Frame Analytics** by searching for it in the top search bar:

<pre><code>data frame analytics</code></pre>

![Apr-06-2026_at_16.21.45-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/c7630983e18d983984bf7f3fe92dbb7e/assets/Apr-06-2026_at_16.21.45-image.png)

Now click on `Create Data Frame Analytics Job`:
![Apr-06-2026_at_16.23.55-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/5e03b0d90aaafe172dbb59e086df3a5f/assets/Apr-06-2026_at_16.23.55-image.png)

Walk through the wizard to understand the available options — this is valuable for
understanding what DFA exposes before you create the job via API. You do not need to
complete or submit the wizard.

**Step 1 — Types:** Select **Data view**.
![Apr-06-2026_at_16.26.09-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/0bd8ab42a33f6d66f532d3b701b70977/assets/Apr-06-2026_at_16.26.09-image.png)

Since we want to create a data frame analytics job which will span multiple data views, we will need to create it; press `Create a data view`:
![Apr-06-2026_at_16.27.50-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/6057cfcf1bc6332da4916c04c79488ac/assets/Apr-06-2026_at_16.27.50-image.png)

Provide the following:

- **Name**: <pre><code>logs-mortgage-privileged-access-classification</code></pre>
- **Index pattern**: <pre><code>logs-ping_one.audit-mortgage*,logs-oracle.database_audit-mortgage*</code></pre>
- **Timestamp field**: *leave the default*
- *Click on **Show advanced settings***
- **Custom data view ID**: <pre><code>logs-mortgage-privileged-access-classification</code></pre>

Press the `Save data view to Kibana`:
![Apr-06-2026_at_16.31.56-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/c1a47b39034cf9aac9bc74d9f5c7f32f/assets/Apr-06-2026_at_16.31.56-image.png)

**Step 2 — Job type:** Select **Classification**:
![Apr-06-2026_at_16.32.33-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/8fa00445b80c693b837ae2e7a660d929/assets/Apr-06-2026_at_16.32.33-image.png)

**Step 3 — Configuration:** Note that **Dependent variable** shows a dropdown of
keyword and boolean fields. `ping_one.audit.risk.level` should appear here — this is
the field the model will learn to predict.

![Apr-06-2026_at_16.33.48-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/293177d0da5267090af391b413cf1234/assets/Apr-06-2026_at_16.33.48-image.png)

Scroll past the scatter plot and then press the `Continue` button:
![Apr-06-2026_at_16.35.25-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/ba4eb54056fe2a2f281e854d577ca3b6/assets/Apr-06-2026_at_16.35.25-image.png)

**Step 4 — Additional options:** Scroll through and expand the `> Hyperparameters` to see Number of top classes, Training
percent, Class assignment objective, Prediction field name, and Feature importance.
These all map directly to the API parameters you will use in Part B.

![Apr-06-2026_at_16.38.54-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/99dec5b4f08da61f97e484dc7db74bbd/assets/Apr-06-2026_at_16.38.54-image.png)

**Step 5 — Analyzed fields:** Click **Edit** from **Step 1**.
![Apr-06-2026_at_16.40.21-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/052be6d41e8db72c6ce47f8797a02ff8/assets/Apr-06-2026_at_16.40.21-image.png)

Expand the rows per page to ***50***:
![Apr-06-2026_at_16.42.10-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/7f6d15d5ceea9786ee1c81dfb4377ae6/assets/Apr-06-2026_at_16.42.10-image.png)

This screen shows a checkbox list of available fields with an **Includes** picker. Notice there is no **Excludes** field — this is the limitation. The UI can only control what is explicitly included; it cannot declare fields that should be excluded from an otherwise broad include. Cancel out of the wizard without creating the job.

![Apr-06-2026_at_16.43.45-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/279c1a0bd0a42b90c3bc11a7f7fa3378/assets/Apr-06-2026_at_16.43.45-image.png)

> *Why does this matter?* If you created the job from the UI using only the includes
> list, DFA would still ingest fields like `@timestamp`, `user.id`, `user.name`, and
> `audit.session_id` into the destination index. It would not analyze them — because
> they are not in the includes list — but their presence inflates memory usage and
> destination index size. More importantly, the `excludes` directive is the correct
> way to tell DFA "these fields exist in the source but should never be carried into
> the analysis at all," which keeps the dest index clean and the feature space tight.

**Step 6  — Create the DFA Classification Job:**

Press `Continue` until you are on `Step 3 - Job Details`
![Apr-06-2026_at_16.51.10-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/f17edc991f5449b437e57c8bec90107c/assets/Apr-06-2026_at_16.51.10-image.png)

## Provide the following values

**Job ID**
<pre><code>mortgage-privileged-access-classification</code></pre>

**Description**
<pre><code>Multi-class classification predicting privileged access risk level across PingOne IAM and Oracle DB audit events</code></pre>

The Data view for the DFA job is preconfigured, so please disable this toggle switch, then press `Continue`:
![Apr-06-2026_at_16.54.30-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/7f497c119fbe300cf67283e67a007f89/assets/Apr-06-2026_at_16.54.30-image.png)

*The Validation step will list a warning of empty values, this is ok as we will configure the rest with DevTools. Press `Continue`*
![Apr-06-2026_at_16.55.38-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/7aaa887bffddf51ce9ebab51622301cf/assets/Apr-06-2026_at_16.55.38-image.png)

>## **IMPORTANT!!!**
>
>**DISABLE THE `Start immediately` BEFORE PRESSING CREATE**
>![Apr-06-2026_at_16.58.29-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/da33d59a44f0736328862fcc4476a13a/assets/Apr-06-2026_at_16.58.29-image.png)


---

### Part B — Retrieve the UI configuration from the API 🧲

Before writing the final `PUT`, use Dev Tools to retrieve the job definition the
wizard would have created. This shows you the exact JSON structure that Kibana
generated from your wizard selections in Part A — source indices, dependent variable,
analyzed fields, and configuration — so you can extend it rather than write it from
scratch.

Search for and select the top result for **Dev Tools** and run:
![Apr-06-2026_at_17.01.10-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/52351240173a123035c296ff22567498/assets/Apr-06-2026_at_17.01.10-image.png)

<pre><code>GET /_ml/data_frame/analytics/mortgage-privileged-access-classification</code></pre>

![Apr-06-2026_at_17.01.39-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/083066f9e897f7d78819b693df75eb5d/assets/Apr-06-2026_at_17.01.39-image.png)

If you did not save the job from the wizard this will return a 404, which is expected.
If you did save it, the response body is your starting point. It will look similar to
this (the `analyzed_fields.includes` list will reflect whatever the wizard selected):

```json
{
  "count": 1,
  "data_frame_analytics": [
    {
      "id": "mortgage-privileged-access-classification",
      "create_time": 1775568315158,
      "version": "12.0.0",
      "authorization": {
        "roles": [
          "superuser"
        ]
      },
      "description": "Multi-class classification predicting privileged access risk level across PingOne IAM and Oracle DB audit events",
      "source": {
        "index": [
          "logs-ping_one.audit-mortgage*",
          "logs-oracle.database_audit-mortgage*"
        ],
        "query": {
          "match_all": {}
        }
      },
      "dest": {
        "index": "mortgage-privileged-access-classification",
        "results_field": "ml"
      },
      "analysis": {
        "classification": {
          "dependent_variable": "ping_one.audit.risk.level",
          "num_top_feature_importance_values": 0,
          "class_assignment_objective": "maximize_minimum_recall",
          "num_top_classes": -1,
          "prediction_field_name": "ping_one.audit.risk.level_prediction",
          "training_percent": 80,
          "randomize_seed": -7454485974122494000,
          "early_stopping_enabled": true
        }
      },
      "analyzed_fields": {
        "includes": [
          "client.user.id",
          "client.user.name",
          "ecs.version",
          "event.action",
          "event.category",
          "event.dataset",
          "event.kind",
          "event.outcome",
          "event.type",
          "host.name",
          "input.type",
          "oracle.database_audit.action",
          "oracle.database_audit.action_number",
          "oracle.database_audit.database.user",
          "oracle.database_audit.entry.id",
          "oracle.database_audit.length",
          "oracle.database_audit.obj.name",
          "oracle.database_audit.obj.schema",
          "oracle.database_audit.privilege",
          "oracle.database_audit.result_code",
          "oracle.database_audit.session_id",
          "oracle.database_audit.status",
          "oracle.database_audit.terminal",
          "ping_one.audit.action.type",
          "ping_one.audit.actors.client.type",
          "ping_one.audit.actors.user.type",
          "ping_one.audit.result.description",
          "ping_one.audit.result.status",
          "ping_one.audit.risk.level",
          "ping_one.audit.risk.score",
          "process.pid",
          "related.hosts",
          "server.address",
          "server.domain",
          "source.geo.city_name",
          "source.geo.country_iso_code",
          "source.geo.region_name",
          "source.ip",
          "tags",
          "user.email",
          "user.id",
          "user.name",
          "user.roles"
        ],
        "excludes": []
      },
      "model_memory_limit": "187mb",
      "allow_lazy_start": false,
      "max_num_threads": 1
    }
  ]
}
```

Notice two things the wizard did not let you set: `analyzed_fields.excludes` is an
empty array, and the hyperparameter fields (`lambda`, `gamma`, `eta`,
`feature_bag_fraction`) are absent from the `classification` block entirely.

If you saved the job from the wizard, delete it now so you can recreate it correctly:
![Apr-06-2026_at_17.03.01-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/caeadb399e32e91720378dbf47c23036/assets/Apr-06-2026_at_17.03.01-image.png)

<pre><code>DELETE /_ml/data_frame/analytics/mortgage-privileged-access-classification</code></pre>

---

### Part B (continued) — Recreate with excludes and hyperparameters

The complete `PUT` below is the updated and correct DFA job.

The three changes from what the wizard produced are highlighted in the comments:

<pre><code>
PUT /_ml/data_frame/analytics/mortgage-privileged-access-classification
{
  "description": "Multi-class classification predicting privileged access risk level across PingOne IAM and Oracle DB audit events",
  "source": {
    "index": [
      "logs-ping_one.audit-mortgage",
      "logs-oracle.database_audit-mortgage"
    ],
    "query": { "match_all": {} }
  },
  "dest": {
    "index":         "mortgage-privileged-access-classification",
    "results_field": "ml"
  },
  "analysis": {
    "classification": {
      "dependent_variable":                "ping_one.audit.risk.level",
      "training_percent":                  80,
      "num_top_classes":                   3,
      "prediction_field_name":             "risk_level_prediction",
      "num_top_feature_importance_values": 5,
      "class_assignment_objective":        "maximize_minimum_recall",
      "lambda":               0.1,
      "gamma":                0.1,
      "eta":                  0.1,
      "feature_bag_fraction": 0.5
    }
  },
  "analyzed_fields": {
    "includes": [
      "ping_one.audit.risk.score",
      "ping_one.audit.risk.level",
      "ping_one.audit.action.type",
      "ping_one.audit.result.status",
      "oracle.database_audit.action",
      "oracle.database_audit.privilege",
      "event.outcome"
    ],
    "excludes": [
      "@timestamp",
      "user.id",
      "user.name"
    ]
  },
  "model_memory_limit": "550mb",
  "allow_lazy_start":   false,
  "max_num_threads":    1
}
</code></pre>

![Apr-06-2026_at_17.05.21-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/b4f0c7ffcc01981f086586ce44c42562/assets/Apr-06-2026_at_17.05.21-image.png)

A `200` response with `"acknowledged": true` confirms the job was created in stopped
state.

**The three additions from what the wizard produced:**

`lambda`, `gamma`, `eta`, `featurebagfraction` — added as direct fields inside
`analysis.classification`. These are the four hyperparameters you set manually.
The API parser rejects a nested `"hyperparameters": {}` object as an unknown field;
they must be flat siblings of `dependent_variable`.

`analyzedfields.excludes` — populated with `@timestamp`, `user.id`, and `user.name`.
The wizard left this as an empty array. Adding these here tells DFA to strip them
from the analysis entirely, keeping the feature space clean and preventing the model
from learning time-based or identity-based patterns.

**Why these specific includes and excludes:**

| Field | Role | Why |
|---|---|---|
| `ping_one.audit.risk.score` | Feature | Numeric 0–100 IAM risk signal — strongest single predictor |
| `ping_one.audit.risk.level` | **Dependent variable** | The label the model learns to predict: LOW / MEDIUM / HIGH |
| `ping_one.audit.action.type` | Feature | IAM action category — AUTHENTICATION, MFA, ADMIN |
| `ping_one.audit.result.status` | Feature | SUCCESS, FAILED, BLOCKED — strong failure signal |
| `oracle.database_audit.action` | Feature | SQL action — DROP TABLE, GRANT, SELECT |
| `oracle.database_audit.privilege` | Feature | SYSDBA, DBA, NOT APPLICABLE — privilege level |
| `event.outcome` | Feature | Cross-source success/failure signal present in both sources |
| `@timestamp` | **Excluded** | Time must never be a feature — the model would learn when events occur rather than what makes them risky |
| `user.id` | **Excluded** | Individual identity must never be a feature — the model would memorize specific users rather than behavioral patterns |
| `user.name` | **Excluded** | Same reason as `user.id` |

**Verify the job was created correctly:**

<pre><code>GET /_ml/data_frame/analytics/mortgage-privileged-access-classification</code></pre>

![Apr-06-2026_at_17.06.50-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/b9272c372b948d7d85e83f3981160cca/assets/Apr-06-2026_at_17.06.50-image.png)

Confirm `analyzed_fields.excludes` contains all three excluded fields and the four
hyperparameter values appear inside `analysis.classification`.

---


## 1.4 — Start the Job and Monitor Training
===

Search for and select the top result for <pre><code>data frame analytics jobs</code></pre>

![Apr-06-2026_at_17.08.12-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/c910082e52721e59f99481617a8e8a87/assets/Apr-06-2026_at_17.08.12-image.png)

Click **▶ Start** on the `mortgage-privileged-access-classification` row.
![Apr-06-2026_at_17.09.12-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/cc375b96bd499da35732c376596c300c/assets/Apr-06-2026_at_17.09.12-image.png)
![Apr-06-2026_at_17.09.40-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/7510a47498f2d3d3b4c383e9808de7b6/assets/Apr-06-2026_at_17.09.40-image.png)

The job moves through eight phases,  press `Refresh` to observe the progress:

| Phase | What happens |
|---|---|
| `reindexing` | All documents from both source indices are copied to the destination index |
| `loading_data` | Features are extracted and prepared for the boosted tree algorithm |
| `feature_selection` | Fields with zero variance or no statistical correlation to the dependent variable are identified and dropped before training begins |
| `coarse_parameter_search` | A Bayesian search sweeps the hyperparameter space to find rough starting values for any parameters left unset ~ the slowest phase |
| `fine_tuning_parameters` | The coarse candidates are narrowed to final values for each auto-tuned hyperparameter |
| `final_training` | Predictions and feature importance scores are written back to the destination index for every document in the held-out test set |
| `writing_results` | The trained model writes predictions to the remaining 20% of documents |
| `inference` | The model is evaluated against the test set and accuracy metrics (precision, recall, and the confusion matrix) are computed and stored |

Training typically takes 3–8 minutes depending on document count and cluster size.
Watch the progress bar in **Data Frame Analytics → Jobs**.

![Apr-06-2026_at_17.10.35-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/ac0f1abff4443873d39adf1b230532b2/assets/Apr-06-2026_at_17.10.35-image.png)

While it trains...

![Apr-06-2026_at_17.11.34-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/6a38aa8d35651af7d88d730ce50aa16a/assets/Apr-06-2026_at_17.11.34-image.png)

> *DFA is training on a dataset where roughly half the documents have null PingOne
> fields and half have null Oracle fields. The model is learning that certain
> combinations — SYSDBA privilege with a failed outcome, or USER.MFA.BYPASS from
> PingOne — are both indicative of HIGH risk, even though they come from completely
> different systems. This cross-source pattern recognition is something a rule-based
> system could not do.*

---

## 1.5 — Explore the Training Results
===

### Synchronize jobs & trained models

After the model is completed, you will need to synchronize it with your cluster; click the `Synchronize your jobs and trained models.` link:
![Apr-07-2026_at_09.33.46-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/11fe0d207f482607ece27fd77cfa5fb4/assets/Apr-07-2026_at_09.33.46-image.png)

Press the `Synchronize` button then `Close`:
![Apr-07-2026_at_09.36.07-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/0c3e6661f5a1df15efc9d2f6073978e1/assets/Apr-07-2026_at_09.36.07-image.png)

Once the job reaches 100% and status shows **Stopped (completed)**, click
**Explore results**.

![Apr-06-2026_at_17.48.42-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/17ccc521c4e09795e7b4737f13423121/assets/Apr-06-2026_at_17.48.42-image.png)

### Confusion matrix

The confusion matrix shows how well the model classified the 20% test set. Look for:

- HIGH recall: what percentage of actual HIGH events were correctly predicted as HIGH?
  Because you used `maximize_minimum_recall`, this should be the strongest class.
- LOW precision: how many LOW predictions were actually LOW?
- Any systematic misclassification between MEDIUM and HIGH?

![Apr-06-2026_at_17.53.06-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/fff7f39dfebd0da89c4bb3fb2385ece4/assets/Apr-06-2026_at_17.53.06-image.png)

### Feature importance

Click any document in the results table. The **Feature importance** panel shows which
fields drove that specific prediction. For a HIGH-risk document you should see
`ping_one.audit.risk.score` or `oracle.database_audit.privilege` near the top.

![Apr-06-2026_at_17.53.43-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/fecd7248a9b3e06b5406f2f6cbb53186/assets/Apr-06-2026_at_17.53.43-image.png)

**Discussion:** Feature importance answers *why* the model made a prediction — not just
*what* it predicted. This is the difference between a black-box score and an auditable
finding. In a mortgage compliance context, being able to show an auditor *why* a
transaction was flagged is as important as flagging it correctly.

### Read the final hyperparameters after training

Navigate to DevTools:
![Apr-07-2026_at_09.37.18-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/6bbf9dc00526481f01bee7535ce6bc14/assets/Apr-07-2026_at_09.37.18-image.png)

Once training completes, run this in Dev Tools:

<pre><code>GET /_ml/data_frame/analytics/mortgage-privileged-access-classification/_stats</code></pre>

Find `analysis_stats.classification_stats.hyperparameters` in the response.
It will look similar to this — your auto-tuned values will differ from run to run:

```json
"hyperparameters": {
  "class_assignment_objective": "maximize_minimum_recall",
  "alpha":                 0.84,
  "downsample_factor":     0.91,
  "eta":                   0.1,
  "eta_growth_rate":       1.02,
  "feature_bag_fraction":  0.5,
  "gamma":                 0.1,
  "lambda":                0.1,
  "max_optimization_rounds_per_hyperparameter": 2,
  "max_trees":             894,
  "num_folds":             5,
  "num_splits_per_feature": 75,
  "soft_tree_depth_limit": 3.05,
  "soft_tree_depth_tolerance": 0.13
}
```

Your four pre-set values appear exactly as you entered them. Everything else was
determined by the Bayesian optimizer. Here is what each auto-tuned value means and
what it tells you about how this particular model was built:

**`alpha: 0.84`** — This is an L2 penalty on the total count of leaf nodes across the
entire forest, distinct from lambda which penalizes individual leaf *weights*. Alpha
controls how many leaves the forest is allowed to grow in aggregate. A value near 1
means the optimizer found that moderate tree density produced the best validation
score — not too sparse, not too bushy. If alpha had come back very high (10+), that
would indicate the optimizer was aggressively pruning the forest and the model may be
underfitting.

**`downsample_factor: 0.91`** — In each boosting round, only 91% of the training
documents are used to compute the gradient. This stochastic subsampling adds
controlled noise that acts as another regularizer — it prevents the model from
perfectly memorizing any individual training example. A value close to 1.0 (as
selected here) indicates the dataset is large enough that full-sample gradients are
nearly as good as subsampled ones. A much lower value (0.5) would suggest high
variance in the training data that benefits from more aggressive subsampling.

**`eta_growth_rate: 1.02`** — The learning rate is not actually constant throughout
training. It starts at your specified `eta: 0.1` and increases by 2% for each new
tree added. This adaptive schedule means early trees make careful small corrections
while later trees, once the model is already well-calibrated, can afford to take
larger steps. The growth rate of 1.02 is intentionally conservative — with 894 trees
and a 2% growth rate, the effective learning rate by the final tree is approximately
`0.1 × 1.02^894 ≈ 7,800`, which sounds large but is tempered by the diminishing
residuals available to correct at that stage.

**`max_trees: 894`** — The optimizer found that 894 trees produced the best
validation score before early stopping fired. This is a meaningful number to
understand: it is not the maximum allowed trees, it is the number the model actually
used. Check the `iteration` field in the same stats output — if it matches
`max_trees`, the model used all allocated trees and more might improve results. If
`iteration` is significantly lower, early stopping kicked in before the limit, which
is the healthy case and indicates the model converged naturally.

**`soft_tree_depth_limit: 3.05` and `soft_tree_depth_tolerance: 0.13`** — Rather
than a hard maximum depth, DFA uses a soft depth limit with a tolerance band. Trees
are allowed to grow beyond `soft_tree_depth_limit` but pay an increasing penalty for
each additional level. The tolerance of 0.13 determines how quickly that penalty
escalates. A depth limit near 3 means the forest primarily uses shallow trees (3–4
levels), which aligns well with this dataset: the underlying risk patterns are
relatively simple combinations of a small number of fields, and deep trees would be
overcomplicating the problem.

**Discussion for the audience:** Notice that the optimizer agreed with the
regularization intent behind your manual settings. It chose a moderate `alpha`,
a near-full `downsample_factor`, and shallow trees — all consistent with a
well-regularized model that generalizes rather than memorizes. When the auto-tuned
values reinforce rather than fight your manual choices, that is a signal the manual
values were sensible for the data. If the optimizer had returned `alpha: 0.01` (very
bushy trees) alongside your `lambda: 0.1` (conservative leaf weights), that tension
would be worth investigating — it would suggest the two regularizers are working
against each other and the problem may benefit from a different tuning approach.

---

## 1.6 — Confirm the Trained Model
===

Retrieve the Trained Model via Dev Tools:

**Note the exact model ID** — you need it in the next step.
<pre><code>GET /_ml/trained_models?tags=mortgage-privileged-access-classification&human=true</code></pre>

![Apr-07-2026_at_13.24.55-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/3319cd2856ccd0be65c9b81aba8bc1f1/assets/Apr-07-2026_at_13.24.55-image.png)

Confirm `model_type: tree_ensemble` is present — this is a deployable model.

---


## 1.7 — Deploy as an Ingest Pipeline
===

Now deploy the trained model so every new event is scored on ingest.

Using Dev Tools — replace `<timestamp>` with the actual model ID you noted above:

<pre><code>PUT /_ingest/pipeline/mortgage-privileged-access-ml
{
  "description": "Score PingOne and Oracle audit events for privileged access risk",
  "processors": [
    {
      "inference": {
        "model_id": "mortgage-privileged-access-classification-{{timestamp}}",
        "target_field": "ml",
        "field_mappings": {},
        "inference_config": {
          "classification": {
            "num_top_classes": 3,
            "results_field": "risk_level_prediction",
            "top_classes_results_field": "top_classes"
          }
        },
        "on_failure": [
          {
            "set": {
              "field": "ml.inference_error",
              "value": "{{ _ingest.on_failure_message }}"
            }
          }
        ]
      }
    }
  ]
}</code></pre>

![Apr-07-2026_at_13.28.26-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/7323cd4c6b9daafdb3ecf85094359963/assets/Apr-07-2026_at_13.28.26-image.png)

### Test the pipeline before wiring it

Simulate against a PingOne document:

<pre><code>POST /_ingest/pipeline/mortgage-privileged-access-ml/_simulate
{
  "docs": [
    {
      "_source": {
        "ping_one.audit.risk.score": 91.5,
        "ping_one.audit.action.type": "AUTHENTICATION",
        "ping_one.audit.result.status": "FAILED",
        "event.outcome": "failure"
      }
    }
  ]
}</code></pre>

![Apr-07-2026_at_13.30.01-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/1962444fa364d35db8b74bc2571062ab/assets/Apr-07-2026_at_13.30.01-image.png)

Simulate against an Oracle document:

<pre><code>POST /_ingest/pipeline/mortgage-privileged-access-ml/_simulate
{
  "docs": [
    {
      "_source": {
        "oracle.database_audit.action": "DROP TABLE",
        "oracle.database_audit.privilege": "SYSDBA",
        "event.outcome": "success"
      }
    }
  ]
}</code></pre>

![Apr-07-2026_at_13.30.31-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/0a87cf65a99238803f5ef0eb90d0b542/assets/Apr-07-2026_at_13.30.31-image.png)

Both should return `ml.risk_level_prediction` as `LOW`, `MEDIUM`, or `HIGH` with a
`ml.top_classes` array showing probabilities for all three classes. If you see
`ml.inference_error` instead, check the model ID.

---

## 1.8 — Wire the Pipeline to Both Indices
===

Apply the pipeline at the data stream level using `_settings` on the data stream
name directly. This propagates to the current backing index and any future rollovers
without needing to touch individual `.ds-*` index names:

<pre><code>PUT /logs-ping_one.audit-mortgage/_settings
{
  "index": { "default_pipeline": "mortgage-privileged-access-ml" }
}

PUT /logs-oracle.database_audit-mortgage/_settings
{
  "index": { "default_pipeline": "mortgage-privileged-access-ml" }
}</code></pre>

![Apr-07-2026_at_13.31.40-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/149e48b5ab66da99b7239b6aac11b6ab/assets/Apr-07-2026_at_13.31.40-image.png)

Verify the setting was applied:

<pre><code>GET /logs-ping_one.audit-mortgage/_settings/index.default_pipeline
GET /logs-oracle.database_audit-mortgage/_settings/index.default_pipeline</code></pre>

Both should return `"mortgage-privileged-access-ml"`.

![Apr-07-2026_at_13.32.38-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/4e91e9d658ce997433cd915b0f15f687/assets/Apr-07-2026_at_13.32.38-image.png)

---

### OPTIONAL:  View Ingest Pipeline via UI

Search for `ingest pipelines` in the top search bar and select the top result:

<pre><code>ingest pipelines</code></pre>

![Apr-08-2026_at_10.32.20-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/fdb778ea009705d668434d20faa283e9/assets/Apr-08-2026_at_10.32.20-image.png)

Search for your recently created ingest pipeline:

<pre><code>mortgage-privileged-access-ml</code></pre>

![Apr-08-2026_at_10.34.17-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/f25c12275dd0f87ff4309cba9ee92b50/assets/Apr-08-2026_at_10.34.17-image.png)

Click on the `Edit pipeline` button:

![Apr-08-2026_at_10.35.00-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/9753156ae3ea64e1613e0f7806547ba6/assets/Apr-08-2026_at_10.35.00-image.png)

Click on `Inference` to observe what this processor looks like in the UI:
![Apr-08-2026_at_10.35.50-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/62aad06cc24750c21090d6af2e6ede35/assets/Apr-08-2026_at_10.35.50-image.png)

![Apr-08-2026_at_10.36.28-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/28735a273731f3d70bed93aa3dfea488/assets/Apr-08-2026_at_10.36.28-image.png)

## 1.9 — Observe Predictions in Discover
===

> [!IMPORTANT]
>Re-initiate data generation first, click this button hop over to the `host-1` cli:
>
>[button label="host1-cli"](tab-1)


Run the following command:

> [!IMPORTANT]
>
>``` run
>bash  /workspace/workshop/elastic-ml-lifecycle-automation/Scripts/livedata.sh
>```

You are ready to move on when your screen resembles this:
![Apr-07-2026_at_13.39.49-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/405dac0271971de6e61b73a872d878ad/assets/Apr-07-2026_at_13.39.49-image.png)

> [!IMPORTANT]
>Go back to Kibana, click this button hop back:
>
>[button label="Kibana"](tab-0)

Navigate to **Kibana → Discover** by searching for `Discover` in your top search bar, select the top result.
![Apr-07-2026_at_13.42.48-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/6b5217223f04357d38376e440598b102/assets/Apr-07-2026_at_13.42.48-image.png)

Select the `logs-ping_one.audit-mortgage` data view.
Set the time range to the last 15 minutes to see only new documents that arrived after
the pipeline was applied.

![Apr-07-2026_at_13.43.24-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/2e91563e4e64a6ba100b53c13d608612/assets/Apr-07-2026_at_13.43.24-image.png)

Add these columns to the table:

| Column | What it shows |
|---|---|
| `@timestamp` | When the event arrived |
| `event.outcome` | success / failure |
| `ping_one.audit.risk.score` | Raw IAM risk score |
| `ping_one.audit.action.type` | What IAM action occurred |
| `ml.risk_level_prediction` | Model prediction: LOW / MEDIUM / HIGH |
| `ml.top_classes` | All three class probabilities |

![Apr-07-2026_at_14.41.05-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/bd2375ecc1b9dc1d7f9c4f0a8ff9aaca/assets/Apr-07-2026_at_14.41.05-image.png)

Every new document should have `ml.risk_level_prediction` populated.

**Filter to HIGH predictions only:**

![Apr-07-2026_at_14.41.37-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/245321699ba72aec0f478dbd64f32ec9/assets/Apr-07-2026_at_14.41.37-image.png)

```
ml.risk_level_prediction : "HIGH"
```

**Questions about our data:**

- Why do documents with `ping_one.audit.risk.score` above 70 and `event.outcome: failure`
  consistently predict HIGH?
- What does a HIGH prediction with 0.95 probability tell you versus 0.55 probability?


<details>
<summary><ins>Answers</ins></summary>

## Why do documents with `ping_one.audit.risk.score` above 70 and `event.outcome: failure` consistently predict HIGH?

Because those two features together represent exactly the pattern the model was trained to associate with HIGH-risk labels. During training, documents in the HIGH class were disproportionately clustered at elevated risk scores and failure outcomes; those correlations were not random, they were embedded by the data generator to simulate real attacker behaviour. The gradient boosted tree algorithm found that risk.score > 70 was one of the strongest splits early in the tree ensemble because it cleanly separates HIGH from LOW/MEDIUM across a large fraction of the training set. event.outcome: failure reinforces that split ~ a high-risk score on a successful authentication is ambiguous, but a high-risk score on a failed authentication is the signature of a brute-force or credential stuffing attempt. The two features in combination create a decision path through the tree ensemble that almost always terminates at a HIGH leaf node, because that path matched HIGH-labeled documents repeatedly during training. Feature importance in the results view will confirm this, those two fields will appear near the top, which means they had the most influence on reducing prediction error across the most training examples.

## What does a HIGH prediction with 0.95 probability tell you versus 0.55 probability?

Probability is the model's confidence in its own classification, not just the class label. At 0.95 the model is telling you the document sits deep within the HIGH region of the feature space; it matched the HIGH pattern decisively across many trees in the ensemble, and the alternative classes (LOW, MEDIUM) were not competitive. You can act on this with high confidence and low noise. At 0.55 the model is telling you the document is technically above the threshold for HIGH but only marginally; it's near the decision boundary where the feature combination is genuinely ambiguous and the ensemble was split on what to predict. That event might warrant investigation but also might resolve as a false positive.
The practical consequence for a SOC is that these two predictions should be treated differently. An alert that fires on every ml.risk_level_prediction: HIGH regardless of probability will include a lot of 0.55-0.65 predictions that drain analyst time. A well-calibrated alert filters on something like ml.risk_level_prediction: HIGH AND ml.top_classes.HIGH.probability >= 0.85, which targets the confident end of the distribution and dramatically reduces false positive volume. The ml.top_classes field written by the inference pipeline contains the probability for each class per document, so this filter is directly queryable in KQL and ES|QL.
</details>

---

You are now ready to move onto the next exercise by pressing `Next`:
![Apr-06-2026_at_16.00.27-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/26c9ffa607eb09146d57fea140c0f823/assets/Apr-06-2026_at_16.00.27-image.png)
`(Reference Image)`
