# Chapter 2 — AI Agent Analysis and Workflow Automation

**Objective:** Use Elastic Agent Builder and built-in platform tools to discover and analyze the `logs-mortgage.audit-default` dataset autonomously, then use a parameterised Elastic Workflow — triggered directly by the agent — to automatically create the `mortgage-audit-classification` job.

---

## 2.1 — Enable Workflows
===

Enable the feature flag, `Save changes`, then refresh Kibana.
![Apr-07-2026_at_15.56.43-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/3f0cedbf967384b6e3fb324c2472395c/assets/Apr-07-2026_at_15.56.43-image.png)

If you noticed the `Reload page` button which appears briefly, click this:
![Apr-07-2026_at_15.58.01-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/cd767d093f2e4cd90f26afdf3f65464b/assets/Apr-07-2026_at_15.58.01-image.png)

If you miss the `Reload page` button which appeared briefly, click this:
![Apr-07-2026_at_15.59.01-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/d03ff3c6000b38bd85c71e82e6c152f4/assets/Apr-07-2026_at_15.59.01-image.png)

You should now see **Workflows** in the left navigation under the `Management` section.

![Apr-07-2026_at_15.32.25-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/27c87bc3b2f6dec6b8416f96e89590af/assets/Apr-07-2026_at_15.32.25-image.png)

---


## 2.2 — Build the AI Agent
===

Navigate to **Elasticsearch → Agents** (from the side navigation).

![Apr-07-2026_at_15.34.26-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/c453a8991ca12f6f68b730aa745efaa7/assets/Apr-07-2026_at_15.34.26-image.png)

### No custom tools required

Elastic Agent Builder ships with a set of built-in platform tools that already
cover everything needed for this exercise. You do not need to create any custom
tools — the agent will use the following built-ins directly:

| Tool | What it does for this exercise |
|---|---|
| `platform.core.index_explorer` | Finds the right index without being told the exact name |
| `platform.core.list_indices` | Lists data streams and indices to orient itself |
| `platform.core.get_index_mapping` | Retrieves field mappings — replaces the manual `GET /_mapping` step |
| `platform.core.generate_esql` | Writes ES|QL queries from natural language descriptions |
| `platform.core.execute_esql` | Runs ES|QL queries and returns results in tabular form |
| `platform.core.search` | Exploratory fallback for any ad-hoc document lookup |
| `platform.core.product_documentation` | Looks up DFA configuration parameters without leaving the chat |
| `platform.core.get_workflow_execution_status` | Retrieve the status of a workflow execution |

The combination of `generate_esql` → `execute_esql` is particularly powerful
here: the agent describes what it wants to measure in plain language, generates
the correct ES|QL, executes it, and reasons over the results — all without the
student writing a single query.

> *The built-in tools contain no hardcoded queries at all —
> the agent writes them on the fly based on what it discovers in the mapping.
> The same agent, with the same system prompt, works against any Elasticsearch
> index in your cluster.*

### Create the agent

Navigate to **Elastic AI Agent → + New**.

![Apr-07-2026_at_16.02.13-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/e6a53554b3b2603814fa0538fee0ed1c/assets/Apr-07-2026_at_16.02.13-image.png)

### Provide the following:

- **Agent ID:** <pre><code>ml_readiness_analyst</code></pre>
- **Custom Instructions:**

<pre><code>You are an expert ML analyst who evaluates Elasticsearch data to determine
whether it is suitable for training a Data Frame Analytics classification model.

You have access to built-in Elastic tools that let you discover indices, retrieve
field mappings, generate and execute ES|QL queries, and look up product
documentation. You do not know in advance what fields exist in any index — you
discover them by using these tools.

IMPORTANT — ALWAYS START WITH THIS STEP NO MATTER WHAT:
Your very first action on every request must be to call list_indices with no
pattern filter. This gives you the complete picture of what exists in the cluster
before you make any assumptions. Do not use index_explorer or guess an index name
first. Do not ask the user any questions before calling list_indices. Just call it.

Your methodology for any dataset:

Step 1 — List all indices (MANDATORY FIRST ACTION)
Call list_indices immediately with no pattern argument. Review the full list and
identify all indices and data streams that are plausible matches for the user's
description based on their names. If the list is long, filter it mentally by the
domain keywords the user provided (e.g. "network", "traffic", "flow", "audit",
"logs"). Select the most specific match — prefer data streams over aliases, and
prefer indices with recent data over empty ones.

Once you have identified one or more candidate indices, call get_index_mapping on
each to retrieve the full field list. Reason over the mapping to classify fields:
  - Boolean fields: strong candidates for classification labels
  - Low-cardinality keyword fields (2-10 distinct values): candidates for labels
    or categorical features
  - Numeric fields: candidates for continuous features or risk scores
  - High-cardinality keyword fields (10+ values): categorical features
  - Timestamp, ID, session, and free-text fields: exclude from ML analysis

If multiple indices look relevant, pick the one with the richest mapping and the
most document coverage. State your selection and briefly explain why.

Step 2 — Confirm document count and time range
Call generate_esql to write a COUNT(*) query for the selected index over the last
30 days, then call execute_esql to run it. If fewer than 5,000 documents exist,
report this clearly — the dataset may be insufficient for reliable training.

Step 3 — Evaluate candidate label fields
For each boolean or low-cardinality keyword field from Step 1, call generate_esql
to write a STATS COUNT(*) BY &lt;field&gt; query, then execute_esql to run it. Check:
  - Binary labels: ideally 10-40% positive class. Below 5% requires
    maximize_minimum_recall as the class assignment objective.
  - Multi-class labels: flag any class with fewer than 200 examples.

Step 4 — Evaluate candidate feature fields
For each numeric field, call generate_esql to write a STATS MIN, MAX, AVG, COUNT
query, then execute_esql to run it. A field where MIN == MAX has zero predictive
value and must be excluded. More than 30% nulls means exclude unless null is
meaningful. For categorical features, call generate_esql + execute_esql to check
cardinality.

Step 5 — Recommend model configuration
Provide a complete recommendation:
  - The dependent_variable field and why
  - The analyzed_fields includes list with reasoning per field
  - The analyzed_fields excludes list — always exclude @timestamp, IDs, session
    fields, and free-text fields
  - Whether to use maximize_minimum_recall or maximize_accuracy
  - A model_memory_limit appropriate for the dataset size
  - Any data quality concerns to resolve before training

Use product_documentation if you need to verify DFA configuration parameters.

Step 6 — Offer to trigger the Workflow
After presenting your complete recommendation, ask the user:
  "I have completed my analysis. Would you like me to create the DFA
   classification job automatically using the Workflow?"

If the user confirms, call the trigger_dfa_classification_workflow tool with
the parameters you derived from your analysis. Show the parameter values to
the user before triggering so they can review and correct them if needed.
Do not call the tool unless the user has explicitly confirmed.

Always base every recommendation on what the tools actually return.
Never assume field names, values, or distributions without querying first.
This methodology works for any domain — security logs, network traffic, financial
transactions, healthcare records, or any structured time-series data.</code></pre>

- **Display Name:**  <pre><code>ML Readiness Analyst</code></pre>
- **Display Description:** <pre><code>Explores any Elasticsearch index to assess whether the data is suitable for ML classification training — discovers fields, analyses distributions, evaluates class balance, and recommends model configuration</code></pre>

Click on `Tools` to configure which features your agent will have access to:
![Apr-07-2026_at_16.08.20-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/026637094569cbd757ad3eaac6e712e9/assets/Apr-07-2026_at_16.08.20-image.png)


### Enable the following built-in tools:
  - `platform.core.get_workflow_execution_status`
  - `platform.core.index_explorer`
  - `platform.core.list_indices`
  - `platform.core.get_index_mapping`
  - `platform.core.generate_esql`
  - `platform.core.execute_esql`
  - `platform.core.search`
  - `platform.core.product_documentation`
  - `platform.core.get_workflow_execution_status`

### Save the Tools configuration:

![Apr-08-2026_at_10.42.17-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/2c76a28f7ad024c7e497a0e9a862bdbb/assets/Apr-08-2026_at_10.42.17-image.png)
---


## 2.3 — Chat with the Agent
===

![Apr-07-2026_at_16.12.47-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/7eaa59d07ee9d4a84ef5f26f5805b048/assets/Apr-07-2026_at_16.12.47-image.png)


The agent has access to built-in tools that let it discover indices, retrieve mappings,
and generate and execute ES|QL queries entirely on its own. Open the agent chat and
give it only a natural language description of what you want — no index names, no
field names, no Dev Tools steps required from the student.

**Opening prompt:**
<pre><code>I want to train an Elastic Data Frame Analytics classification model to detect
suspicious access events in our mortgage platform audit logs. I have not told you
which index to use or what fields it contains. Please find the right data, discover
the schema, assess whether it is suitable for classification training, and recommend
a complete model configuration.</code></pre>

Watch the sequence of tool calls the agent makes without any further prompting.

![Apr-07-2026_at_16.14.22-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/3f2b39ef12888fbb5799c3a58346a614/assets/Apr-07-2026_at_16.14.22-image.png)

**What the agent does — step by step**

The first action is always `list_indices` with no filter — this is explicitly required
by the system prompt as the mandatory first step. The agent gets the full index list,
scans it for names related to the user's description, and selects
`logs-mortgage.audit-default` as the best match. It then calls `get_index_mapping`
on that index to retrieve the full field list. It now has the complete schema without
the student doing anything.

> **Why `list_indices` first matters for real-world clusters**
>
> Without this constraint, `index_explorer` takes a natural language query and
> performs a semantic search against index names. On a familiar cluster with obvious
> index names this works well. On an unfamiliar cluster — or one where index names
> are opaque (e.g. `filebeat-8.12.0-2024.01.15`, `ds-netflow-prod-000047`) —
> `index_explorer` can time out or return too many low-confidence results, causing
> the agent to stall before it produces any output.
>
> `list_indices` always returns immediately with the complete list, giving the agent
> concrete candidates to reason over regardless of how the indices are named. The
> agent then applies its own judgment to select the right one rather than depending
> on a semantic search that may not resolve cleanly.

It then calls `generate_esql` to write a document count query, followed by
`execute_esql` to run it and confirm sufficient data exists. It identifies
`audit.is_suspicious` as a boolean field and generates a query to check its class
balance. It identifies `audit.risk_score` as a numeric field and generates a
distribution query. It works through the keyword fields — `user.roles`,
`event.action`, `source.geo.country_iso_code` — generating cardinality queries
for each.

If it needs to look up valid values for `class_assignment_objective` or confirm
how `analyzed_fields.excludes` works in the API, it calls `product_documentation`.

**The "AH-HA!!" moment**

> *The agent made zero assumptions about the data. It didn't know the index was
> called "logs-mortgage.audit-default" or that the label field was called
> "audit.is_suspicious" until it looked. It found the index by semantic search,
> retrieved the mapping, and wrote every query itself based on what the mapping
> contained. Point this at any index in your cluster — a fraud dataset, a
> healthcare dataset, a network flow dataset — and the same agent follows the
> same methodology and produces a configuration appropriate for that data.*

**Follow-up prompts**

<pre><code>Which fields have the strongest predictive signal? Are there fields I should definitely exclude?</code></pre>

The agent reasons over the distribution results it already collected — a field
where MIN == MAX has no predictive value; a field with many nulls should be
excluded; a field whose distribution differs significantly between the suspicious
and non-suspicious class is a strong feature.

<pre><code>Based on everything you have found, write out the complete PUT request I should run in Dev Tools to create the mortgage-audit-classification job.</code></pre>

This produces a ready-to-run `PUT /_ml/data_frame/analytics/...` block the student
can paste directly into Dev Tools. The agent's configuration should closely match
what the Workflow will create automatically in section 2.4 — the point being that
the agent just did in a conversation what a data scientist would spend hours doing
manually, and what the Workflow will automate entirely.

![Apr-08-2026_at_10.47.24-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/3cb675cf5fd063bb0261cc6e4972f861/assets/Apr-08-2026_at_10.47.24-image.png)

**Expected agent findings for this dataset:**
- `audit.is_suspicious` — boolean label, class balance ~15-25% true, sufficient
  for training, recommend `maximize_minimum_recall`
- `audit.risk_score` — numeric, range 0–100, good variance, strong feature
- `audit.off_hours`, `audit.mfa_used`, `audit.new_device`, `audit.vpn_detected`
  — booleans with meaningful splits, include as features
- `user.roles`, `event.action`, `source.geo.country_iso_code` — keyword features
  with useful cardinality
- `user.id`, `user.name`, `audit.session_id`, `@timestamp` — recommended for
  exclusion: identity fields and timestamps memorise rather than generalise

---


## 2.4 — Build the Automation Workflow
===


<br>

### Step 1 — Create the Workflow

The Workflow receives all job parameters from the agent as a structured JSON payload.
Nothing inside it is hardcoded — the same Workflow runs for the LendPath audit dataset,
a network traffic dataset, a fraud dataset, or any other classification problem.

Navigate to **Management → Workflows → Create a new workflow** and paste the following YAML:

![Apr-08-2026_at_10.49.30-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/be01c20a5582edf5f59bfb9639feaade/assets/Apr-08-2026_at_10.49.30-image.png)

![Apr-08-2026_at_10.49.57-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/98c501eaeaffc8b41aa166d396993bc9/assets/Apr-08-2026_at_10.49.57-image.png)

```yaml
name: DFA Classification — Auto-Setup
description: >
  Generic Data Frame Analytics classification job creator. Accepts all job
  parameters as inputs from the ML Readiness Analyst agent. Checks data
  readiness, validates class balance, creates the DFA job, and logs an audit
  trail. Does NOT start the job — that remains a manual step.
enabled: true
tags: ["ml", "classification", "dfa", "automation"]

# ── Inputs ────────────────────────────────────────────────────────────────────
# All job parameters are declared here so this workflow is fully reusable.
# Nothing is hardcoded — the same workflow creates any classification job
# for any dataset. Values are passed in by the ML Readiness Analyst agent
# after it has analysed the index and produced its recommendation.
# Reference each input throughout the steps as {{ inputs.<name> }}.
inputs:
  # The source index or data stream the DFA job will read from.
  - name: sourceIndex
    type: string
    description: "Source index or data stream pattern"

  # The destination index where DFA writes enriched documents with predictions.
  - name: destIndex
    type: string
    description: "Destination index for DFA results"

  # A unique slug for the DFA job — used in the ML API path and UI.
  - name: jobId
    type: string
    description: "Unique identifier for the DFA job"

  # The field the model will learn to predict. Must be boolean or low-cardinality
  # keyword. Identified by the agent during schema analysis.
  - name: dependentVariable
    type: string
    description: "The field to predict — the classification label"

  # Used in the class balance ES|QL query (Step 2). Typically the same as
  # dependentVariable but declared separately for clarity.
  - name: labelField
    type: string
    description: "Same as dependentVariable — used for class balance check"

  # Controls how the model handles imbalanced classes. Use maximize_minimum_recall
  # when the positive class is rare (< 30%) to prevent the model ignoring it.
  # Use maximize_accuracy when classes are roughly balanced.
  - name: classAssignmentObjective
    type: string
    description: "maximize_minimum_recall or maximize_accuracy"
    default: "maximize_minimum_recall"

  # JVM heap allocation for the ML node during training. Increase for larger
  # datasets or more features. The agent recommends a value based on data size.
  - name: modelMemoryLimit
    type: string
    description: "Memory limit for the DFA job e.g. 100mb, 550mb"
    default: "150mb"

  # Percentage of documents reserved for training. The remainder forms the
  # held-out test set used to evaluate accuracy after training completes.
  - name: trainingPercent
    type: number
    description: "Percentage of documents used for training (0-100)"
    default: 80

# ── Constants ─────────────────────────────────────────────────────────────────
# Minimum document count required before the workflow will proceed.
# Below this threshold the training set is too small for reliable results.
consts:
  minDocs: 5000

# ── Trigger ───────────────────────────────────────────────────────────────────
# Manual trigger — the workflow runs on demand, either from the Kibana UI
# or when called by the ML Readiness Analyst agent via a Workflow tool.
# This is intentional: a human reviews the agent's recommendation before
# the automation proceeds.
triggers:
  - type: manual

steps:

  # ── Step 1a: Count total documents ─────────────────────────────────────────
  # Issues a size:0 search (no documents returned, just the hit count) against
  # the source index. This is the fastest way to get a document count without
  # fetching any data. The total.value from hits is used in the guard below.
  - name: count_total_docs
    type: elasticsearch.search
    with:
      index: "{{ inputs.sourceIndex }}"
      size: 0
      query:
        match_all: {}

  # ── Step 1b: Guard — abort if insufficient data ─────────────────────────────
  # Compares the document count against the minDocs threshold. If too few
  # documents exist the model will be unreliable — training on a small set
  # produces high variance results that don't generalise. The workflow logs
  # a clear failure message and aborts rather than creating a bad job.
  # If the count passes, logs a confirmation and continues to Step 2.
  - name: check_sufficient_data
    type: if
    condition: "{{ steps.count_total_docs.output.hits.total.value }} < {{ consts.minDocs }}"
    steps:
      - name: insufficient_data
        type: console
        with:
          message: >
            ✗ Insufficient data: {{ steps.count_total_docs.output.hits.total.value }}
            documents found. Need at least {{ consts.minDocs }}. Aborting.
    else:
      - name: data_sufficient_log
        type: console
        with:
          message: >
            ✓ Data check passed: {{ steps.count_total_docs.output.hits.total.value }}
            documents in {{ inputs.sourceIndex }}

  # ── Step 2a: Check class balance ─────────────────────────────────────────────
  # Runs an ES|QL query that groups documents by the label field and counts
  # each class. This tells us whether the training data is balanced enough
  # for reliable classification. Severe imbalance (e.g. 99% negative, 1%
  # positive) requires the maximize_minimum_recall objective and may need
  # data augmentation. ES|QL is used here instead of a DSL aggregation because
  # the field name is dynamic ({{ inputs.labelField }}) and ES|QL templating
  # is more reliable than templating nested aggregation body objects.
  - name: count_label_distribution
    type: elasticsearch.esql.query
    with:
      format: json
      query: >
        FROM {{ inputs.sourceIndex }}
        | WHERE @timestamp > NOW() - 30 days
        | STATS doc_count = COUNT(*) BY {{ inputs.labelField }}
        | SORT doc_count DESC

  # ── Step 2b: Log class balance results ───────────────────────────────────────
  # Writes the bucket values to the execution log so the analyst can verify
  # class distribution before the job is created. The log is visible in the
  # Workflows execution history for audit purposes.
  - name: log_class_balance
    type: console
    with:
      message: >
        Class balance for {{ inputs.labelField }}:
        {{ steps.count_label_distribution.output.values }}

  # ── Step 3a: Check whether the DFA job already exists ────────────────────────
  # Issues a GET to the ML API for the requested job ID. If the job exists
  # the API returns 200; if it doesn't exist it returns 404. The on-failure
  # continue directive prevents a 404 from aborting the workflow — instead
  # the status code is captured and evaluated in the next step.
  - name: check_job_exists
    type: elasticsearch.request
    on-failure:
      continue: true
    with:
      method: GET
      path: "/_ml/data_frame/analytics/{{ inputs.jobId }}"

  # ── Step 3b: Branch — skip creation if job already exists ────────────────────
  # If the GET returned 200 the job already exists and creation is skipped
  # to avoid overwriting an existing trained model or running job.
  # If the GET returned anything other than 200 (typically 404) the job does
  # not exist and the workflow proceeds into the else branch to create it.
  - name: handle_existing_job
    type: if
    condition: "{{ steps.check_job_exists.output.status }} == 200"
    steps:
      - name: job_already_exists
        type: console
        with:
          message: >
            ⚠ Job {{ inputs.jobId }} already exists. Skipping creation.
            Delete the existing job first if you want to recreate it.
    else:
      - name: creating_job_log
        type: console
        with:
          message: "→ Creating DFA job: {{ inputs.jobId }}"

      # ── Step 4: Create the DFA classification job ─────────────────────────────
      # Issues a PUT to the ML Data Frame Analytics API with the full job
      # configuration assembled from the input parameters. The job is created
      # in stopped state — it will not start training until manually triggered
      # in Kibana or via a separate _start API call.
      #
      # Note on analyzed_fields: includes and excludes are intentionally omitted
      # here. The Workflows engine cannot reliably template a dynamic JSON array
      # into a nested request body in the current version. After this step
      # completes, apply the agent's recommended field lists via Dev Tools:
      #
      #   POST /_ml/data_frame/analytics/{{ inputs.jobId }}/_update
      #   { "analyzed_fields": { "includes": [...], "excludes": [...] } }
      - name: create_dfa_job
        type: elasticsearch.request
        with:
          method: PUT
          path: "/_ml/data_frame/analytics/{{ inputs.jobId }}"
          body:
            # Human-readable description written to the job metadata.
            description: "Classification model for {{ inputs.dependentVariable }} in {{ inputs.sourceIndex }}. Created automatically by Elastic Workflows."
            source:
              # The index or data stream to read training data from.
              # Wrapped in a list because the DFA API accepts multiple source indices.
              index:
                - "{{ inputs.sourceIndex }}"
              query:
                match_all: {}
            dest:
              # DFA creates this index automatically if it does not exist.
              # Enriched documents (original fields + ml.* predictions) are written here.
              index: "{{ inputs.destIndex }}"
              results_field: "ml"
            analysis:
              classification:
                # The field the model learns to predict. Must exist in the source index
                # as boolean or low-cardinality keyword with no more than 30 distinct values.
                dependent_variable: "{{ inputs.dependentVariable }}"
                # Fraction of documents used for training. The remainder is held out
                # as a test set and used to compute accuracy metrics after training.
                training_percent: "{{ inputs.trainingPercent }}"
                # Number of class probability scores to include per prediction.
                # 2 covers binary labels (true/false, HIGH/LOW etc).
                num_top_classes: 2
                # Number of top features to include in the feature importance explanation
                # written alongside each prediction in the destination index.
                num_top_feature_importance_values: 5
                # How the model resolves ties when assigning a class. maximize_minimum_recall
                # ensures the rarest class is not ignored — critical for imbalanced datasets
                # where a naive model would always predict the majority class.
                class_assignment_objective: "{{ inputs.classAssignmentObjective }}"
            # Upper bound on JVM memory for the ML node during training.
            # Too low and the job fails to start; too high and it may starve other jobs.
            model_memory_limit: "{{ inputs.modelMemoryLimit }}"
            # If true the job queues and waits for an ML node with capacity.
            # False means the job fails immediately if no ML node is available.
            allow_lazy_start: false
            # Number of CPU threads the training algorithm may use.
            # 1 is conservative — increase for faster training on dedicated ML nodes.
            max_num_threads: 1

      # ── Step 4b: Log job creation confirmation ────────────────────────────────
      # Writes a structured confirmation to the execution log including all
      # key parameters. This forms part of the audit trail — in a compliance
      # context it shows exactly what was created, when, and with what config.
      - name: log_job_created
        type: console
        with:
          message: >
            ✓ DFA job created: {{ inputs.jobId }}
            Source:             {{ inputs.sourceIndex }}
            Destination:        {{ inputs.destIndex }}
            Dependent variable: {{ inputs.dependentVariable }}
            Objective:          {{ inputs.classAssignmentObjective }}
            Memory limit:       {{ inputs.modelMemoryLimit }}
            Training:           {{ inputs.trainingPercent }}%

      # ── Step 5: Final summary ─────────────────────────────────────────────────
      # Prints a formatted completion banner to the execution log with the job ID,
      # source index, label field, and total document count from Step 1.
      # Also reminds the analyst to apply analyzed_fields before starting the job.
      - name: final_summary
        type: console
        with:
          message: |
            ══════════════════════════════════════════════════════════
            ✓ DFA Classification Job Created
            ══════════════════════════════════════════════════════════
            Job ID:      {{ inputs.jobId }}
            Index:       {{ inputs.sourceIndex }}
            Label:       {{ inputs.dependentVariable }}
            Total docs:  {{ steps.count_total_docs.output.hits.total.value }}

            Next step: Add analyzed_fields via Dev Tools, then start:
            ML → Data Frame Analytics → {{ inputs.jobId }} → ▶
            ══════════════════════════════════════════════════════════
```

Click **Save**. Note the Workflow ID from the URL — you will need it in the next step.

![Apr-08-2026_at_10.51.38-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/03ccdd5a1ba7cfa16a069796e5a49d03/assets/Apr-08-2026_at_10.51.38-image.png)

---

### Step 2 — Register the Workflow as a tool in Agent Builder

This is the key integration step. Agent Builder supports a **Workflow tool** type that
lets the agent trigger a Workflow directly from the chat conversation, passing parameters
as a structured payload. No copy-pasting JSON — the agent calls the tool, the Workflow runs.

Navigate to **Agents → Tools → New Tool** by searching for `agent tools` in the top search and select the top result to configure a `+ New tool`:

![Apr-08-2026_at_10.54.00-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/2a97111a858e9cc2b9d2368925720586/assets/Apr-08-2026_at_10.54.00-image.png)

![Apr-08-2026_at_10.54.53-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/a877fc3273d8960387534a0cb174c00d/assets/Apr-08-2026_at_10.54.53-image.png)

- **Type:** Workflow
- **Workflow:** Select `DFA Classification — Auto-Setup` from the dropdown
- **Tool ID:** `trigger_dfa_classification_workflow`
- **Description:**
  ```
  Triggers the DFA Classification Auto-Setup Workflow to automatically create
  an Elastic Data Frame Analytics classification job. Call this tool once you
  have completed your analysis and the user has confirmed they want to proceed.
  Pass all parameters derived from your analysis — the workflow will validate
  data readiness, check class balance, and create the job.
  ```

![Apr-08-2026_at_10.57.21-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/6c9eb7cc894c75499daa9c78c6519d8f/assets/Apr-08-2026_at_10.57.21-image.png)

The Workflow tool type automatically maps the agent's output to the Workflow's
`params` block. The agent passes a JSON object and the Workflow receives it as
named parameters.

Once created, go back to the **ML Readiness Analyst** agent and add
`trigger_dfa_classification_workflow` to its tool list alongside the existing
platform tools.

![Apr-08-2026_at_10.59.24-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/cd72ca9779c4ffe60a0616c9453ef2a8/assets/Apr-08-2026_at_10.59.24-image.png)

![Apr-08-2026_at_11.01.43-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/e64fe6275ddda9a164421a2a402a6d2e/assets/Apr-08-2026_at_11.01.43-image.png)

![Apr-08-2026_at_11.03.41-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/bb3049fce230f217e4197420f47a6177/assets/Apr-08-2026_at_11.03.41-image.png)

---

### Step 3 — Update the agent system prompt

Add the following to the end of the ML Readiness Analyst system prompt to instruct
the agent when and how to call the new tool:

![Apr-08-2026_at_11.05.08-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/fc2c31a95c509a93bc091c6e0ddce43a/assets/Apr-08-2026_at_11.05.08-image.png)

```
Step 6 — Offer to trigger the Workflow
After presenting your complete recommendation, ask the user:
  "I have completed my analysis. Would you like me to create the DFA
   classification job automatically using the Workflow?"

If the user confirms, call the trigger_dfa_classification_workflow tool
with the following parameters derived from your analysis:

  sourceIndex:              the index you identified
  destIndex:                <jobId>-results
  jobId:                    a short slug e.g. <domain>-classification
  dependentVariable:        the label field you identified
  labelField:               same as dependentVariable
  classAssignmentObjective: maximize_minimum_recall or maximize_accuracy
  modelMemoryLimit:         your recommended value e.g. 100mb
  trainingPercent:          80
  includes:                 the list of feature fields you recommended
  excludes:                 @timestamp and all ID, session, free-text fields

Do not call this tool unless the user has explicitly confirmed. Always show
your parameter values to the user before triggering so they can review them.
```

![Apr-08-2026_at_11.07.06-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/8f6e57003f6ac9255d8ae84ef51811f0/assets/Apr-08-2026_at_11.07.06-image.png)

---

### How it works end to end

Once all these steps are completed, chat with the agent:

![Apr-08-2026_at_11.08.34-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/d603d43920b5a0b4510f7acf2b8c1cc8/assets/Apr-08-2026_at_11.08.34-image.png)

**Ask again:**
<pre><code>I want to train an Elastic Data Frame Analytics classification model to detect
suspicious access events in our mortgage platform audit logs. I have not told you
which index to use or what fields it contains. Please find the right data, discover
the schema, assess whether it is suitable for classification training, and recommend
a complete model configuration.</code></pre>

![Apr-08-2026_at_11.09.50-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/5c04b02c402e022edba99c4c50aea655/assets/Apr-08-2026_at_11.09.50-image.png)

**Respond with a yes!**

![Apr-08-2026_at_11.12.11-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/851b18d72e9b50da44696030b8f57ca2/assets/Apr-08-2026_at_11.12.11-image.png)

The **ML Readiness Analyst** agent has now built your classification data frame analytics job:

![Apr-08-2026_at_11.13.25-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/1f7a4271b5873dc1883c9a2d69147dec/assets/Apr-08-2026_at_11.13.25-image.png)


## 2.5 — Review Workflow Execution
===

Search for `workflows` in your top search bar:
![Apr-08-2026_at_11.15.52-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/e117daf42f5ebdfe49acbb6361a997a1/assets/Apr-08-2026_at_11.15.52-image.png)

Select our `DFA Classification` workflow:
![Apr-08-2026_at_11.16.37-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/2677d92a9bca5f25d8a93444ec62cd4c/assets/Apr-08-2026_at_11.16.37-image.png)

Click on `Executions`:
![Apr-08-2026_at_11.17.09-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/fcf1cc0cc8ee402064eda1b4ba708651/assets/Apr-08-2026_at_11.17.09-image.png)

There should be one `Success`, click on it to view the steps ran:
![Apr-08-2026_at_11.17.48-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/b657e1be59be7390e5cffc7dea1de65e/assets/Apr-08-2026_at_11.17.48-image.png)

After the workflow runs, the execution log on the right side of the editor shows
each step with its result. Expand each step to see:

- **count_total_docs** — the exact document count that passed the data readiness check
- **check_job_exists** — whether an existing job was found (404 = no job, proceed to create)
- **create_dfa_job** — the PUT response confirming the job was created
- **final_summary** — the full summary with all counts

![Apr-08-2026_at_11.19.07-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/5ac13906b215b35c63295026d0d13698/assets/Apr-08-2026_at_11.19.07-image.png)

This execution log is the audit trail — it shows exactly what the automation did and
with what data, which is important for compliance in a mortgage context.

---



## 2.6 — Start the Job and Monitor Training
===

Navigate to **Machine Learning → Data Frame Analytics** by searching for ```data frame analytics``` in the top search bar:
![Apr-08-2026_at_11.19.53-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/9d872e617dd901c0f9afc1e3c477d189/assets/Apr-08-2026_at_11.19.53-image.png)

Find `mortgage-audit-classification` with status **Stopped**. Click **▶ Start**.

![Apr-08-2026_at_11.20.51-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/ca792e12e831b12e12fe141d9b6f6e11/assets/Apr-08-2026_at_11.20.51-image.png)

Monitor progress — the job goes through four phases:

![Apr-08-2026_at_11.21.41-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/5a370d114ff11895de0d81870cc2a284/assets/Apr-08-2026_at_11.21.41-image.png)

| Phase | What happens |
|---|---|
| `reindexing` | Documents copied from `logs-mortgage.audit-default` to `mortgage-audit-classification` |
| `loading_data` | Features extracted and prepared for the training algorithm |
| `analyzing` | Decision tree ensemble trained on 80% of documents |
| `writing_results` | Model written, predictions added to the remaining 20% |

Once complete navigate to **View** — the scatter plot shows the test set
predictions. Documents colored by `ml.is_suspicious_prediction` should cluster clearly,
with the trained model's decision boundary visible between the `true` and `false` groups.

![Apr-08-2026_at_11.23.04-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/17f531dc02d031ab12f08abdf8047185/assets/Apr-08-2026_at_11.23.04-image.png)

You will have to *`Create a data view`* in order to see the results:
![Apr-08-2026_at_11.24.38-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/881785775c0bd9b06235050a6d23d4a6/assets/Apr-08-2026_at_11.24.38-image.png)

> [!IMPORTANT]
>Click the button below to quickly navigate to the `Create a data view` step.
>
>If not and you click the hyperlink, a new tab will appear:
>
>[button label="Data View Creation"](tab-2)

Provide the following:

**NAME**  `mortgage-classification-results`

**Index pattern**  `mortgage-classification-results*`

**Timestamp field**  *keep @timestamp as the default*

**Custom data view ID**  `mortgage-classification-results`

Then press `Save data view to Kibana`:

![Apr-08-2026_at_11.26.23-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/a7ca5050252aeec37e3be2ce2b156f74/assets/Apr-08-2026_at_11.26.23-image.png)

> [!IMPORTANT]
>Click the button below to quickly navigate back to the details of the classification job:
>
>[button label="Kibana"](tab-0)

You can observe the details of the classification job:
![Apr-08-2026_at_11.29.06-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/4d1b8d79eb6d64ebd74964df1cf1d1e5/assets/Apr-08-2026_at_11.29.06-image.png)

---



## 2.7 — Deploy as Inference Pipeline
===


Once training is complete, get the model ID, navigate to DevTools:

![Apr-08-2026_at_11.29.46-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/87a1d8f35a3ff1e405d33d664238b16b/assets/Apr-08-2026_at_11.29.46-image.png)

```
GET /_ml/trained_models?tags=mortgage-classification&human=true
```

![Apr-08-2026_at_11.31.03-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/5e3b657ec7909e92c734147a4008fcb9/assets/Apr-08-2026_at_11.31.03-image.png)

Use the most recent timestamped model ID. Create the pipeline:

![Apr-08-2026_at_11.32.01-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/1c416a68ec95533825da3256e543eff3/assets/Apr-08-2026_at_11.32.01-image.png)

```
PUT /_ingest/pipeline/mortgage-audit-ml
{
  "description": "Score incoming audit events with the classification model",
  "processors": [
    {
      "inference": {
        "model_id": "mortgage-classification-<timestamp>",
        "target_field": "ml",
        "inference_config": {
          "classification": {
            "num_top_classes": 2,
            "results_field": "is_suspicious_prediction",
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
}
```

Apply the pipeline at the data stream level:

```
PUT /logs-mortgage.audit-default/_settings
{
  "index": { "default_pipeline": "mortgage-audit-ml" }
}
```

Verify it was applied:

```
GET /logs-mortgage.audit-default/_settings/index.default_pipeline
```

![Apr-08-2026_at_11.32.44-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/9fa6e51e5b55b013fb1da4b93441cde9/assets/Apr-08-2026_at_11.32.44-image.png)

## 2.8 — Ingest, Infer, and Observe
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

Once you observe:
![Apr-08-2026_at_11.40.55-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/1c20f6be98b38fbaa5970a8b6fb99f12/assets/Apr-08-2026_at_11.40.55-image.png)

> [!IMPORTANT]
>Go back to Kibana, click this button hop back:
>
>[button label="Kibana"](tab-0)

Navigate to **Discover***

![Apr-08-2026_at_11.33.19-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/a18c1154ce9b323546b5ec977dea9934/assets/Apr-08-2026_at_11.33.19-image.png)

 **Select logs-mortgage.audit-default** and add `ml.is_suspicious_prediction`
as a column. Every new audit event now has a real-time ML prediction attached to it.

![Apr-08-2026_at_11.36.07-image.png](https://play.instruqt.com/assets/tracks/1nedhyisyw5x/ed020016e27972a611efb0d317ede4c1/assets/Apr-08-2026_at_11.36.07-image.png)
---
