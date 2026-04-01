# DOMAIN_NOTES.md — Week 7: Data Contract Enforcer

## Question 1: Backward-Compatible vs. Breaking Schema Changes

A **backward-compatible** change allows existing consumers to continue operating without modification. A **breaking change** forces at least one downstream consumer to update its code or logic to avoid failure — often silent failure.

### Backward-Compatible Examples (from our systems)

1. **Adding a nullable field to Week 5 events** — Adding `"user_agent": null` to the `metadata` object in `events.jsonl` is safe. The Week 7 ValidationRunner reads `correlation_id` and `schema_version`; it ignores unknown keys. Existing consumers continue without error.

2. **Adding a new `event_type` value to Week 5** — Our event store already has `AgentContextLoaded`. Adding `AgentContextUnloaded` is additive. Consumers that switch on `event_type` hit their default/unknown branch, which is acceptable behaviour.

3. **Widening a numeric type in Week 3** — Our `extraction_ledger.jsonl` stores `processing_time_seconds` as a float. Changing it from `float32` to `float64` is a widening change. No consumer loses precision; the statistical checks still pass against the same range constraints.

### Breaking Examples (from our systems)

1. **Renaming `extraction_confidence` to `confidence_score` in Week 3** — Our `extraction_ledger.jsonl` uses `extraction_confidence: 0.85`. The Week 4 Cartographer and the Week 7 ValidationRunner both reference this field by name. A rename silently returns `null` for every record in any consumer that does not adapt.

2. **Changing `metadata.schema_version` from integer `1` to string `"1.0"` in Week 5** — Our DB stores `schema_version` as an integer. The canonical spec requires a string. Any consumer that does a numeric comparison (`schema_version >= 2`) will throw a type error when it receives `"1.0"`.

3. **Removing `rubric_id` from Week 2 verdicts** — Our `verdict.json` contains `rubric_id: "463a0767..."`. The Week 7 ContractGenerator uses this field to verify the rubric SHA-256. Dropping it means the integrity check cannot execute — the contract clause errors silently rather than failing loudly.

---

## Question 2: The Confidence Scale Change — Failure Trace and Contract Clause

### The Failure

Our Week 3 `extraction_ledger.jsonl` stores `extraction_confidence: 0.85` (float, range 0.0–1.0). Suppose a developer updates the extractor to output `extraction_confidence: 85` (integer, range 0–100) to make it "more human-readable".

**Step-by-step failure in the Week 4 Cartographer:**

1. The Cartographer reads `extraction_ledger.jsonl` and maps each document's `extraction_confidence` to node metadata in the lineage graph: `"confidence": 0.85`.
2. After the change, the Cartographer receives `85`. It stores `"confidence": 85` in the node metadata without error — the field is typed as `number` in the graph, so no type exception is raised.
3. The Cartographer's edge confidence threshold filter (`if confidence >= 0.7: include_edge`) now includes **every single edge** because `85 >= 0.7` is always true. The lineage graph becomes artificially complete — no edges are pruned.
4. Downstream, the Week 7 ViolationAttributor traverses this inflated graph and assigns blame to nodes that are not genuinely connected, producing false blast radius reports.
5. No exception is raised anywhere in the pipeline. The output is wrong and looks correct.

### The Contract Clause (Bitol YAML)

```yaml
# generated_contracts/week3_extractions.yaml (excerpt)
schema:
  extraction_confidence:
    type: number
    minimum: 0.0
    maximum: 1.0          # BREAKING CHANGE if changed to 0–100
    required: true
    description: >
      Confidence score for the extraction strategy that succeeded.
      MUST remain a float in the range 0.0–1.0. Changing to integer
      0–100 is a breaking change that corrupts Week 4 lineage edge
      filtering and Week 7 blast radius computation.
quality:
  type: SodaChecks
  specification:
    checks for extractions:
      - min(extraction_confidence) >= 0.0
      - max(extraction_confidence) <= 1.0
      - avg(extraction_confidence) between 0.01 and 0.99
```

The `avg` check between 0.01 and 0.99 is the statistical drift guard. If the scale changes to 0–100, the mean jumps from ~0.85 to ~85.0, which immediately fails the upper bound check — catching the violation even if the type check somehow passes.

---

## Question 3: Using the Week 4 Lineage Graph for Blame Chain Construction

Our Week 4 lineage graph (`lineage_graph.json`) is a directed graph with two node types: `dataset` nodes (e.g., `orders`, `customers`, `stg_customers`) and `transformation` nodes (e.g., `models/marts/customers.sql::sql::0`). Edges connect transformations to the datasets they consume and produce.

### Step-by-Step Blame Chain Logic

**Step 1 — Identify the failing schema element.**
The ValidationRunner reports a FAIL on `extraction_confidence` in `outputs/week3/extractions.jsonl`. The failing element is the column `extraction_confidence` in the dataset node corresponding to Week 3 output.

**Step 2 — Map the column to a lineage node.**
The ContractGenerator, during its lineage context injection step, stored which lineage node produces this column. For Week 3, the producing node is the transformation node whose `source_file` points to the extraction script (e.g., `src/week3/extractor.py`).

**Step 3 — Breadth-first traversal upstream.**
Starting from the failing dataset node, traverse edges in reverse (target → source). At each hop, collect the `source_file` from transformation nodes. Stop at:
- A node with no upstream edges (source boundary), or
- A node whose `source_file` is outside the repository root (external boundary).

For our graph, this traversal would proceed:
```
week3_extractions dataset
  ← week3_extractor transformation  [source_file: src/week3/extractor.py]
    ← (no further upstream in repo)
```

**Step 4 — Git blame on identified files.**
For each `source_file` collected, run:
```bash
git log --follow --since="14 days ago" \
  --format='%H|%an|%ae|%ai|%s' -- src/week3/extractor.py
```
This returns every commit that touched the file in the attribution window.

**Step 5 — Score candidates.**
Apply the confidence formula:
```
base = 1.0 − (days_since_commit × 0.1)
penalty = 0.2 × lineage_hops_from_failing_column
confidence = max(0.0, base − penalty)
```
A commit made 1 day ago at 0 hops scores `1.0 − 0.1 − 0.0 = 0.9`. A commit made 3 days ago at 1 hop scores `1.0 − 0.3 − 0.2 = 0.5`.

**Step 6 — Write the violation record.**
The top-ranked candidate (highest confidence score) is written to `violation_log/violations.jsonl` with the full blame chain, commit hash, author, and blast radius derived from the lineage graph's downstream nodes.

---

## Question 4: Data Contract for LangSmith `trace_record`

```yaml
kind: DataContract
apiVersion: v3.0.0
id: langsmith-trace-records
info:
  title: LangSmith Trace Records — AI Pipeline Observability
  version: 1.0.0
  owner: week7-enforcer
  description: >
    One record per LLM or chain invocation captured by LangSmith.
    Covers Week 2 (verdict generation) and Week 3 (document extraction)
    LLM calls. Used by AI Contract Extensions for drift and cost tracking.
servers:
  local:
    type: local
    path: outputs/traces/runs.jsonl
    format: jsonl
schema:
  id:
    type: string
    format: uuid
    required: true
    unique: true
    description: LangSmith run ID. Primary key.
  run_type:
    type: string
    required: true
    enum: [llm, chain, tool, retriever, embedding]
    description: The category of LangSmith run.
  start_time:
    type: string
    format: date-time
    required: true
  end_time:
    type: string
    format: date-time
    required: true
    description: Must be strictly greater than start_time.
  total_tokens:
    type: integer
    minimum: 0
    required: true
  prompt_tokens:
    type: integer
    minimum: 0
    required: true
  completion_tokens:
    type: integer
    minimum: 0
    required: true
  total_cost:
    type: number
    minimum: 0.0
    required: true
    description: Cost in USD. Must be >= 0.
  tags:
    type: array
    required: false
    description: Run tags for filtering by source system (e.g. week2, week3).
quality:
  type: SodaChecks
  specification:
    checks for traces:
      # Structural clause
      - missing_count(id) = 0
      - missing_count(run_type) = 0
      - invalid_values(run_type, ['llm','chain','tool','retriever','embedding']) = 0
      # Temporal clause: end_time > start_time enforced in ValidationRunner code
      - missing_count(end_time) = 0
      # Statistical clause
      - avg(total_tokens) between 100 and 20000
      - max(total_cost) <= 1.00
      - min(total_cost) >= 0.0
      # Token arithmetic clause
      # total_tokens = prompt_tokens + completion_tokens enforced per-record in runner
ai_extensions:
  embedding_drift:
    applies_to: inputs
    baseline_sample_size: 200
    drift_threshold: 0.15
    alert_on: FAIL
    description: >
      Detects if the semantic distribution of LLM inputs has shifted
      from baseline. A drift score > 0.15 indicates the model is being
      asked questions outside its calibrated distribution.
  output_schema_violation_rate:
    applies_to: outputs
    max_violation_rate: 0.05
    trend_alert: rising
    description: >
      Tracks the fraction of LLM outputs that fail their expected JSON
      schema. A rising trend signals prompt degradation or model
      behaviour change between Claude versions.
lineage:
  upstream:
    - id: week2-digital-courtroom
      description: Verdict generation LLM calls
    - id: week3-document-refinery
      description: Extraction LLM calls
  downstream:
    - id: week7-ai-contract-extensions
      description: Consumes traces for drift and violation rate metrics
```

---

## Question 5: Why Contracts Go Stale — and How This Architecture Prevents It

### The Most Common Failure Mode

The most common failure mode in production contract enforcement systems is **contract abandonment under delivery pressure**. The sequence is always the same: contracts are written once at system launch, they are not updated when schemas evolve, and within two sprints they describe a system that no longer exists. Enforcement then produces false positives (contracts flagging valid new fields as violations), teams learn to ignore the alerts, and the system provides no value at all.

The second failure mode is **statistical baseline staleness**. A contract written when the system processed 50 documents per day has a `row_count >= 1` check. Six months later the system processes 50,000 documents per day. The baseline is useless — it will never catch a data drop from 50,000 to 500, because 500 still passes `>= 1`.

### Why Contracts Go Stale

1. **Contracts live outside the code** — they are YAML files not enforced by the compiler. A developer changes `extraction_confidence` to `confidence_score` in Python, the tests pass, the PR merges, and nobody updates the contract file because there is no gate requiring it.
2. **No ownership model** — contracts written by one person are not maintained by the next. Our Week 3 contract has `owner: week3-team` but if that team rotates, ownership is nominal.
3. **Baselines are snapshots not processes** — a statistical baseline captured in January is wrong by March if the underlying data distribution has shifted for legitimate business reasons.

### How This Architecture Prevents It

1. **ContractGenerator runs on every CI push** — because the generator re-infers contracts from the actual JSONL outputs on every run, the structural clauses stay current automatically. A renamed field is detected on the next generation run, not six months later.

2. **Schema snapshots with diffs** — every ContractGenerator run writes a timestamped snapshot to `schema_snapshots/`. The SchemaEvolutionAnalyzer diffs consecutive snapshots. A renamed column produces a detectable diff within one pipeline run, not one sprint.

3. **Statistical baselines are refreshed on cadence** — the ValidationRunner stores baselines in `schema_snapshots/baselines.json` and flags when the current distribution deviates by more than 2 standard deviations. If the business legitimately changes (volume doubles), the baseline is re-anchored with a documented decision, not silently overwritten.

4. **Violation log as a forcing function** — every contract violation is written to `violation_log/violations.jsonl` with a severity level. The Week 8 Sentinel consumes this log. A contract that produces persistent false positives is visible as noise in the alert pipeline, which creates organisational pressure to fix the contract rather than ignore it.

5. **LLM annotation of ambiguous columns** — the ContractGenerator uses Claude to annotate columns whose business meaning is unclear from name and sample values alone. This reduces the most common cause of stale contracts: clauses that were technically correct but semantically wrong from day one.

---

*DOMAIN_NOTES.md — generated for Week 7 TRP submission. All examples drawn from authentic Week 1–5 system outputs.*
