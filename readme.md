# Data Contract Enforcer — Week 7

Schema Integrity & Lineage Attribution System for a five-system AI platform.

---

## Overview

This system enforces data contracts across five interdependent systems built in Weeks 1–5. It catches schema violations, traces them to the git commit that caused them, computes blast radius via a contract registry, and generates a plain-language Enforcer Report.

**Systems monitored:**
- Week 1 — Intent-Code Correlator
- Week 2 — Digital Courtroom
- Week 3 — Document Intelligence Refinery
- Week 4 — Brownfield Cartographer
- Week 5 — Event Sourcing Platform

---

## Prerequisites

```bash
pip install pandas pyyaml anthropic sentence-transformers numpy jsonschema requests
```

For LLM annotation (optional):
```bash
export OPENROUTER_API_KEY=your_key_here
```

---

## Repository Layout

```
├── contracts/
│   ├── generator.py        # ContractGenerator
│   ├── runner.py           # ValidationRunner
│   ├── attributor.py       # ViolationAttributor
│   ├── schema_analyzer.py  # SchemaEvolutionAnalyzer
│   ├── ai_extensions.py    # AI Contract Extensions
│   └── report_generator.py # EnforcerReport
├── contract_registry/
│   └── subscriptions.yaml  # Inter-system dependency registry
├── generated_contracts/    # Auto-generated Bitol YAML + dbt files
├── outputs/                # Canonical JSONL outputs (weeks 1-5 + traces)
├── validation_reports/     # Structured validation report JSON
├── violation_log/          # Violation records JSONL
├── schema_snapshots/       # Timestamped schema snapshots + baselines
├── migration_impact_reports/ # Breaking change impact reports
├── enforcer_report/        # Auto-generated Enforcer Report
├── migrate_to_canonical.py # Migration script (raw → canonical JSONL)
└── migrate_week3_to_lineage.py # Week 3 lineage snapshot builder
```

---

## Step 0 — Generate Canonical Outputs

Run once to transform raw week 1–5 outputs into canonical JSONL format.

```bash
python migrate_to_canonical.py
```

**Expected output:**
```
[ Week 1 ] Intent Records
  ✓ wrote 10 records → outputs/week1/intent_records.jsonl
[ Week 2 ] Verdict Records
  ✓ wrote 10 records → outputs/week2/verdicts.jsonl
[ Week 3 ] Extraction Records
  ✓ wrote 50 records → outputs/week3/extractions.jsonl
[ Week 4 ] Lineage Snapshots
  ✓ wrote 1 records → outputs/week4/lineage_snapshots.jsonl
[ Week 5 ] Event Records
  ✓ wrote 50 records → outputs/week5/events.jsonl
[ Traces ] Synthetic LangSmith Traces
  ✓ wrote 25 records → outputs/traces/runs.jsonl
```

Then build the Week 4 lineage snapshot from the Week 3 codebase:

```bash
python migrate_week3_to_lineage.py --repo /path/to/Document-Intelligence-Refinery
```

**Expected output:**
```
→ Building lineage snapshot from: /path/to/Document-Intelligence-Refinery
  ✓ found 27 Python files
  ✓ git commit: b0aadd64d112...
  ✓ built 32 nodes
  ✓ built 4 edges
  ✓ snapshot written → outputs/week4/lineage_snapshots.jsonl
```

---

## Step 1 — ContractGenerator

Generates Bitol YAML contracts and dbt schema.yml files from JSONL outputs.

```bash
# Generate contracts for all sources
python contracts/generator.py --all

# Generate for a single source (required by evaluator)
python contracts/generator.py \
    --source outputs/week3/extractions.jsonl \
    --output generated_contracts/

# With LLM annotation (requires OPENROUTER_API_KEY)
python contracts/generator.py --all --annotate
```

**Expected output:**
```
→ Generating contract for: outputs/week3/extractions.jsonl (detected: week3)
  ✓ loaded 50 records, 10 top-level columns
  ✓ lineage: 3 downstream consumers identified
  ✓ contract → generated_contracts/week3_extractions.yaml
  ✓ dbt schema → generated_contracts/week3_extractions_dbt.yml
  ✓ snapshot → schema_snapshots/week3-document-refinery-extractions/20260402_190028.yaml
  ✓ 10 schema clauses generated
```

**Key outputs:**
- `generated_contracts/week3_extractions.yaml` — Bitol contract with 10+ clauses
- `generated_contracts/week3_extractions_dbt.yml` — dbt schema with expression tests
- `generated_contracts/week5_events.yaml` — Bitol contract with 13 clauses
- `schema_snapshots/` — timestamped snapshots for evolution tracking

---

## Step 2 — ValidationRunner

Executes all contract checks against a data snapshot.

```bash
# Run on Week 3 (required by evaluator)
python contracts/runner.py \
    --contract generated_contracts/week3_extractions.yaml \
    --data outputs/week3/extractions.jsonl

# Run on Week 5
python contracts/runner.py \
    --contract generated_contracts/week5_events.yaml \
    --data outputs/week5/events.jsonl

# With verbose failure output
python contracts/runner.py \
    --contract generated_contracts/week3_extractions.yaml \
    --data outputs/week3/extractions.jsonl \
    --verbose
```

**Expected output (Week 3):**
```
→ ValidationRunner
  contract : generated_contracts/week3_extractions.yaml
  data     : outputs/week3/extractions.jsonl
  contract_id: week3-document-refinery-extractions
  rows loaded: 50
  snapshot_id: f91aa529...

  ── Validation Summary ──
  total checks : 23
  passed       : 23
  failed       : 0
  warned       : 0
  errored      : 0
  pass rate    : 100.0%

  report → validation_reports/week3_20260402_1900.json
```

**Expected output (Week 5):**
```
  total checks : 30
  passed       : 26
  failed       : 4
  pass rate    : 86.7%
```

The 4 failures in Week 5 are known migration artefacts from synthetic padding records — documented in `violation_log/violations.jsonl`.

---

## Step 3 — ViolationAttributor

Traces violations to origin using registry blast radius, lineage traversal, and git blame.

```bash
# Attribute failures from a validation report
python contracts/attributor.py \
    --report validation_reports/week5_20260402_1900.json

# Inject a synthetic violation for testing
python contracts/attributor.py \
    --inject \
    --contract-id week3-document-refinery-extractions \
    --field extracted_facts.confidence \
    --message "confidence is in 0-100 range, not 0.0-1.0. Breaking change detected."
```

**Expected output:**
```
→ ViolationAttributor
  contract_id: week5-event-records
  total checks: 30
  failures: 4

  ── Attributing: week5-event-records.aggregate_id.pattern ──
  ✓ registry blast radius: 2 subscribers affected
  ✓ lineage enrichment: 3 nodes
  ✓ blame chain: 3 candidates
    → rank 1: src/agents/extractor.py | b0aadd64d112 | estifanosteklay1@gmail.com | score=0.9

  violation written → violation_log/violations.jsonl
```

**Key output:** `violation_log/violations.jsonl` with blame chain, registry blast radius, and contamination depth.

---

## Step 4 — SchemaEvolutionAnalyzer

Diffs schema snapshots, classifies changes, and generates migration impact reports.

```bash
# Inject a breaking change and analyze (generates migration impact report)
python contracts/schema_analyzer.py \
    --contract-id week3-document-refinery-extractions \
    --inject-change

# Analyze Week 5 evolution
python contracts/schema_analyzer.py \
    --contract-id week5-event-records \
    --inject-change

# Diff two specific snapshots
python contracts/schema_analyzer.py \
    --snapshot-a schema_snapshots/week3-document-refinery-extractions/20260401_190000.yaml \
    --snapshot-b schema_snapshots/week3-document-refinery-extractions/20260402_190028.yaml
```

**Expected output:**
```
→ SchemaEvolutionAnalyzer
  snapshot A: schema_snapshots/week3-.../20260401_190000.yaml
  snapshot B: schema_snapshots/week3-.../20260402_190028_injected.yaml
  fields in A: 10, fields in B: 9
  total changes: 3
  breaking changes: 2

  ── Breaking Changes ──
  ✗ [NARROW_TYPE] payload.confidence_score.type  number → integer
  ✗ [REMOVE_FIELD] source_hash.existence

  compatibility verdict: BREAKING
  report → migration_impact_reports/migration_impact_week3_20260402_1900.json
```

---

## Step 5 — AI Contract Extensions

Runs embedding drift detection, prompt input schema validation, and LLM output schema violation rate.

```bash
# Run all three extensions
python contracts/ai_extensions.py --all

# Run individually
python contracts/ai_extensions.py --embedding-drift
python contracts/ai_extensions.py --prompt-validation
python contracts/ai_extensions.py --output-schema
```

**Expected output:**
```
── Extension 1: Embedding Drift Detection ──
  ✓ status: BASELINE_SET
  drift score: 0.0
  threshold:   0.15

── Extension 2: Prompt Input Schema Validation ──
  ✓ status: PASS
  total: 50, valid: 50, violations: 0
  violation rate: 0.00%

── Extension 3: LLM Output Schema Violation Rate ──
  ✓ status: PASS
  violation rate: 0.00%
  trend: stable

  overall AI risk: LOW
```

**Key output:** `validation_reports/ai_metrics.json` with all three metrics.

---

## Step 6 — ReportGenerator

Auto-generates the Enforcer Report from live validation data.

```bash
python contracts/report_generator.py
```

**Expected output:**
```
→ ReportGenerator
  violations loaded:   4
  validation reports:  2
  schema snapshots:    6 contracts tracked
  ✓ report_data.json → enforcer_report/report_data.json
  data_health_score: 94.4/100
  ✓ markdown report → enforcer_report/report_20260402.md

  ── Enforcer Report Summary ──
  health score    : 94.4/100
  violations      : 4
  ai risk         : LOW — baselines being established
  recommendations : 3
```

**Key outputs:**
- `enforcer_report/report_data.json` — machine-readable report with `data_health_score`
- `enforcer_report/report_20260402.md` — human-readable report for PDF embedding

---

## Running End-to-End on a Fresh Clone

```bash
git clone https://github.com/your-username/Data-Contract-Enforcer.git
cd Data-Contract-Enforcer
pip install pandas pyyaml sentence-transformers numpy jsonschema requests

# 1. Rebuild outputs
python migrate_to_canonical.py
python migrate_week3_to_lineage.py --repo /path/to/Document-Intelligence-Refinery

# 2. Generate contracts
python contracts/generator.py --all

# 3. Validate
python contracts/runner.py --contract generated_contracts/week3_extractions.yaml --data outputs/week3/extractions.jsonl
python contracts/runner.py --contract generated_contracts/week5_events.yaml --data outputs/week5/events.jsonl

# 4. Attribute violations
python contracts/attributor.py --report validation_reports/$(ls -t validation_reports/week5*.json | head -1 | xargs basename)
python contracts/attributor.py --inject --contract-id week3-document-refinery-extractions --field extracted_facts.confidence

# 5. Analyze schema evolution
python contracts/schema_analyzer.py --contract-id week3-document-refinery-extractions --inject-change

# 6. Run AI extensions
python contracts/ai_extensions.py --all

# 7. Generate report
python contracts/report_generator.py
```

---

## Notes for Evaluators

- **Week 3 validation:** 23 checks, 23 passed (100%). All structural, range, pattern, format, and statistical drift checks pass.
- **Week 5 validation:** 30 checks, 26 passed (86.7%). 4 failures are known migration artefacts from synthetic padding records — documented in `violation_log/violations.jsonl`.
- **Violation log:** 4 records — 3 real violations from Week 5, 1 intentionally injected violation simulating a confidence scale change.
- **Embedding baseline:** established on first run. Run `ai_extensions.py --embedding-drift` twice to see drift detection active.
- **LangSmith traces:** currently synthetic. Real traces can be substituted by exporting from LangSmith and replacing `outputs/traces/runs.jsonl`.
