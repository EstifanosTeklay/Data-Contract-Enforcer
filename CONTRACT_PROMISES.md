# Contract Promises (Quick Reference)

This document summarizes what each generated contract promises to provide, in plain language.

## Week 1 - Intent Records
Source: `outputs/week1/intent_records.jsonl`

Promise 1: Every record has a unique intent identifier (`intent_id`) in UUID format.

Promise 2: Every record includes a human intent description and associated code references.

Promise 3: Every record includes governance tags and a creation timestamp (`created_at`).

Promise 4: The dataset will never have missing or duplicate `intent_id` values.

Promise 5: The contract expects at least 5 rows of data to be present.

## Week 2 - Verdict Records
Source: `outputs/week2/verdicts.jsonl`

Promise 1: Every verdict has a unique UUID (`verdict_id`) and full scoring metadata.

Promise 2: `overall_verdict` is always one of: `PASS`, `FAIL`, `WARN`.

Promise 3: `overall_score` is always between 1.0 and 5.0.

Promise 4: `confidence` is always between 0.0 and 1.0.

Promise 5: No missing or duplicate `verdict_id` values are allowed.

Promise 6: Includes timestamp `evaluated_at` and score dimensions for quality, coverage, documentation, and security.

## Week 3 - Extraction Records
Source: `outputs/week3/extractions.jsonl`

Promise 1: Each extraction has a unique document ID (`doc_id`, UUID).

Promise 2: Each record includes source provenance (`source_path`, `source_hash`) where `source_hash` is SHA-256 format.

Promise 3: Extracted content is delivered as structured arrays (`extracted_facts`, `entities`).

Promise 4: `processing_time_ms` is positive and bounded by observed profile constraints.

Promise 5: `extraction_model` matches expected model naming (`claude-*` or `gpt-*`).

Promise 6: `extracted_at` is an ISO date-time value.

Promise 7: Downstream consumers depend on this contract:
- `week4-cartographer` (uses `doc_id`, `extracted_facts`, `extraction_model`)
- `week7-contract-enforcer` (uses `doc_id`, `extracted_facts`, `entities`)

## Week 4 - Lineage Snapshots
Source: `outputs/week4/lineage_snapshots.jsonl`

Promise 1: Every snapshot has a unique UUID (`snapshot_id`).

Promise 2: Every snapshot includes repository context (`codebase_root`) and graph structures (`nodes`, `edges`).

Promise 3: `git_commit` is exactly a 40-character hexadecimal SHA-1 string.

Promise 4: `captured_at` is present as an ISO date-time value.

Promise 5: No missing or duplicate `snapshot_id` values are allowed.

Promise 6: Downstream dependency exists:
- `week7-violation-attributor` (uses `nodes`, `edges`, `git_commit`)

## Week 5 - Event Records
Source: `outputs/week5/events.jsonl`

Promise 1: Every event has a unique UUID (`event_id`).

Promise 2: `event_type` is mandatory and constrained to registered domain event names.

Promise 3: `aggregate_id`, `aggregate_type`, and `sequence_number` are required for event stream ordering and identity.

Promise 4: `sequence_number` starts at 1 and remains positive.

Promise 5: `occurred_at` and `recorded_at` are mandatory date-times; contract semantics require `recorded_at >= occurred_at`.

Promise 6: `payload.confidence_score` is constrained to 0.0-1.0.

Promise 7: Required trace metadata is present (`metadata.correlation_id`, `metadata.user_id`, `metadata.source_service`).

Promise 8: Downstream dependency exists:
- `week7-contract-enforcer` (uses `event_type`, `payload`, `schema_version`)

## Traces - LangSmith Run Records
Source: `outputs/traces/runs.jsonl`

Promise 1: Every trace has a unique UUID (`id`) and run metadata (`name`, `run_type`, timings).

Promise 2: `run_type` is constrained to known telemetry types (e.g., `chain`, `llm`).

Promise 3: Token accounting and cost fields are always non-negative (`total_tokens`, `prompt_tokens`, `completion_tokens`, `total_cost`).

Promise 4: Temporal fields are present (`start_time`, `end_time`) and expected to maintain `end_time > start_time` semantics.

Promise 5: Confidence values in traced outputs remain in 0.0-1.0 range where applicable.

Promise 6: Downstream analytics dependency exists:
- `week7-ai-contract-extensions` (uses `inputs`, `outputs`, `total_tokens`, `total_cost`)

---

## How To Use This File

- Use this as a quick agreement checklist during reviews.
- If a producer changes any promised field/rule, treat it as a contract change.
- For implementation details, open the corresponding YAML in `generated_contracts/`.
