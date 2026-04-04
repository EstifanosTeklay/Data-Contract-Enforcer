# How The Two Projects Work Together

This note explains how these two projects are connected:

- `Data-Contract-Enforcer` — the Week 7 system that generates contracts, validates data, attributes failures, and produces compliance-style reports.
- `agentic-event-ledger` — the Week 5 event-sourced audit system that stores immutable events in streams backed by its `EventStore`.

If you are new to the codebase, the fastest way to think about them is this:

`Data-Contract-Enforcer` is the system that detects and explains data quality problems.

`agentic-event-ledger` is the system that preserves those important findings as durable audit events.

In other words, the Enforcer is the analyst and the Ledger is the legal notebook.

## The Big Picture

These projects are not duplicates. They solve different parts of the same operational problem.

The Data Contract Enforcer watches the structured outputs produced by the Week 1–5 systems. It answers questions like:

- Did the schema change?
- Did a field drift from its promised range?
- Which downstream systems are affected?
- Which commit most likely caused the break?

The Agentic Event Ledger answers a different set of questions:

- Can we store important decisions as immutable events?
- Can we reconstruct what happened later?
- Can we audit the sequence of changes without trusting mutable flat files?
- Can multiple writers append safely without corrupting history?

The integration between them exists because a contract violation is not just a temporary validation result. In a production setting, it is an auditable operational event.

That is the core design decision linking the two repos.

## What Each Project Owns

### 1. Data Contract Enforcer owns detection and explanation

Inside this repo, the main contract workflow lives under `contracts/`:

- `generator.py` profiles datasets and generates Bitol contracts, dbt YAML, schema snapshots, and statistical baselines.
- `runner.py` executes the checks in those contracts against actual JSONL data.
- `attributor.py` takes failures and computes blame chains using the contract registry, lineage graph, and git history.
- `ai_extensions.py` applies AI-specific checks like embedding drift, prompt input schema validation, and LLM output schema validation.
- `report_generator.py` turns the system outputs into a human-readable report.

This project is therefore responsible for understanding whether the platform is healthy and for explaining what broke when it is not.

### 2. Agentic Event Ledger owns durable audit persistence

The sibling `agentic-event-ledger` repo provides the append-only event infrastructure:

- `src/event_store.py` exposes the event store abstraction.
- `src/models/events.py` defines event-related exceptions such as optimistic concurrency conflicts.
- PostgreSQL-backed event streams preserve immutable history.
- The ledger already knows how to handle append semantics, stream versions, and concurrency.

This project is therefore responsible for preserving important operational facts as immutable records rather than leaving them as loose files only.

## The Integration Point

The direct coupling between the two repos lives in `contracts/ledger_bridge.py`.

That file is the adapter layer. It allows the Week 7 repo to call the Week 5 ledger without duplicating ledger logic.

The bridge does four important things:

1. It finds the sibling ledger repo on disk.

The function `_ledger_root()` assumes this layout:

- `Projects/`
  - `Data-Contract-Enforcer/`
  - `agentic-event-ledger/`

So the bridge expects both repos to sit side by side.

2. It adjusts Python import paths.

Because the two projects may run under different virtual environments, the bridge inserts the ledger repo and its `.venv` site-packages into `sys.path` before importing ledger modules.

3. It imports ledger primitives dynamically.

The bridge imports:

- `src.event_store.EventStore`
- `src.models.events.OptimisticConcurrencyError`
- `src.models.events.StreamNotFoundError`

This means the Enforcer does not implement its own event store. It delegates persistence to the ledger project directly.

4. It normalizes Enforcer outputs into ledger-compatible events.

The bridge wraps Enforcer payloads in a minimal event object and appends them into ledger streams using the ledger's optimistic concurrency rules.

## What Actually Flows Between The Projects

The Enforcer does not send all raw source data into the ledger. It sends high-value operational events.

There are currently two major categories of cross-project events.

### A. Schema snapshot events

Produced by: `contracts/generator.py`

When contract generation runs, it creates a schema snapshot and then appends that snapshot to the ledger using `append_schema_snapshot_event(...)`.

Purpose:

- Preserve a durable history of schema evolution.
- Make schema state auditable over time.
- Allow later analysis of when a contract shape changed.

Ledger stream naming:

- `audit-schema-snapshots-{contract_id}`

Example meaning:

- Every time Week 3 extraction schema is regenerated, a new immutable event is appended to the stream for `week3-document-refinery-extractions`.

This is important because the YAML file in `schema_snapshots/` is useful locally, but the ledger stream is the stronger operational record.

### B. Contract violation events

Produced by:

- `contracts/attributor.py`
- `contracts/ai_extensions.py`

When a serious contract issue is discovered, the Enforcer writes the violation to its local JSONL log and also appends it to the ledger using `append_violation_event(...)`.

Purpose:

- Preserve a non-editable record of contract failures.
- Capture severity, message, failing field, blast radius, and blame chain.
- Feed future audit, reporting, and alerting workflows.

Ledger stream naming:

- `audit-contract-violations`

Example meaning:

- If `extracted_facts.confidence` drifts from `0.0–1.0` to `0–100`, the Enforcer can detect it, attribute it, and then append a `ContractViolationDetected` event to the ledger.

That turns a transient validation result into a durable operational fact.

## End-to-End Narrative: What Happens In Real Life

Here is the newcomer version of the full story.

### Step 1. Upstream systems produce data

The Week 1–5 systems produce JSONL outputs under `outputs/`.

Examples:

- Week 3 produces extraction records.
- Week 4 produces lineage snapshots.
- Week 5 produces event records.
- LangSmith traces are exported into `outputs/traces/`.

These files are the raw contract subjects.

### Step 2. The Contract Generator profiles those datasets

`contracts/generator.py` reads those files, profiles them, infers constraints, builds contract YAML, and stores snapshots.

At this point the Enforcer is establishing what the system is promising downstream consumers.

This is a local artifact stage:

- generated contract YAML
- generated dbt schema YAML
- local schema snapshot files
- local statistical baselines

Then the generator crosses project boundaries.

It appends a `SchemaSnapshotPersisted` event into the ledger.

That is the first major integration point.

### Step 3. The Validation Runner checks whether the promise still holds

`contracts/runner.py` executes contract checks against a dataset snapshot.

This is where the Enforcer detects:

- missing required fields
- duplicate IDs
- invalid enum values
- numeric range violations
- type or pattern mismatches
- statistical drift

At this stage the result is still a report. It is not yet an attributed operational incident.

### Step 4. The Attributor explains why the failure happened

`contracts/attributor.py` takes failed checks and enriches them using three sources:

- `contract_registry/subscriptions.yaml` for direct downstream blast radius
- `outputs/week4/lineage_snapshots.jsonl` for graph-based transitive impact
- git history for likely source commits

The output becomes a violation record with:

- check ID
- failing field
- severity
- message
- blast radius
- blame chain

Now the failure has context, not just symptoms.

Then the Attributor crosses project boundaries.

It appends a `ContractViolationDetected` event into the ledger.

That is the second major integration point.

### Step 5. AI Extensions can also produce violations

`contracts/ai_extensions.py` performs checks that normal tabular contracts do not cover well, such as:

- embedding drift
- prompt input schema validation
- LLM output schema violations

When those checks detect meaningful issues, they also append violation events into the same ledger stream.

This matters because not all risk in an AI system is a classic column mismatch. Some risk is model-facing and semantic. The ledger integration makes those failures auditable too.

## Why This Architecture Makes Sense

This integration is useful because each repo is doing the thing it is best at.

### The Enforcer is optimized for reasoning about data contracts

It knows how to:

- read platform outputs
- infer and enforce schema promises
- compare current state to baselines
- compute blast radius
- explain incidents in business and engineering terms

### The Ledger is optimized for preserving history safely

It knows how to:

- append events immutably
- manage stream versions
- handle optimistic concurrency
- preserve audit history in PostgreSQL-backed streams

If the Enforcer tried to build its own audit event infrastructure, it would duplicate a large part of the Week 5 system and likely do it worse.

If the Ledger tried to understand contract semantics directly, it would become bloated with validation logic that belongs elsewhere.

The bridge keeps those responsibilities separate.

## The Most Important Concept For Newcomers

The key idea is that there are two layers of truth.

### Layer 1: Analytical truth in the Enforcer repo

This repo contains rich working artifacts:

- generated contracts
- validation reports
- local violation logs
- schema snapshots
- AI metrics
- blast radius analyses

These files are easy to inspect, regenerate, diff, and use during development.

### Layer 2: Audit truth in the Ledger repo

The ledger stores the durable historical record that should not be casually rewritten.

This is where important operational conclusions become immutable events.

So the Enforcer is the place where evidence is assembled, and the Ledger is the place where final evidence is preserved.

## Streams You Should Know About

If you are onboarding, these are the first ledger streams to understand.

### `audit-contract-violations`

Contains contract and AI-related violation events.

Typical payload content:

- violation ID
- contract ID
- failing field
- severity
- message
- actual vs expected values
- records failing
- blast radius
- blame chain

This stream is the audit trail for broken promises between systems.

### `audit-schema-snapshots-{contract_id}`

Contains schema snapshots persisted by the Contract Generator.

Typical payload content:

- contract ID
- snapshot timestamp
- source path
- schema object

This stream is the audit trail for how a contract evolved over time.

## Failure Modes A Newcomer Should Expect

There are a few practical integration risks.

### 1. The sibling repo is missing

`ledger_bridge.py` assumes `agentic-event-ledger` exists beside this repo. If that folder is missing, imports fail immediately.

### 2. The ledger virtual environment is missing dependencies

The bridge tries to add the ledger `.venv` site-packages to the current process. If the ledger environment is not installed correctly, imports like `asyncpg` may fail.

### 3. The ledger is available, but the database is not

Even if imports work, appending events still depends on the ledger's runtime and database configuration.

### 4. The Enforcer can detect a problem before it can persist it

That is intentional. The Enforcer's analytical files can still exist locally, but the bridge is strict because audit persistence should not silently degrade.

This is an important design choice: if the system claims to create an audit event, and that append fails, the code should fail loudly.

## A Simple Mental Model

If you only remember one thing, remember this sentence:

The Data Contract Enforcer discovers and explains contract events; the Agentic Event Ledger preserves those events as immutable operational history.

That is the interconnection.

## Where To Start Reading In Code

For a new engineer, the best reading order is:

1. `contracts/ledger_bridge.py`
2. `contracts/generator.py`
3. `contracts/runner.py`
4. `contracts/attributor.py`
5. `contracts/ai_extensions.py`
6. `contract_registry/subscriptions.yaml`
7. The `agentic-event-ledger` repo README and `src/event_store.py`

That sequence moves from the narrow integration seam outward into the broader architecture.

## Practical Summary

From a platform perspective, these repos form a producer-of-audit-events and keeper-of-audit-events pair.

- `Data-Contract-Enforcer` produces high-value operational facts about schema health, drift, and contract failure.
- `agentic-event-ledger` stores those facts in immutable streams.
- `ledger_bridge.py` is the seam that connects the two.
- Schema snapshots and contract violations are the primary shared event types.
- The result is a system that not only detects silent breakage, but also preserves a trustworthy history of it.
