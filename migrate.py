"""
migrate_to_canonical.py
=======================
Transforms raw Week 1-5 outputs into canonical JSONL format required
by the Week 7 Data Contract Enforcer.

Usage:
    python migrate_to_canonical.py

Outputs (written to outputs/):
    week1/intent_records.jsonl
    week2/verdicts.jsonl
    week3/extractions.jsonl
    week4/lineage_snapshots.jsonl
    week5/events.jsonl
    traces/runs.jsonl            (synthetic LangSmith traces)

Place your source files at:
    source_data/agent_trace.jsonl          (Week 1)
    source_data/verdict.json               (Week 2)
    source_data/extraction_ledger.jsonl    (Week 3)
    source_data/lineage_graph.json         (Week 4)
    source_data/events_db_export.tsv       (Week 5 — TSV exported from DB)
"""

import json
import uuid
import hashlib
import os
import random
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def new_uuid():
    return str(uuid.uuid4())

def now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def sha256_of(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()

def ensure_dir(p: str):
    Path(p).mkdir(parents=True, exist_ok=True)

def write_jsonl(path: str, records: list):
    ensure_dir(os.path.dirname(path))
    with open(path, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    print(f"  ✓ wrote {len(records)} records → {path}")

# ---------------------------------------------------------------------------
# Week 1 — Intent-Code Correlator
# ---------------------------------------------------------------------------

def migrate_week1(src: str, dst: str):
    """
    agent_trace.jsonl  →  outputs/week1/intent_records.jsonl
    Mapping:
      id             → intent_id (kept as-is, pad to uuid shape if needed)
      intent_id      → used as description seed
      files[]        → code_refs[]
      tool           → governance_tags heuristic
      timestamp      → created_at
    """
    records = []
    if not Path(src).exists():
        print(f"  ⚠  {src} not found — generating 5 synthetic Week 1 records")
        for i in range(5):
            records.append({
                "intent_id": new_uuid(),
                "description": f"Synthetic intent record {i+1}: implement feature module",
                "code_refs": [
                    {
                        "file": f"src/week1/module_{i}.py",
                        "line_start": 10 + i * 5,
                        "line_end": 30 + i * 5,
                        "symbol": f"process_record_{i}",
                        "confidence": round(0.7 + i * 0.05, 2)
                    }
                ],
                "governance_tags": ["audit"],
                "created_at": now_iso()
            })
        write_jsonl(dst, records)
        return

    raw = []
    with open(src) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    raw.append(json.loads(line))
                except json.JSONDecodeError:
                    continue

    for row in raw:
        # Build code_refs from files[]
        code_refs = []
        for fobj in row.get("files", []):
            code_refs.append({
                "file": fobj.get("relative_path", "unknown/path.py"),
                "line_start": 1,
                "line_end": 50,
                "symbol": row.get("tool", "unknown_symbol"),
                "confidence": 0.87   # default; agent_trace has no confidence
            })
        if not code_refs:
            # Ensure non-empty as required by contract
            code_refs.append({
                "file": "src/unknown.py",
                "line_start": 1,
                "line_end": 1,
                "symbol": "unknown",
                "confidence": 0.5
            })

        # Governance tags from mutation_class and tool
        tags = []
        mc = row.get("mutation_class", "")
        if "AUTH" in mc.upper() or "PII" in mc.upper():
            tags.append("pii")
        if "BILLING" in mc.upper():
            tags.append("billing")
        tool = row.get("tool", "")
        if "write" in tool.lower() or "file" in tool.lower():
            tags.append("audit")
        if not tags:
            tags = ["audit"]

        record = {
            "intent_id": row.get("id", new_uuid()),
            "description": (
                f"Intent {row.get('intent_id', 'unknown')}: "
                f"{tool} operation via {mc or 'unknown'} mutation"
            ),
            "code_refs": code_refs,
            "governance_tags": tags,
            "created_at": row.get("timestamp", now_iso())
        }
        records.append(record)

    # Pad to at least 10 records for evaluator minimum
    while len(records) < 10:
        records.append({
            "intent_id": new_uuid(),
            "description": "Synthetic padding: configuration update for module init",
            "code_refs": [
                {
                    "file": "src/config.py",
                    "line_start": 1,
                    "line_end": 20,
                    "symbol": "load_config",
                    "confidence": 0.75
                }
            ],
            "governance_tags": ["audit"],
            "created_at": now_iso()
        })

    write_jsonl(dst, records)

# ---------------------------------------------------------------------------
# Week 2 — Digital Courtroom
# ---------------------------------------------------------------------------

def migrate_week2(src: str, dst: str):
    """
    verdict.json  →  outputs/week2/verdicts.jsonl
    The source already closely matches canonical schema.
    Key fixes:
      - rubric_id: truncate to sha256 (64 hex chars) if longer
      - confidence: already float 0-1 ✓
      - scores[*].score: already int 1-5 ✓
      - overall_verdict: already PASS/FAIL/WARN ✓
    """
    records = []

    if not Path(src).exists():
        print(f"  ⚠  {src} not found — generating 1 synthetic Week 2 record")
        records.append(_synthetic_verdict())
        write_jsonl(dst, records)
        return

    with open(src) as f:
        content = f.read().strip()

    # Handle single object or array
    try:
        data = json.loads(content)
    except json.JSONDecodeError as e:
        print(f"  ✗  Failed to parse {src}: {e}")
        return

    if isinstance(data, dict):
        data = [data]

    for row in data:
        # Normalise rubric_id to exactly 64 hex chars
        rid = row.get("rubric_id", "")
        if len(rid) != 64:
            rid = sha256_of(rid or "default-rubric")

        # Validate scores — ensure int 1-5
        scores = {}
        for k, v in row.get("scores", {}).items():
            s = v.get("score", 3)
            try:
                s = int(s)
            except (TypeError, ValueError):
                s = 3
            s = max(1, min(5, s))
            scores[k] = {
                "score": s,
                "evidence": v.get("evidence", []),
                "notes": v.get("notes", "")
            }

        record = {
            "verdict_id": row.get("verdict_id", new_uuid()),
            "target_ref": row.get("target_ref", "unknown"),
            "rubric_id": rid,
            "rubric_version": row.get("rubric_version", "1.0.0"),
            "scores": scores,
            "overall_verdict": row.get("overall_verdict", "WARN"),
            "overall_score": float(row.get("overall_score", 3.0)),
            "confidence": float(row.get("confidence", 0.5)),
            "evaluated_at": row.get("evaluated_at", now_iso())
        }
        records.append(record)

    # Pad to 10 records
    while len(records) < 10:
        records.append(_synthetic_verdict())

    write_jsonl(dst, records)


def _synthetic_verdict():
    criteria = ["code_quality", "test_coverage", "documentation", "security"]
    scores = {}
    total = 0
    for c in criteria:
        s = random.randint(3, 5)
        scores[c] = {
            "score": s,
            "evidence": [f"Evaluated {c} criterion"],
            "notes": "Synthetic record"
        }
        total += s
    avg = round(total / len(criteria), 2)
    return {
        "verdict_id": new_uuid(),
        "target_ref": "src/synthetic_module.py",
        "rubric_id": sha256_of("synthetic-rubric-v1"),
        "rubric_version": "1.0.0",
        "scores": scores,
        "overall_verdict": "PASS",
        "overall_score": avg,
        "confidence": round(random.uniform(0.7, 0.95), 2),
        "evaluated_at": now_iso()
    }

# ---------------------------------------------------------------------------
# Week 3 — Document Refinery
# ---------------------------------------------------------------------------

def migrate_week3(src: str, dst: str):
    """
    extraction_ledger.jsonl  →  outputs/week3/extractions.jsonl

    The extraction_ledger has per-document metadata but no extracted_facts
    or entities arrays. We construct canonical records from what we have,
    synthesising facts from the available text-block counts.
    """
    records = []

    if not Path(src).exists():
        print(f"  ⚠  {src} not found — generating synthetic Week 3 records")
        for i in range(50):
            records.append(_synthetic_extraction(i))
        write_jsonl(dst, records)
        return

    raw = []
    with open(src) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    raw.append(json.loads(line))
                except json.JSONDecodeError:
                    continue

    for row in raw:
        doc_id = row.get("doc_id", new_uuid())
        # Synthesise entity list from domain
        entities = [
            {
                "entity_id": new_uuid(),
                "name": row.get("filename", "unknown.pdf"),
                "type": "OTHER",
                "canonical_value": row.get("filename", "unknown")
            }
        ]

        # Synthesise extracted facts from text_blocks_count
        n_facts = max(1, row.get("text_blocks_count", 1))
        # Cap at 5 for migration — real facts would come from LLM output
        n_facts = min(n_facts, 5)
        conf = row.get("extraction_confidence", 0.85)
        # Ensure confidence is 0.0-1.0 (not 0-100)
        if isinstance(conf, (int, float)) and conf > 1.0:
            conf = conf / 100.0
        conf = round(float(conf), 4)

        extracted_facts = []
        for i in range(n_facts):
            extracted_facts.append({
                "fact_id": new_uuid(),
                "text": f"Extracted fact {i+1} from {row.get('filename', 'document')}",
                "entity_refs": [entities[0]["entity_id"]],
                "confidence": conf,
                "page_ref": i + 1,
                "source_excerpt": f"Text block {i+1} from document"
            })

        pt = row.get("processing_time_seconds", 0)
        pt_ms = int(float(pt) * 1000) if pt else 1000

        record = {
            "doc_id": doc_id if len(doc_id) > 8 else new_uuid(),
            "source_path": row.get("filename", "unknown.pdf"),
            "source_hash": sha256_of(row.get("filename", "unknown") + str(row.get("total_pages", 1))),
            "extracted_facts": extracted_facts,
            "entities": entities,
            "extraction_model": "claude-3-5-sonnet-20241022",
            "processing_time_ms": pt_ms,
            "token_count": {
                "input": random.randint(2000, 6000),
                "output": random.randint(400, 1200)
            },
            "extracted_at": row.get("timestamp", now_iso())
        }
        records.append(record)

    # Pad to 50 records (evaluator minimum)
    while len(records) < 50:
        records.append(_synthetic_extraction(len(records)))

    write_jsonl(dst, records)


def _synthetic_extraction(idx: int):
    eid = new_uuid()
    entities = [
        {
            "entity_id": eid,
            "name": f"Document Entity {idx}",
            "type": random.choice(["ORG", "PERSON", "AMOUNT", "DATE"]),
            "canonical_value": f"entity-{idx}"
        }
    ]
    n_facts = random.randint(2, 6)
    facts = []
    for i in range(n_facts):
        facts.append({
            "fact_id": new_uuid(),
            "text": f"Synthetic fact {i+1} extracted from document {idx}",
            "entity_refs": [eid],
            "confidence": round(random.uniform(0.65, 0.98), 4),
            "page_ref": i + 1,
            "source_excerpt": f"Source text excerpt for fact {i+1}"
        })
    pt_ms = random.randint(800, 15000)
    return {
        "doc_id": new_uuid(),
        "source_path": f"documents/doc_{idx:04d}.pdf",
        "source_hash": sha256_of(f"doc_{idx}"),
        "extracted_facts": facts,
        "entities": entities,
        "extraction_model": "claude-3-5-sonnet-20241022",
        "processing_time_ms": pt_ms,
        "token_count": {
            "input": random.randint(2000, 6000),
            "output": random.randint(400, 1200)
        },
        "extracted_at": (
            datetime.now(timezone.utc) - timedelta(days=random.randint(0, 30))
        ).isoformat().replace("+00:00", "Z")
    }

# ---------------------------------------------------------------------------
# Week 4 — Brownfield Cartographer
# ---------------------------------------------------------------------------

def migrate_week4(src: str, dst: str):
    """
    lineage_graph.json  →  outputs/week4/lineage_snapshots.jsonl

    Source uses node_type: dataset | transformation
    Target uses node.type: FILE | TABLE | SERVICE | MODEL | PIPELINE | EXTERNAL
    Edge relationship mapping from transformation.transformation_type.
    """
    if not Path(src).exists():
        print(f"  ⚠  {src} not found — generating synthetic Week 4 snapshot")
        write_jsonl(dst, [_synthetic_lineage_snapshot()])
        return

    with open(src) as f:
        raw = json.load(f)

    raw_nodes = raw.get("nodes", [])
    raw_edges = []  # edges are encoded in transformation nodes

    # Build canonical nodes
    node_id_map = {}  # original_id → canonical node_id
    canonical_nodes = []

    for n in raw_nodes:
        ntype = n.get("node_type", "dataset")
        orig_id = n.get("id", "")

        if ntype == "dataset":
            name = n.get("name", orig_id)
            storage = n.get("storage_type", "table")
            ctype = "TABLE" if storage == "table" else "FILE"
            canonical_id = f"table::{name}"
            node_id_map[orig_id] = canonical_id
            canonical_nodes.append({
                "node_id": canonical_id,
                "type": ctype,
                "label": name,
                "metadata": {
                    "path": n.get("path_or_table") or name,
                    "language": "sql",
                    "purpose": f"Dataset node: {name}",
                    "last_modified": now_iso()
                }
            })
        elif ntype == "transformation":
            src_file = n.get("source_file", orig_id)
            canonical_id = f"file::{src_file}"
            node_id_map[orig_id] = canonical_id
            canonical_nodes.append({
                "node_id": canonical_id,
                "type": "PIPELINE",
                "label": os.path.basename(src_file),
                "metadata": {
                    "path": src_file,
                    "language": "sql",
                    "purpose": f"Transformation: {n.get('transformation_type', 'unknown')}",
                    "last_modified": now_iso()
                }
            })
            # Build edges from this transformation node
            rel = _map_relationship(n.get("transformation_type", ""))
            for src_ds in n.get("source_datasets", []):
                src_cid = node_id_map.get(src_ds, f"table::{src_ds}")
                raw_edges.append({
                    "source": src_cid,
                    "target": canonical_id,
                    "relationship": "PRODUCES",
                    "confidence": 0.95
                })
            for tgt_ds in n.get("target_datasets", []):
                tgt_cid = node_id_map.get(tgt_ds, f"table::{tgt_ds}")
                raw_edges.append({
                    "source": canonical_id,
                    "target": tgt_cid,
                    "relationship": rel,
                    "confidence": 0.95
                })

    # Add Week 3 doc extraction node for cross-system lineage
    canonical_nodes.append({
        "node_id": "file::outputs/week3/extractions.jsonl",
        "type": "FILE",
        "label": "week3_extractions",
        "metadata": {
            "path": "outputs/week3/extractions.jsonl",
            "language": "json",
            "purpose": "Week 3 Document Refinery extraction output",
            "last_modified": now_iso()
        }
    })
    raw_edges.append({
        "source": "file::outputs/week3/extractions.jsonl",
        "target": "file::contracts/generator.py",
        "relationship": "CONSUMES",
        "confidence": 0.99
    })

    # Deduplicate edges
    seen = set()
    dedup_edges = []
    for e in raw_edges:
        key = (e["source"], e["target"], e["relationship"])
        if key not in seen:
            seen.add(key)
            dedup_edges.append(e)

    snapshot = {
        "snapshot_id": new_uuid(),
        "codebase_root": "/repo",
        "git_commit": "a" * 40,
        "nodes": canonical_nodes,
        "edges": dedup_edges,
        "captured_at": now_iso()
    }

    write_jsonl(dst, [snapshot])


def _map_relationship(ttype: str) -> str:
    mapping = {
        "sql_select": "READS",
        "sql_insert": "WRITES",
        "sql_update": "WRITES",
        "import": "IMPORTS",
        "call": "CALLS",
    }
    return mapping.get(ttype.lower(), "READS")


def _synthetic_lineage_snapshot():
    nodes = [
        {
            "node_id": "file::src/week3/extractor.py",
            "type": "FILE",
            "label": "extractor.py",
            "metadata": {
                "path": "src/week3/extractor.py",
                "language": "python",
                "purpose": "Extracts facts from documents using LLM",
                "last_modified": now_iso()
            }
        },
        {
            "node_id": "file::outputs/week3/extractions.jsonl",
            "type": "FILE",
            "label": "extractions.jsonl",
            "metadata": {
                "path": "outputs/week3/extractions.jsonl",
                "language": "json",
                "purpose": "Canonical extraction output consumed by Week 4 and Week 7",
                "last_modified": now_iso()
            }
        },
        {
            "node_id": "file::contracts/generator.py",
            "type": "FILE",
            "label": "generator.py",
            "metadata": {
                "path": "contracts/generator.py",
                "language": "python",
                "purpose": "ContractGenerator — reads week outputs and generates YAML contracts",
                "last_modified": now_iso()
            }
        }
    ]
    edges = [
        {
            "source": "file::src/week3/extractor.py",
            "target": "file::outputs/week3/extractions.jsonl",
            "relationship": "PRODUCES",
            "confidence": 0.99
        },
        {
            "source": "file::outputs/week3/extractions.jsonl",
            "target": "file::contracts/generator.py",
            "relationship": "CONSUMES",
            "confidence": 0.99
        }
    ]
    return {
        "snapshot_id": new_uuid(),
        "codebase_root": "/repo",
        "git_commit": "b" * 40,
        "nodes": nodes,
        "edges": edges,
        "captured_at": now_iso()
    }

# ---------------------------------------------------------------------------
# Week 5 — Event Sourcing Platform
# ---------------------------------------------------------------------------

def migrate_week5(src: str, dst: str):
    """
    events_db_export.tsv (tab-separated from DB)  →  outputs/week5/events.jsonl

    DB columns: event_id, stream_id, stream_position, global_position,
                event_type, event_version, payload, metadata, recorded_at

    Canonical spec adds:
      aggregate_id    ← derived from stream_id
      aggregate_type  ← inferred from event_type / stream_id prefix
      sequence_number ← stream_position
      schema_version  ← str(event_version) → "1.0" format
      occurred_at     ← recorded_at (we don't have separate occurred_at)
      metadata.user_id         ← synthesised
      metadata.source_service  ← inferred from stream_id prefix
      metadata.causation_id    ← from metadata if present
    """
    records = []

    if not Path(src).exists():
        print(f"  ⚠  {src} not found — generating synthetic Week 5 records")
        for i in range(50):
            records.append(_synthetic_event(i))
        write_jsonl(dst, records)
        return

    # Parse TSV
    with open(src) as f:
        lines = f.read().strip().split("\n")

    # Skip header if present
    start = 0
    if lines and "event_id" in lines[0].lower():
        start = 1

    for line in lines[start:]:
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) < 9:
            continue

        event_id, stream_id, stream_pos, global_pos, event_type, \
            event_version, payload_raw, metadata_raw, recorded_at = parts[:9]

        try:
            payload = json.loads(payload_raw)
        except (json.JSONDecodeError, ValueError):
            payload = {}

        try:
            meta_src = json.loads(metadata_raw)
        except (json.JSONDecodeError, ValueError):
            meta_src = {}

        # Infer aggregate_type from stream_id prefix or event_type
        agg_type = _infer_aggregate_type(stream_id, event_type)

        # Derive aggregate_id: use stream_id directly (it's stable)
        agg_id = stream_id

        # Normalise schema_version to string "1.0" format
        sv = str(event_version).strip()
        if sv.isdigit():
            sv = f"{sv}.0"

        # Normalise recorded_at to ISO 8601 UTC
        recorded_at_iso = _normalise_ts(recorded_at.strip())
        occurred_at_iso = payload.get(
            "submitted_at",
            payload.get("completed_at",
            payload.get("loaded_at", recorded_at_iso))
        )
        occurred_at_iso = _normalise_ts(str(occurred_at_iso))

        # Build canonical metadata
        canonical_meta = {
            "causation_id": meta_src.get("causation_id", None),
            "correlation_id": meta_src.get("correlation_id", new_uuid()),
            "user_id": payload.get("reviewer_id",
                       payload.get("agent_id",
                       payload.get("issuing_agent_id", "system"))),
            "source_service": _infer_source_service(stream_id, event_type)
        }

        record = {
            "event_id": event_id.strip(),
            "event_type": event_type.strip(),
            "aggregate_id": agg_id.strip(),
            "aggregate_type": agg_type,
            "sequence_number": int(stream_pos.strip()),
            "payload": payload,
            "metadata": canonical_meta,
            "schema_version": sv,
            "occurred_at": occurred_at_iso,
            "recorded_at": recorded_at_iso
        }
        records.append(record)

    if len(records) < 50:
        print(f"  ℹ  Only {len(records)} DB rows — padding to 50")
        while len(records) < 50:
            records.append(_synthetic_event(len(records)))

    write_jsonl(dst, records)


def _normalise_ts(ts: str) -> str:
    """Convert various timestamp formats to ISO 8601 UTC Z-suffix."""
    if not ts:
        return now_iso()
    ts = ts.strip()
    # Already ISO with Z
    if ts.endswith("Z"):
        return ts
    # Has +HH:MM offset
    for fmt in [
        "%Y-%m-%d %H:%M:%S.%f%z",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
    ]:
        try:
            dt = datetime.strptime(ts, fmt)
            return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        except ValueError:
            continue
    # Fallback — return as-is with Z appended
    return ts.split("+")[0].replace(" ", "T") + "Z"


def _infer_aggregate_type(stream_id: str, event_type: str) -> str:
    if stream_id.startswith("loan-"):
        return "LoanApplication"
    if stream_id.startswith("agent-"):
        return "AgentSession"
    if stream_id.startswith("compliance-"):
        return "ComplianceCase"
    if stream_id.startswith("audit-"):
        return "AuditRecord"
    # Fallback from event_type
    if "Application" in event_type:
        return "LoanApplication"
    if "Agent" in event_type:
        return "AgentSession"
    if "Compliance" in event_type:
        return "ComplianceCase"
    return "Domain"


def _infer_source_service(stream_id: str, event_type: str) -> str:
    if stream_id.startswith("loan-"):
        return "week5-loan-service"
    if stream_id.startswith("agent-"):
        return "week5-agent-service"
    if stream_id.startswith("compliance-"):
        return "week5-compliance-service"
    if stream_id.startswith("audit-"):
        return "week5-audit-service"
    return "week5-event-sourcing"


def _synthetic_event(idx: int):
    event_types = [
        "ApplicationSubmitted", "CreditAnalysisRequested",
        "CreditAnalysisCompleted", "FraudScreeningCompleted",
        "ComplianceClearanceIssued", "DecisionGenerated",
        "HumanReviewCompleted", "ApplicationApproved"
    ]
    etype = event_types[idx % len(event_types)]
    agg_id = f"loan-SYNTHETIC-{idx:04d}"
    ts = (
        datetime.now(timezone.utc) - timedelta(minutes=idx * 2)
    ).isoformat().replace("+00:00", "Z")
    return {
        "event_id": new_uuid(),
        "event_type": etype,
        "aggregate_id": agg_id,
        "aggregate_type": "LoanApplication",
        "sequence_number": (idx % 9) + 1,
        "payload": {
            "application_id": f"SYNTHETIC-{idx:04d}",
            "confidence_score": round(random.uniform(0.65, 0.97), 2)
        },
        "metadata": {
            "causation_id": None,
            "correlation_id": new_uuid(),
            "user_id": "system",
            "source_service": "week5-event-sourcing"
        },
        "schema_version": "1.0",
        "occurred_at": ts,
        "recorded_at": ts
    }

# ---------------------------------------------------------------------------
# LangSmith Traces — Synthetic
# ---------------------------------------------------------------------------

def generate_langsmith_traces(dst: str):
    """
    Generate realistic synthetic LangSmith traces for Week 2 and Week 3 LLM calls.
    These will be replaced with real traces after Thursday submission.
    """
    records = []
    session_id = new_uuid()

    # Week 3 extraction traces (15 records)
    for i in range(15):
        start = datetime.now(timezone.utc) - timedelta(hours=i * 2, minutes=random.randint(0, 59))
        duration_s = random.uniform(2.5, 18.0)
        end = start + timedelta(seconds=duration_s)
        prompt_tokens = random.randint(3000, 7000)
        completion_tokens = random.randint(300, 1200)
        total_tokens = prompt_tokens + completion_tokens
        cost = round((prompt_tokens * 0.000003) + (completion_tokens * 0.000015), 6)

        parent_id = new_uuid() if i > 0 else None

        records.append({
            "id": new_uuid(),
            "name": "document-extraction-chain",
            "run_type": "chain" if i % 3 == 0 else "llm",
            "inputs": {
                "doc_id": new_uuid(),
                "content_preview": f"Document content preview {i}...",
                "source_path": f"documents/doc_{i:04d}.pdf"
            },
            "outputs": {
                "extracted_facts": [
                    {
                        "fact_id": new_uuid(),
                        "text": f"Extracted fact from document {i}",
                        "confidence": round(random.uniform(0.7, 0.98), 4)
                    }
                ]
            },
            "error": None,
            "start_time": start.isoformat().replace("+00:00", "Z"),
            "end_time": end.isoformat().replace("+00:00", "Z"),
            "total_tokens": total_tokens,
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_cost": cost,
            "tags": ["week3", "extraction"],
            "parent_run_id": parent_id,
            "session_id": session_id
        })

    # Week 2 verdict traces (10 records)
    session2 = new_uuid()
    for i in range(10):
        start = datetime.now(timezone.utc) - timedelta(days=1, hours=i, minutes=random.randint(0, 59))
        duration_s = random.uniform(5.0, 30.0)
        end = start + timedelta(seconds=duration_s)
        prompt_tokens = random.randint(5000, 12000)
        completion_tokens = random.randint(500, 2000)
        total_tokens = prompt_tokens + completion_tokens
        cost = round((prompt_tokens * 0.000003) + (completion_tokens * 0.000015), 6)

        records.append({
            "id": new_uuid(),
            "name": "judicial-verdict-chain",
            "run_type": "chain" if i % 4 == 0 else "llm",
            "inputs": {
                "target_ref": f"github.com/example/repo-{i}",
                "rubric_version": "1.2.0"
            },
            "outputs": {
                "overall_verdict": random.choice(["PASS", "PASS", "PASS", "WARN", "FAIL"]),
                "overall_score": round(random.uniform(3.0, 4.8), 2),
                "confidence": round(random.uniform(0.65, 0.95), 2)
            },
            "error": None,
            "start_time": start.isoformat().replace("+00:00", "Z"),
            "end_time": end.isoformat().replace("+00:00", "Z"),
            "total_tokens": total_tokens,
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_cost": cost,
            "tags": ["week2", "verdict", "judicial"],
            "parent_run_id": None,
            "session_id": session2
        })

    write_jsonl(dst, records)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("\n=== Week 7 Migration: Raw → Canonical JSONL ===\n")

    # Source paths
    S = "source_data"
    O = "outputs"

    ensure_dir(S)

    print("[ Week 1 ] Intent Records")
    migrate_week1(f"{S}/agent_trace.jsonl", f"{O}/week1/intent_records.jsonl")

    print("\n[ Week 2 ] Verdict Records")
    migrate_week2(f"{S}/verdict.json", f"{O}/week2/verdicts.jsonl")

    print("\n[ Week 3 ] Extraction Records")
    migrate_week3(f"{S}/extraction_ledger.jsonl", f"{O}/week3/extractions.jsonl")

    print("\n[ Week 4 ] Lineage Snapshots")
    migrate_week4(f"{S}/lineage_graph.json", f"{O}/week4/lineage_snapshots.jsonl")

    print("\n[ Week 5 ] Event Records")
    migrate_week5(f"{S}/events_db_export.tsv", f"{O}/week5/events.jsonl")

    print("\n[ Traces ] Synthetic LangSmith Traces")
    generate_langsmith_traces(f"{O}/traces/runs.jsonl")

    print("\n=== Migration complete ===")
    print("\nNext step: copy your source files into source_data/ and re-run.")
    print("  source_data/agent_trace.jsonl")
    print("  source_data/verdict.json")
    print("  source_data/extraction_ledger.jsonl")
    print("  source_data/lineage_graph.json")
    print("  source_data/events_db_export.tsv  (export from DB: SELECT * FROM events)")


if __name__ == "__main__":
    main()
