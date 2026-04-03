"""
contracts/schema_analyzer.py
==============================
SchemaEvolutionAnalyzer — Phase 3 of the Data Contract Enforcer.

Diffs consecutive schema snapshots, classifies every detected change
using the Confluent compatibility taxonomy, and generates a migration
impact report for breaking changes.

Usage:
    # Diff latest two snapshots for a contract
    python contracts/schema_analyzer.py \
        --contract-id week3-document-refinery-extractions

    # Diff two specific snapshot files
    python contracts/schema_analyzer.py \
        --snapshot-a schema_snapshots/week3-document-refinery-extractions/20260401_190000.yaml \
        --snapshot-b schema_snapshots/week3-document-refinery-extractions/20260402_190028.yaml

    # Inject a breaking change into a snapshot for testing
    python contracts/schema_analyzer.py \
        --contract-id week3-document-refinery-extractions \
        --inject-change

    # Output to specific file
    python contracts/schema_analyzer.py \
        --contract-id week3-document-refinery-extractions \
        --output validation_reports/schema_evolution_week3.json

Requirements:
    pip install pyyaml
"""

import argparse
import copy
import json
import os
import sys
import uuid
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

import yaml

# ---------------------------------------------------------------------------
# Constants — Change Classification Taxonomy
# ---------------------------------------------------------------------------

CHANGE_TAXONOMY = {
    "ADD_NULLABLE_FIELD": {
        "compatible": True,
        "severity": "LOW",
        "action": "None. Downstream consumers can ignore the new field.",
        "confluent_equivalent": "BACKWARD mode: allows",
    },
    "ADD_REQUIRED_FIELD": {
        "compatible": False,
        "severity": "CRITICAL",
        "action": (
            "Coordinate with all producers. Provide default or migration script. "
            "Block deployment until all producers updated."
        ),
        "confluent_equivalent": "BACKWARD mode: blocks",
    },
    "RENAME_FIELD": {
        "compatible": False,
        "severity": "CRITICAL",
        "action": (
            "Deprecation period with alias column. Notify all registry subscribers. "
            "Minimum 1 sprint before alias removal."
        ),
        "confluent_equivalent": "Confluent blocks. dbt: manual. Pact: consumer pact fails immediately.",
    },
    "WIDEN_TYPE": {
        "compatible": True,
        "severity": "LOW",
        "action": (
            "Validate no precision loss on existing data. "
            "Re-run statistical checks to confirm distribution unchanged."
        ),
        "confluent_equivalent": "FULL mode: allows. Most tools: pass silently.",
    },
    "NARROW_TYPE": {
        "compatible": False,
        "severity": "CRITICAL",
        "action": (
            "CRITICAL. Requires migration plan with rollback. "
            "Registry blast radius report mandatory. "
            "Statistical baseline must be re-established after migration. "
            "Example: float 0.0-1.0 -> int 0-100 is a CRITICAL narrowing change."
        ),
        "confluent_equivalent": "FORWARD mode: blocks. Great Expectations: catches via distribution check.",
    },
    "REMOVE_FIELD": {
        "compatible": False,
        "severity": "CRITICAL",
        "action": (
            "Two-sprint deprecation minimum. Each registry subscriber must acknowledge removal. "
            "No silent drops."
        ),
        "confluent_equivalent": "Confluent blocks. Pact: consumer pact fails if field was declared.",
    },
    "CHANGE_ENUM_ADDITIVE": {
        "compatible": True,
        "severity": "LOW",
        "action": "Notify all consumers of new values. No blocking required.",
        "confluent_equivalent": "BACKWARD: allows additions.",
    },
    "CHANGE_ENUM_REMOVAL": {
        "compatible": False,
        "severity": "HIGH",
        "action": (
            "Treat as breaking change. Blast radius report required. "
            "Each consumer must confirm the removed value is not in use."
        ),
        "confluent_equivalent": "BACKWARD: blocks removals.",
    },
    "CHANGE_RANGE_NARROWING": {
        "compatible": False,
        "severity": "HIGH",
        "action": (
            "Statistical baseline must be re-validated. "
            "Consumers relying on wider range will fail silently. "
            "Notify all registry subscribers."
        ),
        "confluent_equivalent": "Not handled by Confluent. Great Expectations: catches via distribution.",
    },
    "CHANGE_RANGE_WIDENING": {
        "compatible": True,
        "severity": "LOW",
        "action": "Re-run statistical checks. Consumers accepting wider range are unaffected.",
        "confluent_equivalent": "Not handled by Confluent.",
    },
    "CHANGE_PATTERN": {
        "compatible": False,
        "severity": "HIGH",
        "action": (
            "Notify all consumers. Validate existing data against new pattern. "
            "Provide migration script if existing data does not match new pattern."
        ),
        "confluent_equivalent": "Not handled by Confluent. Custom validation required.",
    },
    "CHANGE_FORMAT": {
        "compatible": False,
        "severity": "HIGH",
        "action": (
            "Notify all consumers. Format changes affect parsing logic. "
            "Coordinate with all downstream systems before deploying."
        ),
        "confluent_equivalent": "Not handled by Confluent.",
    },
    "NO_CHANGE": {
        "compatible": True,
        "severity": "LOW",
        "action": "No action required.",
        "confluent_equivalent": "N/A",
    },
}

# Type widening pairs (from_type, to_type) → compatible
WIDENING_PAIRS = {
    ("integer", "number"),
    ("integer", "string"),
    ("number", "string"),
}

DEFAULT_REPORT_DIR = "migration_impact_reports"
LINEAGE_GRAPH_PATH = "source_data/lineage_graph.json"
LINEAGE_SNAPSHOT_PATH = "outputs/week4/lineage_snapshots.jsonl"

CONTRACT_OUTPUT_MAP = {
    "week1-intent-records": "outputs/week1/intent_records.jsonl",
    "week2-verdict-records": "outputs/week2/verdicts.jsonl",
    "week3-document-refinery-extractions": "outputs/week3/extractions.jsonl",
    "week4-lineage-snapshots": "outputs/week4/lineage_snapshots.jsonl",
    "week5-event-records": "outputs/week5/events.jsonl",
    "langsmith-trace-records": "outputs/traces/runs.jsonl",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def ensure_dir(p: str):
    Path(p).mkdir(parents=True, exist_ok=True)

def load_snapshot(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)

def get_snapshots_for_contract(contract_id: str,
                               since: str = None) -> list:
    """
    Return sorted list of snapshot file paths for a contract.
    If since is provided (e.g. '7 days ago'), filter to snapshots
    created within that window.
    """
    snap_dir = Path(f"schema_snapshots/{contract_id}")
    if not snap_dir.exists():
        return []
    files = sorted(snap_dir.glob("*.yaml"))

    if since:
        # Parse 'N days ago' format
        cutoff = _parse_since(since)
        if cutoff:
            filtered = []
            for f in files:
                try:
                    # Timestamp is encoded in filename: YYYYMMDD_HHMMSS
                    stem = f.stem.split("_injected")[0]
                    ts = datetime.strptime(stem, "%Y%m%d_%H%M%S")
                    ts = ts.replace(tzinfo=timezone.utc)
                    if ts >= cutoff:
                        filtered.append(str(f))
                except ValueError:
                    filtered.append(str(f))
            return filtered

    return [str(f) for f in files]


def _parse_since(since_str: str):
    """
    Parse human-readable since strings like '7 days ago', '24 hours ago'.
    Returns a timezone-aware datetime cutoff or None if unparseable.
    """
    import re
    since_str = since_str.strip().lower()
    match = re.match(r"(\d+)\s*(day|hour|week)s?\s*ago", since_str)
    if not match:
        return None
    amount = int(match.group(1))
    unit = match.group(2)
    from datetime import timedelta
    now = datetime.now(timezone.utc)
    if unit == "day":
        return now - timedelta(days=amount)
    if unit == "hour":
        return now - timedelta(hours=amount)
    if unit == "week":
        return now - timedelta(weeks=amount)
    return None

def load_registry() -> dict:
    registry_path = "contract_registry/subscriptions.yaml"
    if not Path(registry_path).exists():
        return {"subscriptions": []}
    with open(registry_path) as f:
        return yaml.safe_load(f)

def get_registry_subscribers(contract_id: str) -> list:
    registry = load_registry()
    return [
        s for s in registry.get("subscriptions", [])
        if s.get("contract_id") == contract_id
    ]


def load_lineage_graph() -> dict:
    """Load lineage graph from canonical JSON, with snapshot fallback."""
    if Path(LINEAGE_GRAPH_PATH).exists():
        with open(LINEAGE_GRAPH_PATH) as f:
            return json.load(f)

    if Path(LINEAGE_SNAPSHOT_PATH).exists():
        with open(LINEAGE_SNAPSHOT_PATH) as f:
            snapshots = [json.loads(line) for line in f if line.strip()]
        return snapshots[-1] if snapshots else {}

    return {}


def _coerce_node_id(node: dict) -> str:
    return str(node.get("id") or node.get("node_id") or "").strip()


def _coerce_node_type(node: dict) -> str:
    return str(node.get("node_type") or node.get("type") or "unknown").strip().lower()


def _normalize_rel_path(path: str) -> str:
    if not path:
        return ""
    return str(path).replace("\\", "/").strip().lower()


def _extract_path_candidates(node: dict) -> set:
    candidates = set()
    for key in ["path_or_table", "name", "source_file", "id", "node_id"]:
        value = node.get(key)
        if isinstance(value, str) and value.strip():
            cleaned = _normalize_rel_path(value)
            candidates.add(cleaned)
            if cleaned.startswith("file::"):
                candidates.add(cleaned.replace("file::", "", 1))
            if "::" in cleaned:
                candidates.add(cleaned.split("::", 1)[0])
    return {c for c in candidates if c}


def _contract_source_candidates(contract_id: str, source_path: str) -> set:
    mapped = CONTRACT_OUTPUT_MAP.get(contract_id, "")
    base = source_path or mapped
    candidates = set()
    for raw in [source_path, mapped, base]:
        if not raw:
            continue
        norm = _normalize_rel_path(raw)
        candidates.add(norm)
        candidates.add(f"file::{norm}")
        candidates.add(Path(norm).name)
    return {c for c in candidates if c}


def _build_adjacency(lineage: dict) -> tuple:
    nodes = lineage.get("nodes", []) or []
    edges = lineage.get("edges", []) or []

    node_lookup = {}
    path_index = {}
    for node in nodes:
        node_id = _coerce_node_id(node)
        if not node_id:
            continue
        node_lookup[node_id] = node
        for p in _extract_path_candidates(node):
            path_index.setdefault(p, set()).add(node_id)

    forward_adj = {}
    reverse_adj = {}
    for edge in edges:
        src = str(edge.get("source", "")).strip()
        tgt = str(edge.get("target", "")).strip()
        if not src or not tgt:
            continue
        forward_adj.setdefault(src, set()).add(tgt)
        reverse_adj.setdefault(tgt, set()).add(src)

    return node_lookup, path_index, forward_adj, reverse_adj


def _bfs(start_nodes: set, adjacency: dict, max_hops: int = 6) -> list:
    visited = set(start_nodes)
    queue = deque((node_id, 0) for node_id in start_nodes)
    traversal = []

    while queue:
        node_id, depth = queue.popleft()
        if depth >= max_hops:
            continue
        for nxt in adjacency.get(node_id, set()):
            if nxt in visited:
                continue
            visited.add(nxt)
            nd = depth + 1
            traversal.append({"node_id": nxt, "depth": nd})
            queue.append((nxt, nd))

    return traversal


def compute_lineage_blast_radius(contract_id: str,
                                 old_snapshot: dict,
                                 new_snapshot: dict,
                                 breaking_changes: list) -> dict:
    """Build full lineage blast radius for this contract change."""
    lineage = load_lineage_graph()
    if not lineage:
        return {
            "lineage_available": False,
            "start_nodes": [],
            "downstream_nodes": [],
            "upstream_nodes": [],
            "summary": {
                "start_node_count": 0,
                "downstream_node_count": 0,
                "upstream_node_count": 0,
                "max_downstream_depth": 0,
                "max_upstream_depth": 0,
            },
        }

    node_lookup, path_index, forward_adj, reverse_adj = _build_adjacency(lineage)

    source_path = (new_snapshot or {}).get("source_path") or (old_snapshot or {}).get("source_path")
    source_candidates = _contract_source_candidates(contract_id, source_path)

    start_nodes = set()
    for candidate in source_candidates:
        start_nodes.update(path_index.get(candidate, set()))

    # Fallback: match by contract ID fragments in node paths.
    if not start_nodes:
        normalized_contract = _normalize_rel_path(contract_id).replace("-", "_")
        for path_value, node_ids in path_index.items():
            if normalized_contract in path_value:
                start_nodes.update(node_ids)

    downstream = _bfs(start_nodes, forward_adj)
    upstream = _bfs(start_nodes, reverse_adj)

    changed_fields = sorted({c.get("field", "") for c in breaking_changes if c.get("field")})

    def annotate(nodes_with_depth: list) -> list:
        annotated = []
        for entry in nodes_with_depth:
            node_id = entry["node_id"]
            node = node_lookup.get(node_id, {})
            annotated.append({
                "node_id": node_id,
                "node_type": _coerce_node_type(node),
                "name": node.get("name") or node.get("path_or_table") or node_id,
                "depth": entry["depth"],
                "potentially_impacted_fields": changed_fields,
            })
        return sorted(annotated, key=lambda x: (x["depth"], x["node_id"]))

    start_node_details = []
    for node_id in sorted(start_nodes):
        node = node_lookup.get(node_id, {})
        start_node_details.append({
            "node_id": node_id,
            "node_type": _coerce_node_type(node),
            "name": node.get("name") or node.get("path_or_table") or node_id,
            "matched_by": sorted(list(source_candidates)),
        })

    return {
        "lineage_available": True,
        "start_nodes": start_node_details,
        "downstream_nodes": annotate(downstream),
        "upstream_nodes": annotate(upstream),
        "summary": {
            "start_node_count": len(start_node_details),
            "downstream_node_count": len(downstream),
            "upstream_node_count": len(upstream),
            "max_downstream_depth": max([n["depth"] for n in downstream], default=0),
            "max_upstream_depth": max([n["depth"] for n in upstream], default=0),
        },
    }

# ---------------------------------------------------------------------------
# Change Detection
# ---------------------------------------------------------------------------

def classify_type_change(old_type: str, new_type: str) -> str:
    if old_type == new_type:
        return "NO_CHANGE"
    if (old_type, new_type) in WIDENING_PAIRS:
        return "WIDEN_TYPE"
    if (new_type, old_type) in WIDENING_PAIRS:
        return "NARROW_TYPE"
    return "NARROW_TYPE"  # unknown type change treated as narrowing


def classify_enum_change(old_enum: list, new_enum: list) -> str:
    old_set = set(str(v) for v in (old_enum or []))
    new_set = set(str(v) for v in (new_enum or []))
    removed = old_set - new_set
    added = new_set - old_set
    if removed:
        return "CHANGE_ENUM_REMOVAL"
    if added:
        return "CHANGE_ENUM_ADDITIVE"
    return "NO_CHANGE"


def classify_range_change(old_min, old_max, new_min, new_max) -> str:
    """Detect if range narrowed or widened."""
    # Narrowing: new range is tighter than old
    narrowed = False
    widened = False
    if old_min is not None and new_min is not None:
        if new_min > old_min:
            narrowed = True
        elif new_min < old_min:
            widened = True
    if old_max is not None and new_max is not None:
        if new_max < old_max:
            narrowed = True
        elif new_max > old_max:
            widened = True
    if narrowed:
        return "CHANGE_RANGE_NARROWING"
    if widened:
        return "CHANGE_RANGE_WIDENING"
    return "NO_CHANGE"


def diff_field(field_name: str, old_clause: dict, new_clause: dict) -> list:
    """
    Compare two field clauses and return list of detected changes.
    Each change is a dict with change_type, field, old_value, new_value.
    """
    changes = []

    # Type change
    old_type = old_clause.get("type", "string")
    new_type = new_clause.get("type", "string")
    if old_type != new_type:
        ct = classify_type_change(old_type, new_type)
        changes.append({
            "change_type": ct,
            "field": field_name,
            "property": "type",
            "old_value": old_type,
            "new_value": new_type,
        })

    # Required change (nullable → required is breaking)
    old_req = bool(old_clause.get("required", False))
    new_req = bool(new_clause.get("required", False))
    if not old_req and new_req:
        changes.append({
            "change_type": "ADD_REQUIRED_FIELD",
            "field": field_name,
            "property": "required",
            "old_value": False,
            "new_value": True,
        })

    # Enum change
    old_enum = old_clause.get("enum")
    new_enum = new_clause.get("enum")
    if old_enum is not None or new_enum is not None:
        ct = classify_enum_change(old_enum, new_enum)
        if ct != "NO_CHANGE":
            old_set = set(str(v) for v in (old_enum or []))
            new_set = set(str(v) for v in (new_enum or []))
            changes.append({
                "change_type": ct,
                "field": field_name,
                "property": "enum",
                "old_value": sorted(old_set),
                "new_value": sorted(new_set),
                "added": sorted(new_set - old_set),
                "removed": sorted(old_set - new_set),
            })

    # Range change
    old_min = old_clause.get("minimum")
    old_max = old_clause.get("maximum")
    new_min = new_clause.get("minimum")
    new_max = new_clause.get("maximum")
    if any(v is not None for v in [old_min, old_max, new_min, new_max]):
        ct = classify_range_change(old_min, old_max, new_min, new_max)
        if ct != "NO_CHANGE":
            changes.append({
                "change_type": ct,
                "field": field_name,
                "property": "range",
                "old_value": f"[{old_min}, {old_max}]",
                "new_value": f"[{new_min}, {new_max}]",
            })

    # Pattern change
    old_pat = old_clause.get("pattern")
    new_pat = new_clause.get("pattern")
    if old_pat != new_pat and (old_pat or new_pat):
        changes.append({
            "change_type": "CHANGE_PATTERN",
            "field": field_name,
            "property": "pattern",
            "old_value": old_pat,
            "new_value": new_pat,
        })

    # Format change
    old_fmt = old_clause.get("format")
    new_fmt = new_clause.get("format")
    if old_fmt != new_fmt and (old_fmt or new_fmt):
        changes.append({
            "change_type": "CHANGE_FORMAT",
            "field": field_name,
            "property": "format",
            "old_value": old_fmt,
            "new_value": new_fmt,
        })

    return changes


def diff_schemas(old_schema: dict, new_schema: dict) -> list:
    """
    Diff two schema dicts and return all detected changes.
    """
    changes = []
    old_fields = set(old_schema.keys())
    new_fields = set(new_schema.keys())

    # Removed fields
    for field in old_fields - new_fields:
        changes.append({
            "change_type": "REMOVE_FIELD",
            "field": field,
            "property": "existence",
            "old_value": old_schema[field],
            "new_value": None,
        })

    # Added fields
    for field in new_fields - old_fields:
        clause = new_schema[field]
        is_required = bool(clause.get("required", False))
        ct = "ADD_REQUIRED_FIELD" if is_required else "ADD_NULLABLE_FIELD"
        changes.append({
            "change_type": ct,
            "field": field,
            "property": "existence",
            "old_value": None,
            "new_value": clause,
        })

    # Modified fields
    for field in old_fields & new_fields:
        field_changes = diff_field(
            field,
            old_schema[field] if isinstance(old_schema[field], dict) else {},
            new_schema[field] if isinstance(new_schema[field], dict) else {},
        )
        changes.extend(field_changes)

    return changes

# ---------------------------------------------------------------------------
# Migration Impact Report
# ---------------------------------------------------------------------------

def build_migration_checklist(breaking_changes: list,
                               subscribers: list) -> list:
    """Generate ordered migration checklist from breaking changes."""
    checklist = []
    step = 1

    severity_rank = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
    ordered_breaking = sorted(
        breaking_changes,
        key=lambda c: (
            severity_rank.get(
                CHANGE_TAXONOMY.get(c.get("change_type", ""), {}).get("severity", "LOW"),
                99,
            ),
            c.get("field", ""),
            c.get("change_type", ""),
        ),
    )

    for change in ordered_breaking:
        field = change["field"]
        ct = change["change_type"]
        taxonomy = CHANGE_TAXONOMY.get(ct, {})

        checklist.append({
            "step": step,
            "action": taxonomy.get("action", "Review and coordinate with consumers."),
            "field": field,
            "change_type": ct,
            "responsible": "producer-team",
        })
        step += 1

    # Subscriber notification steps
    for sub in subscribers:
        checklist.append({
            "step": step,
            "action": (
                f"Notify {sub['subscriber_id']} ({sub.get('contact', 'unknown')}) "
                f"of breaking changes. Confirm fields_consumed: {sub.get('fields_consumed', [])}."
            ),
            "field": "all",
            "change_type": "NOTIFICATION",
            "responsible": sub.get("subscriber_team", "consumer-team"),
        })
        step += 1

    # Final steps
    checklist.extend([
        {
            "step": step,
            "action": "Re-run ContractGenerator to update schema snapshots after migration.",
            "field": "all",
            "change_type": "VALIDATION",
            "responsible": "week7-team",
        },
        {
            "step": step + 1,
            "action": "Re-run ValidationRunner on all affected contracts to confirm 0 FAIL results.",
            "field": "all",
            "change_type": "VALIDATION",
            "responsible": "week7-team",
        },
    ])

    return checklist


def build_rollback_plan(breaking_changes: list, old_snapshot_path: str) -> dict:
    """Generate rollback plan for breaking changes."""
    return {
        "rollback_to_snapshot": old_snapshot_path,
        "steps": [
            {
                "step": 1,
                "action": (
                    f"Revert producer code changes that caused: "
                    f"{[c['field'] for c in breaking_changes]}"
                ),
            },
            {
                "step": 2,
                "action": (
                    f"Re-deploy producer from last known good commit. "
                    f"Reference snapshot: {old_snapshot_path}"
                ),
            },
            {
                "step": 3,
                "action": "Re-run ValidationRunner to confirm all checks PASS.",
            },
            {
                "step": 4,
                "action": (
                    "Re-establish statistical baselines in schema_snapshots/baselines.json "
                    "if the rollback changes numeric distributions."
                ),
            },
        ],
        "estimated_recovery_time": "1-2 hours for code rollback + 30 min validation",
    }


def _value_to_text(value) -> str:
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, ensure_ascii=True)
    return str(value)


def _match_breaking_field(field_name: str, breaking_field_spec) -> tuple:
    if isinstance(breaking_field_spec, dict):
        candidate = str(breaking_field_spec.get("field", "")).strip()
        reason = str(breaking_field_spec.get("reason", "")).strip().replace("\n", " ")
    else:
        candidate = str(breaking_field_spec).strip()
        reason = ""

    if not candidate:
        return False, "", ""

    matched = (
        field_name == candidate
        or field_name.startswith(candidate + ".")
        or candidate.startswith(field_name + ".")
    )
    return matched, candidate, reason


def build_consumer_failure_analysis(subscribers: list,
                                    breaking_changes: list) -> list:
    """Build per-consumer failure mode analysis from registry + breaking fields."""
    analyses = []
    breaking_fields = sorted({c.get("field", "") for c in breaking_changes if c.get("field")})

    for sub in subscribers:
        matched_fields = set()
        impact_reasons = []
        for bf in sub.get("breaking_fields", []):
            for field in breaking_fields:
                is_match, candidate, reason = _match_breaking_field(field, bf)
                if is_match:
                    matched_fields.add(field)
                    if reason:
                        impact_reasons.append(f"{candidate}: {reason}")

        consumed_fields = [str(f).strip() for f in sub.get("fields_consumed", [])]
        for field in breaking_fields:
            for consumed in consumed_fields:
                if not consumed:
                    continue
                if field == consumed or field.startswith(consumed + ".") or consumed.startswith(field + "."):
                    matched_fields.add(field)

        mode = str(sub.get("validation_mode", "AUDIT")).upper()
        if matched_fields and mode == "ENFORCE":
            failure_mode = "Pipeline blocked"
            severity = "CRITICAL"
        elif matched_fields and mode == "WARN":
            failure_mode = "Warnings emitted; degraded data may flow"
            severity = "HIGH"
        elif matched_fields:
            failure_mode = "Silent corruption risk"
            severity = "HIGH"
        else:
            failure_mode = "Unaffected"
            severity = "LOW"

        analyses.append({
            "subscriber_id": sub.get("subscriber_id"),
            "subscriber_team": sub.get("subscriber_team"),
            "validation_mode": mode,
            "contact": sub.get("contact", ""),
            "fields_consumed": consumed_fields,
            "affected_fields": sorted(matched_fields),
            "failure_mode": failure_mode,
            "impact_severity": severity,
            "impact_reasons": impact_reasons,
        })

    return analyses


def build_migration_impact_report(contract_id: str, changes: list,
                                   old_path: str, new_path: str,
                                   old_snapshot: dict,
                                   new_snapshot: dict) -> dict:
    """Build the full migration impact report."""
    breaking = [c for c in changes if not CHANGE_TAXONOMY.get(
        c["change_type"], {}).get("compatible", True)]
    compatible = [c for c in changes if CHANGE_TAXONOMY.get(
        c["change_type"], {}).get("compatible", True)]

    subscribers = get_registry_subscribers(contract_id)

    consumer_analysis = build_consumer_failure_analysis(subscribers, breaking)
    lineage_blast_radius = compute_lineage_blast_radius(
        contract_id=contract_id,
        old_snapshot=old_snapshot,
        new_snapshot=new_snapshot,
        breaking_changes=breaking,
    )

    return {
        "report_id": str(uuid.uuid4()),
        "report_type": "migration_impact",
        "contract_id": contract_id,
        "generated_at": now_iso(),
        "snapshot_a": old_path,
        "snapshot_b": new_path,
        "total_changes": len(changes),
        "breaking_changes": len(breaking),
        "compatible_changes": len(compatible),
        "compatibility_verdict": "BREAKING" if breaking else "COMPATIBLE",
        "human_readable_diff": _format_diff(changes),
        "changes": changes,
        "lineage_blast_radius": lineage_blast_radius,
        "consumer_impact": consumer_analysis,
        "migration_checklist": build_migration_checklist(breaking, subscribers),
        "rollback_plan": build_rollback_plan(breaking, old_path) if breaking else None,
    }


def _format_diff(changes: list) -> str:
    """Human-readable diff summary."""
    if not changes:
        return "No changes detected."
    lines = []
    for c in changes:
        ct = c["change_type"]
        field = c["field"]
        prop = c.get("property", "")
        old_v = c.get("old_value", "")
        new_v = c.get("new_value", "")
        taxonomy = CHANGE_TAXONOMY.get(ct, {})
        compat = "✓ COMPATIBLE" if taxonomy.get("compatible") else "✗ BREAKING"
        lines.append(f"[{compat}] {ct} — field: {field}.{prop}")
        if old_v is not None and new_v is not None:
            lines.append(f"  before: {_value_to_text(old_v)}")
            lines.append(f"  after:  {_value_to_text(new_v)}")
        lines.append(f"  action: {taxonomy.get('action', 'Review required.')}")
        lines.append("")
    return "\n".join(lines)

# ---------------------------------------------------------------------------
# Snapshot injection for testing
# ---------------------------------------------------------------------------

def inject_breaking_change(contract_id: str) -> tuple:
    """
    Take the latest snapshot for a contract and inject a breaking change
    (confidence range narrowing: 0.0-1.0 → 0-100 scale).
    Returns (original_path, injected_path).
    """
    snapshots = get_snapshots_for_contract(contract_id)
    if not snapshots:
        print(f"  ✗ No snapshots found for {contract_id}")
        print(f"    Run: python contracts/generator.py --all first")
        sys.exit(1)

    latest = snapshots[-1]
    original = load_snapshot(latest)

    # Deep copy and inject breaking change
    modified = copy.deepcopy(original)
    schema = modified.get("schema", {})

    injected_fields = []

    # Inject 1: confidence scale change (0.0-1.0 → 0-100)
    conf_candidates = [
        k for k in schema.keys()
        if "confidence" in k.lower()
    ]
    for field in conf_candidates:
        if isinstance(schema[field], dict):
            schema[field]["minimum"] = 0
            schema[field]["maximum"] = 100
            schema[field]["type"] = "integer"
            injected_fields.append(field)

    # Inject 2: remove a field (source_hash or doc_id)
    remove_candidates = ["source_hash", "source_path"]
    for rc in remove_candidates:
        if rc in schema:
            del schema[rc]
            injected_fields.append(f"REMOVED:{rc}")
            break

    if not injected_fields:
        # Generic injection — change type of first string field
        for field, clause in schema.items():
            if isinstance(clause, dict) and clause.get("type") == "string":
                schema[field]["type"] = "integer"
                injected_fields.append(f"TYPE:{field}")
                break

    modified["schema"] = schema
    modified["snapshot_timestamp"] = now_iso()
    modified["_injection_note"] = (
        f"INJECTED breaking change for testing: {injected_fields}"
    )

    # Write injected snapshot
    snap_dir = f"schema_snapshots/{contract_id}"
    ensure_dir(snap_dir)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    injected_path = f"{snap_dir}/{ts}_injected.yaml"
    with open(injected_path, "w") as f:
        yaml.dump(modified, f, default_flow_style=False, sort_keys=False)

    print(f"  ✓ Injected breaking change: {injected_fields}")
    print(f"  ✓ Injected snapshot → {injected_path}")
    return latest, injected_path

# ---------------------------------------------------------------------------
# Main analysis pipeline
# ---------------------------------------------------------------------------

def analyze(contract_id: str = None,
            snapshot_a: str = None,
            snapshot_b: str = None,
            output_path: str = None,
            since: str = None) -> dict:
    """
    Run schema evolution analysis.
    Either provide contract_id (auto-detects latest two snapshots)
    or provide explicit snapshot_a and snapshot_b paths.
    since: optional filter e.g. '7 days ago'
    """
    print(f"\n→ SchemaEvolutionAnalyzer")

    # Resolve snapshot paths
    if contract_id and not (snapshot_a and snapshot_b):
        snapshots = get_snapshots_for_contract(contract_id, since=since)
        if len(snapshots) < 2:
            print(f"  ✗ Need at least 2 snapshots for {contract_id}")
            print(f"    Found: {len(snapshots)}")
            if since:
                print(f"    Since filter: '{since}' — try a wider window or omit --since")
            print(f"    Run generator twice or use --inject-change to create a second snapshot")
            sys.exit(1)
        snapshot_a = snapshots[-2]
        snapshot_b = snapshots[-1]
        print(f"  using latest two snapshots"
              + (f" (since: {since})" if since else "") + ":")
    else:
        contract_id = contract_id or "unknown"

    print(f"  snapshot A: {snapshot_a}")
    print(f"  snapshot B: {snapshot_b}")

    # Load snapshots
    snap_a = load_snapshot(snapshot_a)
    snap_b = load_snapshot(snapshot_b)
    schema_a = snap_a.get("schema", {})
    schema_b = snap_b.get("schema", {})

    print(f"  fields in A: {len(schema_a)}, fields in B: {len(schema_b)}")

    # Diff
    changes = diff_schemas(schema_a, schema_b)
    breaking = [c for c in changes if not CHANGE_TAXONOMY.get(
        c["change_type"], {}).get("compatible", True)]

    print(f"  total changes: {len(changes)}")
    print(f"  breaking changes: {len(breaking)}")

    if breaking:
        print(f"\n  ── Breaking Changes ──")
        for c in breaking:
            print(f"  ✗ [{c['change_type']}] {c['field']}.{c.get('property', '')}")
            print(f"    {c.get('old_value', '')} → {c.get('new_value', '')}")

    # Build full impact report
    report = build_migration_impact_report(
        contract_id, changes,
        snapshot_a, snapshot_b,
        snap_a, snap_b
    )

    # Determine output path
    if not output_path:
        ensure_dir(DEFAULT_REPORT_DIR)
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
        safe_id = contract_id.replace("-", "_")
        if breaking:
            output_path = f"{DEFAULT_REPORT_DIR}/migration_impact_{safe_id}_{ts}.json"
        else:
            output_path = f"{DEFAULT_REPORT_DIR}/schema_evolution_{safe_id}_{ts}.json"

    output_dir = os.path.dirname(output_path)
    if output_dir:
        ensure_dir(output_dir)
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2, default=str)

    print(f"\n  compatibility verdict: {report['compatibility_verdict']}")
    print(f"  report → {output_path}")

    if breaking:
        print(f"\n  ── Migration Checklist ({len(report['migration_checklist'])} steps) ──")
        for step in report["migration_checklist"][:5]:
            print(f"  Step {step['step']}: {step['action'][:80]}")

    return report

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="SchemaEvolutionAnalyzer — diff schema snapshots and classify changes"
    )
    parser.add_argument(
        "--contract-id", type=str,
        help="Contract ID — auto-detects latest two snapshots"
    )
    parser.add_argument(
        "--snapshot-a", type=str,
        help="Path to older snapshot YAML"
    )
    parser.add_argument(
        "--snapshot-b", type=str,
        help="Path to newer snapshot YAML"
    )
    parser.add_argument(
        "--since", type=str, default=None,
        help="Filter snapshots to those created within this window e.g. '7 days ago'"
    )
    parser.add_argument(
        "--output", type=str, default=None,
        help="Output path for migration impact report JSON"
    )
    parser.add_argument(
        "--inject-change", action="store_true",
        help="Inject a breaking change into the latest snapshot for testing"
    )
    args = parser.parse_args()

    if not args.contract_id and not (args.snapshot_a and args.snapshot_b):
        parser.print_help()
        sys.exit(1)

    if args.inject_change:
        if not args.contract_id:
            print("✗ --inject-change requires --contract-id")
            sys.exit(1)
        print(f"\n→ Injecting breaking change into {args.contract_id}")
        snap_a, snap_b = inject_breaking_change(args.contract_id)
        analyze(
            contract_id=args.contract_id,
            snapshot_a=snap_a,
            snapshot_b=snap_b,
            output_path=args.output,
            since=args.since
        )
    else:
        analyze(
            contract_id=args.contract_id,
            snapshot_a=args.snapshot_a,
            snapshot_b=args.snapshot_b,
            output_path=args.output,
            since=args.since
        )


if __name__ == "__main__":
    main()
