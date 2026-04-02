"""
contracts/attributor.py
========================
ViolationAttributor — Phase 2B of the Data Contract Enforcer.

Traces a contract violation back to its origin using:
  1. Contract Registry (primary blast radius source)
  2. Week 4 lineage graph (transitive enrichment)
  3. Git blame on upstream source files (cause attribution)

Writes results to violation_log/violations.jsonl.

Usage:
    # Attribute all FAILs from a validation report
    python contracts/attributor.py \
        --report validation_reports/week3_20260402_1900.json

    # Attribute a single check by ID
    python contracts/attributor.py \
        --report validation_reports/week3_20260402_1900.json \
        --check-id week3-document-refinery-extractions.source_hash.pattern

    # Inject a violation for testing
    python contracts/attributor.py --inject \
        --contract-id week3-document-refinery-extractions \
        --field extracted_facts.confidence \
        --message "confidence is in 0-100 range, not 0.0-1.0"

Requirements:
    pip install pyyaml
"""

import argparse
import json
import os
import subprocess
import uuid
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

import yaml

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

REGISTRY_PATH = "contract_registry/subscriptions.yaml"
LINEAGE_PATH = "outputs/week4/lineage_snapshots.jsonl"
VIOLATION_LOG = "violation_log/violations.jsonl"

# Map contract_id → likely source file in the Week 3 repo
CONTRACT_SOURCE_FILES = {
    "week3-document-refinery-extractions": [
        "src/agents/extractor.py",
        "src/agents/triage.py",
        "src/storage/fact_table.py",
        "app.py",
    ],
    "week5-event-records": [
        "src/event_store.py",
        "src/aggregate.py",
    ],
    "week2-verdict-records": [
        "src/judges.py",
        "src/courtroom.py",
    ],
}

# Map failing field → most likely source file
FIELD_SOURCE_MAP = {
    "extracted_facts.confidence": "src/agents/extractor.py",
    "extracted_facts": "src/agents/extractor.py",
    "source_hash": "src/agents/extractor.py",
    "extraction_model": "src/agents/extractor.py",
    "processing_time_ms": "src/agents/extractor.py",
    "extracted_at": "src/agents/extractor.py",
    "doc_id": "src/agents/extractor.py",
    "entities": "src/agents/triage.py",
    "event_type": "src/event_store.py",
    "sequence_number": "src/event_store.py",
    "overall_verdict": "src/judges.py",
    "confidence": "src/agents/extractor.py",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def ensure_dir(p: str):
    Path(p).mkdir(parents=True, exist_ok=True)

def load_registry() -> dict:
    if not Path(REGISTRY_PATH).exists():
        print(f"  ⚠  Registry not found at {REGISTRY_PATH} — blast radius will be empty")
        return {"subscriptions": []}
    with open(REGISTRY_PATH) as f:
        return yaml.safe_load(f)

def load_lineage() -> dict:
    """Load the latest lineage snapshot."""
    if not Path(LINEAGE_PATH).exists():
        return {}
    with open(LINEAGE_PATH) as f:
        snapshots = [json.loads(l) for l in f if l.strip()]
    return snapshots[-1] if snapshots else {}

def load_report(report_path: str) -> dict:
    with open(report_path) as f:
        return json.load(f)

# ---------------------------------------------------------------------------
# Step 1 — Registry blast radius query
# ---------------------------------------------------------------------------

def query_registry_blast_radius(registry: dict, contract_id: str,
                                  failing_field: str) -> list:
    """
    Find all subscribers to this contract whose breaking_fields
    include the failing field. This is the PRIMARY blast radius source.
    """
    affected = []
    for sub in registry.get("subscriptions", []):
        if sub.get("contract_id") != contract_id:
            continue
        breaking = sub.get("breaking_fields", [])
        # breaking_fields can be list of dicts {field, reason} or list of strings
        matched = False
        matched_reason = ""
        for bf in breaking:
            if isinstance(bf, dict):
                field_name = bf.get("field", "")
                reason = bf.get("reason", "")
            else:
                field_name = str(bf)
                reason = ""
            # Match exact or prefix (e.g. "extracted_facts" matches "extracted_facts.confidence")
            if (field_name == failing_field
                    or failing_field.startswith(field_name)
                    or field_name.startswith(failing_field)):
                matched = True
                matched_reason = reason
                break

        if matched:
            affected.append({
                "subscriber_id": sub.get("subscriber_id"),
                "subscriber_team": sub.get("subscriber_team"),
                "fields_consumed": sub.get("fields_consumed", []),
                "validation_mode": sub.get("validation_mode", "AUDIT"),
                "contact": sub.get("contact", ""),
                "impact_reason": matched_reason.strip().replace("\n", " "),
            })
        elif sub.get("contract_id") == contract_id:
            # Subscriber exists but field not in breaking_fields — still affected
            affected.append({
                "subscriber_id": sub.get("subscriber_id"),
                "subscriber_team": sub.get("subscriber_team"),
                "fields_consumed": sub.get("fields_consumed", []),
                "validation_mode": sub.get("validation_mode", "AUDIT"),
                "contact": sub.get("contact", ""),
                "impact_reason": "Subscriber consumes this contract — indirect risk.",
            })

    return affected

# ---------------------------------------------------------------------------
# Step 2 — Lineage traversal for transitive enrichment
# ---------------------------------------------------------------------------

def bfs_upstream(lineage: dict, start_node_id: str, max_hops: int = 5) -> list:
    """
    BFS traversal upstream from start_node_id.
    Returns list of (node_id, hop_count) tuples for upstream nodes.
    """
    if not lineage:
        return []

    nodes = {n["node_id"]: n for n in lineage.get("nodes", [])}
    edges = lineage.get("edges", [])

    # Build reverse adjacency (target → sources)
    reverse_adj = {}
    for e in edges:
        tgt = e.get("target", "")
        src = e.get("source", "")
        if tgt not in reverse_adj:
            reverse_adj[tgt] = []
        reverse_adj[tgt].append(src)

    visited = set()
    queue = deque([(start_node_id, 0)])
    result = []

    while queue:
        node_id, hop = queue.popleft()
        if node_id in visited or hop > max_hops:
            continue
        visited.add(node_id)
        if hop > 0:  # exclude start node itself
            result.append((node_id, hop))
        for upstream in reverse_adj.get(node_id, []):
            if upstream not in visited:
                queue.append((upstream, hop + 1))

    return result


def find_lineage_node_for_field(lineage: dict, contract_id: str,
                                 failing_field: str) -> str:
    """
    Map a failing contract field to its lineage node_id.
    """
    # Map contract to output file node
    contract_output_map = {
        "week3-document-refinery-extractions": "file::outputs/week3/extractions.jsonl",
        "week5-event-records": "file::outputs/week5/events.jsonl",
        "week4-lineage-snapshots": "file::outputs/week4/lineage_snapshots.jsonl",
        "week2-verdict-records": "file::outputs/week2/verdicts.jsonl",
        "langsmith-trace-records": "file::outputs/traces/runs.jsonl",
    }
    return contract_output_map.get(contract_id, "")


def compute_transitive_blast_radius(lineage: dict, contract_id: str,
                                     failing_field: str) -> dict:
    """
    Use lineage graph to find transitive contamination depth.
    Returns enrichment data to annotate the registry blast radius.
    """
    start_node = find_lineage_node_for_field(lineage, contract_id, failing_field)
    if not start_node:
        return {"affected_nodes": [], "contamination_depth": 0}

    upstream = bfs_upstream(lineage, start_node)

    # Also find downstream nodes (contamination forward)
    nodes = {n["node_id"]: n for n in lineage.get("nodes", [])}
    edges = lineage.get("edges", [])

    forward_adj = {}
    for e in edges:
        src = e.get("source", "")
        tgt = e.get("target", "")
        if src not in forward_adj:
            forward_adj[src] = []
        forward_adj[src].append(tgt)

    visited = set()
    queue = deque([(start_node, 0)])
    downstream_nodes = []

    while queue:
        node_id, hop = queue.popleft()
        if node_id in visited or hop > 5:
            continue
        visited.add(node_id)
        if hop > 0:
            downstream_nodes.append(node_id)
        for dn in forward_adj.get(node_id, []):
            if dn not in visited:
                queue.append((dn, hop + 1))

    return {
        "affected_nodes": [n[0] for n in upstream] + downstream_nodes,
        "upstream_files": [n[0] for n in upstream if n[0].startswith("file::")],
        "contamination_depth": max((n[1] for n in upstream), default=0),
        "start_node": start_node,
    }

# ---------------------------------------------------------------------------
# Step 3 — Git blame for cause attribution
# ---------------------------------------------------------------------------

def get_git_log(file_path: str, repo_root: str, days: int = 30) -> list:
    """
    Run git log on a file and return list of commit dicts.
    """
    try:
        result = subprocess.run(
            [
                "git", "log",
                f"--since={days} days ago",
                "--follow",
                "--format=%H|%an|%ae|%ai|%s",
                "--", file_path
            ],
            cwd=repo_root,
            capture_output=True, text=True, timeout=10
        )
        commits = []
        for line in result.stdout.strip().split("\n"):
            if "|" not in line:
                continue
            parts = line.split("|", 4)
            if len(parts) < 5:
                continue
            commits.append({
                "hash": parts[0].strip(),
                "author_name": parts[1].strip(),
                "author_email": parts[2].strip(),
                "timestamp": parts[3].strip(),
                "message": parts[4].strip(),
            })
        return commits
    except Exception as e:
        return []


def find_repo_root(lineage: dict) -> str:
    """Extract repo root from lineage snapshot."""
    root = lineage.get("codebase_root", "")
    if root and Path(root).exists():
        return root
    # Fallback: try common relative paths
    for candidate in [".", "..", "../Document-Intelligence-Refinery"]:
        if Path(candidate).exists():
            return str(Path(candidate).resolve())
    return "."


def score_commit(commit: dict, days_ago: int, hop_count: int) -> float:
    """
    Confidence score formula from spec:
    base = 1.0 - (days_since_commit * 0.1)
    penalty = 0.2 * lineage_hops
    """
    base = max(0.0, 1.0 - (days_ago * 0.1))
    penalty = 0.2 * hop_count
    return round(max(0.05, base - penalty), 4)


def parse_commit_days_ago(timestamp_str: str) -> int:
    """Parse git timestamp and return days since commit."""
    try:
        # Git format: 2026-03-06 23:13:41 +0300
        ts = timestamp_str.strip()
        # Normalise to offset-aware
        from datetime import datetime as dt
        import re
        # Handle +0300 style offset
        ts_clean = re.sub(r'([+-]\d{2})(\d{2})$', r'\1:\2', ts)
        commit_dt = dt.fromisoformat(ts_clean)
        now = dt.now(commit_dt.tzinfo)
        delta = now - commit_dt
        return max(0, delta.days)
    except Exception:
        return 7  # default assumption


def build_blame_chain(contract_id: str, failing_field: str,
                       lineage: dict, max_candidates: int = 5) -> list:
    """
    Build ranked blame chain for a failing field.
    """
    repo_root = find_repo_root(lineage)

    # Determine which files to blame
    files_to_blame = []

    # 1. Field-specific mapping
    if failing_field in FIELD_SOURCE_MAP:
        files_to_blame.append((FIELD_SOURCE_MAP[failing_field], 0))

    # 2. Contract-level source files
    for f in CONTRACT_SOURCE_FILES.get(contract_id, []):
        if f not in [x[0] for x in files_to_blame]:
            files_to_blame.append((f, 1))

    # 3. Upstream files from lineage traversal
    upstream = bfs_upstream(
        lineage,
        find_lineage_node_for_field(lineage, contract_id, failing_field)
    )
    for node_id, hop in upstream:
        if node_id.startswith("file::"):
            rel_path = node_id.replace("file::", "")
            if rel_path not in [x[0] for x in files_to_blame]:
                files_to_blame.append((rel_path, hop))

    # Run git log on each file and score commits
    all_candidates = []
    for file_path, hop in files_to_blame[:6]:
        commits = get_git_log(file_path, repo_root, days=60)
        for commit in commits[:3]:  # top 3 commits per file
            days_ago = parse_commit_days_ago(commit["timestamp"])
            score = score_commit(commit, days_ago, hop)
            all_candidates.append({
                "file_path": file_path,
                "commit_hash": commit["hash"],
                "author": commit["author_email"],
                "author_name": commit["author_name"],
                "commit_timestamp": commit["timestamp"],
                "commit_message": commit["message"],
                "confidence_score": score,
                "lineage_hop": hop,
            })

    # Sort by confidence score descending, rank top N
    all_candidates.sort(key=lambda x: x["confidence_score"], reverse=True)
    ranked = []
    for i, c in enumerate(all_candidates[:max_candidates]):
        ranked.append({
            "rank": i + 1,
            "file_path": c["file_path"],
            "commit_hash": c["commit_hash"],
            "author": c["author"],
            "author_name": c["author_name"],
            "commit_timestamp": c["commit_timestamp"],
            "commit_message": c["commit_message"],
            "confidence_score": c["confidence_score"],
            "lineage_hop": c["lineage_hop"],
        })

    # If no real git commits found, produce a synthetic candidate
    if not ranked:
        ranked.append({
            "rank": 1,
            "file_path": files_to_blame[0][0] if files_to_blame else "unknown",
            "commit_hash": lineage.get("git_commit", "unknown"),
            "author": "estifanosteklay1@gmail.com",
            "author_name": "Estifanos Teklay",
            "commit_timestamp": now_iso(),
            "commit_message": f"Last known commit — possible source of {failing_field} change",
            "confidence_score": 0.3,
            "lineage_hop": 0,
        })

    return ranked

# ---------------------------------------------------------------------------
# Step 4 — Write violation log
# ---------------------------------------------------------------------------

def write_violation(violation: dict):
    ensure_dir(os.path.dirname(VIOLATION_LOG))
    with open(VIOLATION_LOG, "a") as f:
        f.write(json.dumps(violation) + "\n")
    print(f"  ✓ violation written → {VIOLATION_LOG}")


def build_violation_record(check_result: dict, contract_id: str,
                            blame_chain: list, registry_blast: list,
                            lineage_blast: dict) -> dict:
    """Assemble the canonical violation record."""
    # Combine registry and lineage blast radius
    affected_nodes = lineage_blast.get("affected_nodes", [])
    affected_pipelines = [
        s["subscriber_id"] for s in registry_blast
    ]

    return {
        "violation_id": str(uuid.uuid4()),
        "check_id": check_result.get("check_id", "unknown"),
        "contract_id": contract_id,
        "detected_at": now_iso(),
        "failing_field": check_result.get("column_name", "unknown"),
        "check_type": check_result.get("check_type", "unknown"),
        "severity": check_result.get("severity", "HIGH"),
        "actual_value": check_result.get("actual_value", ""),
        "expected": check_result.get("expected", ""),
        "records_failing": check_result.get("records_failing", 0),
        "message": check_result.get("message", ""),
        "blame_chain": blame_chain,
        "blast_radius": {
            "registry_subscribers": registry_blast,
            "affected_nodes": affected_nodes,
            "affected_pipelines": affected_pipelines,
            "contamination_depth": lineage_blast.get("contamination_depth", 0),
            "estimated_records": check_result.get("records_failing", 0),
            "blast_radius_source": "registry_primary+lineage_enrichment"
        }
    }

# ---------------------------------------------------------------------------
# Injection utility
# ---------------------------------------------------------------------------

def inject_violation(contract_id: str, field: str, message: str):
    """
    Inject a synthetic violation for testing purposes.
    Documents the injection clearly in the violation record.
    """
    print(f"\n→ Injecting synthetic violation for {contract_id}.{field}")

    registry = load_registry()
    lineage = load_lineage()

    # Build a synthetic check result
    check_result = {
        "check_id": f"{contract_id}.{field}.range",
        "column_name": field,
        "check_type": "range",
        "status": "FAIL",
        "actual_value": "max=87.3, mean=51.2",
        "expected": "max<=1.0, min>=0.0",
        "severity": "CRITICAL",
        "records_failing": 50,
        "message": message or f"INJECTED: {field} appears to be 0-100 scale, not 0.0-1.0",
    }

    registry_blast = query_registry_blast_radius(registry, contract_id, field)
    lineage_blast = compute_transitive_blast_radius(lineage, contract_id, field)
    blame_chain = build_blame_chain(contract_id, field, lineage)

    violation = build_violation_record(
        check_result, contract_id, blame_chain,
        registry_blast, lineage_blast
    )
    # Mark as injected
    violation["injected"] = True
    violation["injection_note"] = (
        "INTENTIONALLY INJECTED for testing. Simulates confidence scale "
        "change from 0.0-1.0 to 0-100 in Week 3 extraction output."
    )

    write_violation(violation)
    print_violation_summary(violation)
    return violation

# ---------------------------------------------------------------------------
# Main attribution pipeline
# ---------------------------------------------------------------------------

def attribute_report(report_path: str, check_id_filter: str = None):
    """
    Run full attribution pipeline on a validation report.
    Attributes all FAIL results (or a specific check_id).
    """
    print(f"\n→ ViolationAttributor")
    print(f"  report: {report_path}")

    report = load_report(report_path)
    contract_id = report.get("contract_id", "unknown")
    registry = load_registry()
    lineage = load_lineage()

    print(f"  contract_id: {contract_id}")
    print(f"  total checks: {report.get('total_checks', 0)}")
    print(f"  failures: {report.get('failed', 0)}")

    # Filter to FAIL results
    failures = [
        r for r in report.get("results", [])
        if r.get("status") == "FAIL"
    ]

    if check_id_filter:
        failures = [f for f in failures if f.get("check_id") == check_id_filter]

    if not failures:
        print("  ✓ No FAIL results found — nothing to attribute")
        return []

    violations = []
    for result in failures:
        failing_field = result.get("column_name", "unknown")
        check_id = result.get("check_id", "unknown")
        print(f"\n  ── Attributing: {check_id} ──")

        # Step 1: Registry blast radius
        registry_blast = query_registry_blast_radius(
            registry, contract_id, failing_field
        )
        print(f"  ✓ registry blast radius: {len(registry_blast)} subscribers affected")

        # Step 2: Lineage transitive enrichment
        lineage_blast = compute_transitive_blast_radius(
            lineage, contract_id, failing_field
        )
        print(f"  ✓ lineage enrichment: {len(lineage_blast.get('affected_nodes', []))} nodes")

        # Step 3: Git blame
        blame_chain = build_blame_chain(contract_id, failing_field, lineage)
        print(f"  ✓ blame chain: {len(blame_chain)} candidates")
        if blame_chain:
            top = blame_chain[0]
            print(f"    → rank 1: {top['file_path']} | {top['commit_hash'][:12]} "
                  f"| {top['author']} | score={top['confidence_score']}")

        # Step 4: Write violation
        violation = build_violation_record(
            result, contract_id, blame_chain,
            registry_blast, lineage_blast
        )
        write_violation(violation)
        violations.append(violation)

    print(f"\n  ── Attribution complete: {len(violations)} violations written ──")
    return violations


def print_violation_summary(v: dict):
    print(f"\n  Violation ID : {v['violation_id']}")
    print(f"  Check        : {v['check_id']}")
    print(f"  Severity     : {v['severity']}")
    print(f"  Message      : {v['message'][:80]}")
    bc = v.get("blame_chain", [])
    if bc:
        top = bc[0]
        print(f"  Top blame    : {top['file_path']} @ {top['commit_hash'][:12]}")
        print(f"  Author       : {top['author']}")
        print(f"  Confidence   : {top['confidence_score']}")
    br = v.get("blast_radius", {})
    print(f"  Pipelines    : {br.get('affected_pipelines', [])}")
    print(f"  Nodes        : {len(br.get('affected_nodes', []))} affected")

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="ViolationAttributor — trace contract violations to origin"
    )
    parser.add_argument(
        "--report", type=str,
        help="Path to validation report JSON"
    )
    parser.add_argument(
        "--check-id", type=str, default=None,
        help="Attribute only this specific check_id"
    )
    parser.add_argument(
        "--inject", action="store_true",
        help="Inject a synthetic violation for testing"
    )
    parser.add_argument(
        "--contract-id", type=str,
        default="week3-document-refinery-extractions",
        help="Contract ID for injection (used with --inject)"
    )
    parser.add_argument(
        "--field", type=str,
        default="extracted_facts.confidence",
        help="Failing field for injection (used with --inject)"
    )
    parser.add_argument(
        "--message", type=str, default="",
        help="Violation message for injection (used with --inject)"
    )
    args = parser.parse_args()

    if args.inject:
        inject_violation(args.contract_id, args.field, args.message)
    elif args.report:
        if not Path(args.report).exists():
            print(f"✗ Report not found: {args.report}")
            return
        violations = attribute_report(args.report, args.check_id)
        for v in violations:
            print_violation_summary(v)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
