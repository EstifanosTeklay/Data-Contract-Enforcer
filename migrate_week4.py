"""
migrate_week3_to_lineage.py
============================
Builds a canonical lineage_snapshot from the Week 3 Document Intelligence
Refinery codebase by:
  1. Walking all .py files in the repo
  2. Parsing import statements to build edges
  3. Reading git log for the latest commit SHA
  4. Writing outputs/week4/lineage_snapshots.jsonl

Usage (run from your Week 7 repo root):
    python migrate_week3_to_lineage.py --repo /path/to/Document-Intelligence-Refinery

Requirements:
    pip install pyyaml
"""

import ast
import argparse
import json
import os
import re
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def ensure_dir(p: str):
    Path(p).mkdir(parents=True, exist_ok=True)


def get_git_commit(repo_root: str) -> str:
    """Get the latest commit SHA from the repo."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            capture_output=True, text=True
        )
        sha = result.stdout.strip()
        if len(sha) == 40:
            return sha
    except Exception:
        pass
    return "a" * 40  # fallback


def get_git_last_modified(repo_root: str, rel_path: str) -> str:
    """Get last commit timestamp for a specific file."""
    try:
        result = subprocess.run(
            ["git", "log", "-1", "--format=%aI", "--", rel_path],
            cwd=repo_root,
            capture_output=True, text=True
        )
        ts = result.stdout.strip()
        if ts:
            return ts
    except Exception:
        pass
    return now_iso()


def infer_purpose(rel_path: str, file_content: str) -> str:
    """
    Infer a one-sentence purpose from the file path and first docstring.
    Falls back to path-based heuristic.
    """
    # Try to extract module docstring
    try:
        tree = ast.parse(file_content)
        docstring = ast.get_docstring(tree)
        if docstring:
            first_line = docstring.strip().split("\n")[0]
            if len(first_line) > 10:
                return first_line[:120]
    except SyntaxError:
        pass

    # Path-based heuristics
    name = Path(rel_path).stem.lower()
    heuristics = {
        "extractor": "Extracts structured facts from documents using LLM strategies",
        "chunker": "Splits documents into semantically coherent chunks for processing",
        "chunk_validator": "Validates chunk quality against 5 inviolable chunking rules",
        "triage": "Routes documents to the appropriate extraction strategy",
        "audit_mode": "Verifies extracted claims against source document evidence",
        "query_agent": "Handles semantic and structured queries over extracted content",
        "indexer": "Builds and maintains the document page index",
        "fact_table": "Stores and retrieves numerical facts using SQLite",
        "vector_store": "TF-IDF vector store for semantic search over LDU metadata",
        "strategy_a": "Extraction strategy A for simple single-column documents",
        "strategy_b": "Extraction strategy B for multi-column documents",
        "strategy_c": "Extraction strategy C for complex layout documents",
        "document_profile": "Data model for document layout and complexity profile",
        "extracted_document": "Data model for the canonical extraction output record",
        "ldu": "Logical Document Unit model for structured content representation",
        "pageindex": "Page index model for document section hierarchy",
        "provenance": "Provenance tracking model for fact attribution",
        "routing": "Routing decision model for strategy selection",
        "app": "Main application entry point for the Document Intelligence Refinery",
        "run_phase0": "Phase 0 runner for document profiling and strategy selection",
        "config": "Configuration loader for model endpoints and processing parameters",
        "base": "Abstract base class for extraction strategies",
    }
    return heuristics.get(name, f"Module: {rel_path}")


def infer_language(rel_path: str) -> str:
    ext_map = {
        ".py": "python", ".sql": "sql", ".yaml": "yaml",
        ".yml": "yaml", ".json": "json", ".md": "markdown"
    }
    return ext_map.get(Path(rel_path).suffix.lower(), "unknown")


def node_type_for(rel_path: str) -> str:
    """Map file path to canonical node type."""
    p = rel_path.lower()
    if "model" in p:
        return "MODEL"
    if "storage" in p or "table" in p or "store" in p:
        return "TABLE"
    if "agent" in p or "strategy" in p:
        return "PIPELINE"
    if "app" in p or "run_" in p:
        return "SERVICE"
    return "FILE"


# ---------------------------------------------------------------------------
# Import analysis
# ---------------------------------------------------------------------------

def parse_imports(file_path: str, repo_root: str) -> list:
    """
    Parse a Python file and return list of local module paths it imports.
    Returns relative paths from repo root.
    """
    try:
        content = Path(file_path).read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return []

    imports = []
    try:
        tree = ast.parse(content)
    except SyntaxError:
        return []

    repo_path = Path(repo_root)

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.module and node.level == 0:
                # Absolute import — check if it resolves to a local file
                module_path = node.module.replace(".", "/")
                candidates = [
                    repo_path / f"{module_path}.py",
                    repo_path / module_path / "__init__.py",
                ]
                for c in candidates:
                    if c.exists():
                        imports.append(str(c.relative_to(repo_path)))
            elif node.level > 0:
                # Relative import
                current_dir = Path(file_path).parent
                for _ in range(node.level - 1):
                    current_dir = current_dir.parent
                if node.module:
                    module_path = node.module.replace(".", "/")
                    candidates = [
                        current_dir / f"{module_path}.py",
                        current_dir / module_path / "__init__.py",
                    ]
                else:
                    candidates = [current_dir / "__init__.py"]
                for c in candidates:
                    if c.exists():
                        try:
                            imports.append(str(c.relative_to(repo_path)))
                        except ValueError:
                            pass

        elif isinstance(node, ast.Import):
            for alias in node.names:
                module_path = alias.name.replace(".", "/")
                candidates = [
                    repo_path / f"{module_path}.py",
                    repo_path / module_path / "__init__.py",
                ]
                for c in candidates:
                    if c.exists():
                        imports.append(str(c.relative_to(repo_path)))

    return list(set(imports))


# ---------------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------------

def build_lineage_snapshot(repo_root: str, output_path: str):
    repo_root = str(Path(repo_root).resolve())
    print(f"\n→ Building lineage snapshot from: {repo_root}")

    # Collect all Python files
    py_files = []
    for root, dirs, files in os.walk(repo_root):
        # Skip hidden dirs, __pycache__, venv, .git
        dirs[:] = [d for d in dirs if not d.startswith(".")
                   and d not in ("__pycache__", "venv", "env", "node_modules")]
        for f in files:
            if f.endswith(".py"):
                full = os.path.join(root, f)
                rel = os.path.relpath(full, repo_root)
                rel = rel.replace("\\", "/")  # normalise Windows paths
                py_files.append(rel)

    print(f"  ✓ found {len(py_files)} Python files")

    # Get git info
    git_commit = get_git_commit(repo_root)
    print(f"  ✓ git commit: {git_commit[:12]}...")

    # Build nodes
    nodes = []
    node_ids = set()

    for rel_path in py_files:
        full_path = os.path.join(repo_root, rel_path)
        try:
            content = Path(full_path).read_text(encoding="utf-8", errors="ignore")
        except Exception:
            content = ""

        node_id = f"file::{rel_path}"
        node_ids.add(node_id)
        last_mod = get_git_last_modified(repo_root, rel_path)

        nodes.append({
            "node_id": node_id,
            "type": node_type_for(rel_path),
            "label": Path(rel_path).name,
            "metadata": {
                "path": rel_path,
                "language": infer_language(rel_path),
                "purpose": infer_purpose(rel_path, content),
                "last_modified": last_mod
            }
        })

    # Add key output datasets as nodes
    output_nodes = [
        {
            "node_id": "file::outputs/week3/extractions.jsonl",
            "type": "FILE",
            "label": "extractions.jsonl",
            "metadata": {
                "path": "outputs/week3/extractions.jsonl",
                "language": "json",
                "purpose": "Canonical extraction output — one record per processed document",
                "last_modified": now_iso()
            }
        },
        {
            "node_id": "file::outputs/week3/extraction_ledger.jsonl",
            "type": "FILE",
            "label": "extraction_ledger.jsonl",
            "metadata": {
                "path": "outputs/week3/extraction_ledger.jsonl",
                "language": "json",
                "purpose": "Extraction ledger — per-document processing metadata and strategy trace",
                "last_modified": now_iso()
            }
        }
    ]
    for on in output_nodes:
        node_ids.add(on["node_id"])
        nodes.append(on)

    print(f"  ✓ built {len(nodes)} nodes")

    # Build edges from import analysis
    edges = []
    seen_edges = set()

    for rel_path in py_files:
        full_path = os.path.join(repo_root, rel_path)
        imported = parse_imports(full_path, repo_root)
        src_id = f"file::{rel_path}"

        for imp in imported:
            tgt_id = f"file::{imp}"
            if tgt_id not in node_ids:
                continue
            key = (src_id, tgt_id)
            if key in seen_edges:
                continue
            seen_edges.add(key)
            edges.append({
                "source": src_id,
                "target": tgt_id,
                "relationship": "IMPORTS",
                "confidence": 0.97
            })

    # Add key production edges manually
    # extractor.py PRODUCES extractions.jsonl
    key_edges = [
        {
            "source": "file::src/agents/extractor.py",
            "target": "file::outputs/week3/extractions.jsonl",
            "relationship": "PRODUCES",
            "confidence": 0.99
        },
        {
            "source": "file::app.py",
            "target": "file::outputs/week3/extraction_ledger.jsonl",
            "relationship": "PRODUCES",
            "confidence": 0.99
        },
        {
            "source": "file::src/agents/extractor.py",
            "target": "file::outputs/week3/extraction_ledger.jsonl",
            "relationship": "WRITES",
            "confidence": 0.95
        },
        {
            "source": "file::src/storage/fact_table.py",
            "target": "file::outputs/week3/extractions.jsonl",
            "relationship": "WRITES",
            "confidence": 0.90
        },
    ]
    for e in key_edges:
        key = (e["source"], e["target"])
        if key not in seen_edges:
            # Only add if both nodes exist
            if e["source"] in node_ids and e["target"] in node_ids:
                seen_edges.add(key)
                edges.append(e)

    print(f"  ✓ built {len(edges)} edges")

    # Build snapshot
    snapshot = {
        "snapshot_id": str(uuid.uuid4()),
        "codebase_root": repo_root,
        "git_commit": git_commit,
        "nodes": nodes,
        "edges": edges,
        "captured_at": now_iso()
    }

    # Write output
    ensure_dir(os.path.dirname(output_path))
    with open(output_path, "w") as f:
        f.write(json.dumps(snapshot) + "\n")

    print(f"  ✓ snapshot written → {output_path}")
    print(f"  ✓ nodes: {len(nodes)}, edges: {len(edges)}, commit: {git_commit[:12]}")
    return snapshot


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Build canonical lineage snapshot from Week 3 repo"
    )
    parser.add_argument(
        "--repo", required=True,
        help="Path to Week 3 Document Intelligence Refinery repo root"
    )
    parser.add_argument(
        "--output", default="outputs/week4/lineage_snapshots.jsonl",
        help="Output path (default: outputs/week4/lineage_snapshots.jsonl)"
    )
    args = parser.parse_args()

    build_lineage_snapshot(args.repo, args.output)
    print("\n=== Done. Re-run contracts/generator.py --all to update contracts. ===")


if __name__ == "__main__":
    main()
