"""
contracts/generator.py
======================
ContractGenerator — Phase 1 of the Data Contract Enforcer.

Reads canonical JSONL outputs from outputs/ directories and produces:
  - Bitol-compatible YAML contracts in generated_contracts/
  - dbt schema.yml counterparts in generated_contracts/
  - Timestamped schema snapshots in schema_snapshots/

Usage:
    python contracts/generator.py --source outputs/week3/extractions.jsonl \
                                   --output generated_contracts/

    python contracts/generator.py --all   # runs all known sources

Requirements:
    pip install pandas pyyaml anthropic
"""

import argparse
import hashlib
import json
import math
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml

# Optional: ydata-profiling for deep stats (graceful fallback if not installed)
try:
    from ydata_profiling import ProfileReport
    HAS_PROFILING = True
except ImportError:
    HAS_PROFILING = False

try:
    import anthropic
    HAS_ANTHROPIC = True
except ImportError:
    HAS_ANTHROPIC = False

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

KNOWN_SOURCES = {
    "week1": "outputs/week1/intent_records.jsonl",
    "week2": "outputs/week2/verdicts.jsonl",
    "week3": "outputs/week3/extractions.jsonl",
    "week4": "outputs/week4/lineage_snapshots.jsonl",
    "week5": "outputs/week5/events.jsonl",
    "traces": "outputs/traces/runs.jsonl",
}

CONTRACT_IDS = {
    "week1": "week1-intent-records",
    "week2": "week2-verdict-records",
    "week3": "week3-document-refinery-extractions",
    "week4": "week4-lineage-snapshots",
    "week5": "week5-event-records",
    "traces": "langsmith-trace-records",
}

CONTRACT_TITLES = {
    "week1": "Week 1 Intent-Code Correlator — Intent Records",
    "week2": "Week 2 Digital Courtroom — Verdict Records",
    "week3": "Week 3 Document Refinery — Extraction Records",
    "week4": "Week 4 Brownfield Cartographer — Lineage Snapshots",
    "week5": "Week 5 Event Sourcing Platform — Event Records",
    "traces": "LangSmith Trace Records — AI Pipeline Observability",
}

DOWNSTREAM_MAP = {
    "week3": [
        {
            "id": "week4-cartographer",
            "description": "Cartographer ingests doc_id and extracted_facts as node metadata",
            "fields_consumed": ["doc_id", "extracted_facts", "extraction_model"],
            "breaking_if_changed": ["extracted_facts.confidence", "doc_id"]
        },
        {
            "id": "week7-contract-enforcer",
            "description": "ContractGenerator reads extractions.jsonl for contract generation",
            "fields_consumed": ["doc_id", "extracted_facts", "entities"],
            "breaking_if_changed": ["doc_id", "extracted_facts.confidence"]
        }
    ],
    "week4": [
        {
            "id": "week7-violation-attributor",
            "description": "ViolationAttributor traverses lineage graph for blame chain",
            "fields_consumed": ["nodes", "edges", "git_commit"],
            "breaking_if_changed": ["edges.source", "edges.target", "nodes.node_id"]
        }
    ],
    "week5": [
        {
            "id": "week7-contract-enforcer",
            "description": "ValidationRunner validates event payload schema",
            "fields_consumed": ["event_type", "payload", "schema_version"],
            "breaking_if_changed": ["event_type", "payload", "sequence_number"]
        }
    ],
    "traces": [
        {
            "id": "week7-ai-contract-extensions",
            "description": "AI Extensions use traces for drift and violation rate metrics",
            "fields_consumed": ["inputs", "outputs", "total_tokens", "total_cost"],
            "breaking_if_changed": ["total_tokens", "prompt_tokens", "completion_tokens"]
        }
    ],
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def ensure_dir(p: str):
    Path(p).mkdir(parents=True, exist_ok=True)

def detect_week(source_path: str) -> str:
    p = source_path.lower()
    for key in KNOWN_SOURCES:
        if key in p:
            return key
    return "unknown"

def load_jsonl_flat(path: str) -> pd.DataFrame:
    """Load JSONL into a flat DataFrame (top-level keys only)."""
    records = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    if not records:
        return pd.DataFrame()
    return pd.json_normalize(records, max_level=1)

def infer_type(series: pd.Series) -> str:
    dtype = str(series.dtype)
    if "int" in dtype:
        return "integer"
    if "float" in dtype:
        return "number"
    if "bool" in dtype:
        return "boolean"
    if "object" in dtype:
        # Try to detect if it looks like a date
        sample = series.dropna().head(5).astype(str)
        if sample.str.match(r"\d{4}-\d{2}-\d{2}").any():
            return "string"
        return "string"
    return "string"

def null_fraction(series: pd.Series) -> float:
    return round(series.isna().sum() / max(len(series), 1), 4)

def sample_values(series: pd.Series, n: int = 5) -> list:
    vals = series.dropna().unique()[:n]
    return [str(v) for v in vals]

def all_unique_values(series: pd.Series) -> list:
    """Return ALL unique non-null values — used for enum detection."""
    try:
        s_hash = series.apply(
            lambda x: json.dumps(x, sort_keys=True) if isinstance(x, (list, dict)) else x
        )
        vals = s_hash.dropna().unique().tolist()
        return [str(v) for v in vals]
    except Exception:
        return []


def to_builtin(value):
    """Recursively convert pandas/numpy scalars into plain Python YAML-safe types."""
    if isinstance(value, dict):
        return {to_builtin(k): to_builtin(v) for k, v in value.items()}
    if isinstance(value, list):
        return [to_builtin(v) for v in value]
    if isinstance(value, tuple):
        return [to_builtin(v) for v in value]

    if hasattr(value, "item") and callable(getattr(value, "item")):
        try:
            value = value.item()
        except Exception:
            pass

    if isinstance(value, bool):
        return bool(value)
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return float(value)
    return value

# ---------------------------------------------------------------------------
# Step 1 & 2 — Structural + Statistical Profiling
# ---------------------------------------------------------------------------

def profile_dataframe(df: pd.DataFrame) -> dict:
    """Return per-column profile dict."""
    profile = {}
    for col in df.columns:
        s = df[col]
        col_type = infer_type(s)
        # Columns containing lists/dicts can't be hashed — convert to str for profiling
        try:
            s_hash = s.apply(lambda x: json.dumps(x, sort_keys=True) if isinstance(x, (list, dict)) else x)
        except Exception:
            s_hash = s.astype(str)

        try:
            cardinality = int(s_hash.nunique())
        except TypeError:
            cardinality = -1

        try:
            sv = sample_values(s_hash)
        except Exception:
            sv = []

        # Collect ALL unique values for enum detection (cardinality <= 20)
        all_vals = []
        if cardinality <= 20:
            all_vals = all_unique_values(s_hash)

        entry = {
            "name": col,
            "dtype": str(s.dtype),
            "inferred_type": col_type,
            "null_fraction": null_fraction(s),
            "cardinality": cardinality,
            "sample_values": sv,
            "all_values": all_vals,
        }
        if col_type in ("integer", "number"):
            numeric = pd.to_numeric(s, errors="coerce").dropna()
            if len(numeric) > 0:
                entry["stats"] = {
                    "min": float(numeric.min()),
                    "max": float(numeric.max()),
                    "mean": float(numeric.mean()),
                    "p25": float(numeric.quantile(0.25)),
                    "p50": float(numeric.median()),
                    "p75": float(numeric.quantile(0.75)),
                    "p95": float(numeric.quantile(0.95)),
                    "p99": float(numeric.quantile(0.99)),
                    "stddev": float(numeric.std()),
                }
                # Confidence-scale anomaly detection
                if "confidence" in col.lower():
                    mn = entry["stats"]["min"]
                    mx = entry["stats"]["max"]
                    mean = entry["stats"]["mean"]
                    if mx > 1.0:
                        entry["confidence_scale_warning"] = (
                            f"CRITICAL: max={mx:.2f} > 1.0 — "
                            "confidence appears to be 0-100 scale, not 0.0-1.0"
                        )
                    elif mean > 0.99:
                        entry["confidence_scale_warning"] = (
                            f"WARNING: mean={mean:.4f} > 0.99 — values appear clamped"
                        )
                    elif mean < 0.01:
                        entry["confidence_scale_warning"] = (
                            f"WARNING: mean={mean:.4f} < 0.01 — confidence appears broken"
                        )
        profile[col] = entry

    return profile

# ---------------------------------------------------------------------------
# Step 3 — Lineage Context Injection
# ---------------------------------------------------------------------------

def load_lineage_graph(lineage_path: str = "outputs/week4/lineage_snapshots.jsonl") -> dict:
    """Return latest snapshot as dict, or empty dict if not found."""
    if not Path(lineage_path).exists():
        return {}
    with open(lineage_path) as f:
        snapshots = [json.loads(l) for l in f if l.strip()]
    if not snapshots:
        return {}
    return snapshots[-1]  # latest snapshot

def find_downstream_consumers(week_key: str, lineage: dict) -> list:
    """
    Query the lineage graph to find downstream nodes that consume
    the dataset for this week.
    """
    hardcoded = DOWNSTREAM_MAP.get(week_key, [])
    if not lineage:
        return hardcoded

    # Also traverse the actual graph
    nodes = {n["node_id"]: n for n in lineage.get("nodes", [])}
    edges = lineage.get("edges", [])

    # Find the node corresponding to this week's output
    week_file_patterns = {
        "week3": "extractions",
        "week4": "lineage",
        "week5": "events",
        "traces": "runs",
    }
    pattern = week_file_patterns.get(week_key, week_key)

    source_nodes = [
        nid for nid in nodes
        if pattern in nid.lower()
    ]

    graph_consumers = []
    for edge in edges:
        if edge.get("source", "") in source_nodes:
            tgt_id = edge.get("target", "")
            tgt_node = nodes.get(tgt_id, {})
            graph_consumers.append({
                "id": tgt_id,
                "description": tgt_node.get("metadata", {}).get("purpose", "downstream consumer"),
                "fields_consumed": [],
                "breaking_if_changed": []
            })

    # Merge: hardcoded takes priority, graph consumers fill gaps
    existing_ids = {c["id"] for c in hardcoded}
    for gc in graph_consumers:
        if gc["id"] not in existing_ids:
            hardcoded.append(gc)

    return hardcoded

# ---------------------------------------------------------------------------
# Step 4 — LLM Annotation (optional)
# ---------------------------------------------------------------------------

def llm_annotate_column(col_name: str, table_name: str,
                         sample_values: list, adjacent_cols: list) -> dict:
    """
    Call Claude to annotate ambiguous columns.
    Returns annotation dict or empty dict if unavailable.
    """
    if not HAS_ANTHROPIC:
        return {}

    try:
        client = anthropic.Anthropic()
        prompt = f"""You are a data engineer annotating a data contract.

Column: {col_name}
Table: {table_name}
Sample values: {sample_values}
Adjacent columns: {adjacent_cols}

Respond ONLY with a JSON object (no markdown) containing:
{{
  "description": "plain English description of this column",
  "business_rule": "validation expression e.g. value >= 0 and value <= 1",
  "cross_column_relationship": "any relationship with adjacent columns or null"
}}"""

        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}]
        )
        text = response.content[0].text.strip()
        text = re.sub(r"```json|```", "", text).strip()
        return json.loads(text)
    except Exception as e:
        return {"description": f"Auto-annotation unavailable: {e}"}

# ---------------------------------------------------------------------------
# Step 5 — Contract YAML Builder
# ---------------------------------------------------------------------------

def build_schema_clauses(week_key: str, col_profile: dict) -> dict:
    """
    Build schema section of the Bitol contract from column profiles.
    Combines profiled data with known domain constraints.
    """
    schema = {}

    # Domain-specific known constraints per week
    known = _known_constraints(week_key)

    for col, info in col_profile.items():
        # Skip deeply nested columns (handled separately)
        if "." in col and col.count(".") > 1:
            continue

        clause = {
            "type": info["inferred_type"],
            "required": bool(info["null_fraction"] == 0.0),
            "description": f"Column {col} — null fraction: {info['null_fraction']:.1%}"
        }

        # Apply known domain constraints
        if col in known:
            clause.update(known[col])

        # Add stats-derived constraints for numeric columns
        if "stats" in info:
            st = info["stats"]
            if "minimum" not in clause:
                clause["minimum"] = st["min"]
            if "maximum" not in clause:
                clause["maximum"] = st["max"]

        # Add enum detection for low-cardinality string columns
        # Use ALL unique values (not just 5 samples) so enum is complete
        if (info["inferred_type"] == "string"
                and info["cardinality"] <= 20
                and info["cardinality"] >= 2
                and info["null_fraction"] < 0.5):
            clause["enum"] = info.get("all_values", info["sample_values"])

        # UUID pattern detection
        if any(x in col.lower() for x in ["_id", "id_"]) or col.endswith("_id") or col == "id":
            if info["inferred_type"] == "string":
                clause["format"] = "uuid"

        # ISO 8601 detection
        if any(x in col.lower() for x in ["_at", "_time", "timestamp", "date"]):
            if info["inferred_type"] == "string":
                clause["format"] = "date-time"

        # Confidence-scale warning as annotation
        if "confidence_scale_warning" in info:
            clause["x_warning"] = info["confidence_scale_warning"]

        schema[col] = clause

    return schema


def _known_constraints(week_key: str) -> dict:
    """Hard-coded domain constraints per week schema."""
    base = {}
    if week_key == "week3":
        base = {
            "doc_id": {"type": "string", "format": "uuid", "unique": True,
                        "description": "Primary key. UUIDv4. Stable across re-extractions of the same source."},
            "source_hash": {"type": "string", "pattern": "^[a-f0-9]{64}$",
                             "description": "SHA-256 of the source file. Changes iff source content changes."},
            "extraction_model": {"type": "string", "pattern": "^(claude|gpt)-",
                                  "description": "Model identifier. Must match claude-* or gpt-*."},
            "processing_time_ms": {"type": "integer", "minimum": 1,
                                    "description": "Processing duration in milliseconds. Must be > 0."},
            "extracted_at": {"type": "string", "format": "date-time",
                              "description": "ISO 8601 timestamp of extraction completion."},
            "extracted_facts": {"type": "array", "required": True,
                                  "description": "List of facts extracted from the document. Must be non-empty."},
            "entities": {"type": "array", "required": True,
                          "description": "List of entities referenced by extracted facts."},
        }
    elif week_key == "week2":
        base = {
            "verdict_id": {"type": "string", "format": "uuid", "unique": True},
            "overall_verdict": {"type": "string", "enum": ["PASS", "FAIL", "WARN"],
                                  "description": "Must be exactly one of PASS, FAIL, WARN."},
            "overall_score": {"type": "number", "minimum": 1.0, "maximum": 5.0},
            "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0,
                            "description": "Confidence in verdict. Float 0.0-1.0."},
            "evaluated_at": {"type": "string", "format": "date-time"},
        }
    elif week_key == "week5":
        base = {
            "event_id": {"type": "string", "format": "uuid", "unique": True,
                          "description": "Primary key. UUIDv4. Unique per event."},
            "event_type": {"type": "string", "required": True,
                            "description": "PascalCase event type name. Registered in schema registry."},
            "aggregate_id": {"type": "string", "required": True,
                              "description": (
                                  "ID of the aggregate root. Format: '{type}-{id}' "
                                  "e.g. loan-APEX-COMMERCIAL-001 or agent-{id}. "
                                  "NOT a UUID — stream-scoped identifier."
                              )},
            "aggregate_type": {"type": "string", "required": True,
                                 "enum": ["LoanApplication", "AgentSession",
                                          "ComplianceCase", "AuditRecord", "Domain"],
                                 "description": "PascalCase aggregate type."},
            "sequence_number": {"type": "integer", "minimum": 1,
                                  "description": "Monotonically increasing per aggregate_id. No gaps, no duplicates."},
            "schema_version": {"type": "string", "required": True,
                                 "description": "Semantic version string e.g. '1.0' or '2.0'."},
            "occurred_at": {"type": "string", "format": "date-time", "required": True,
                             "description": "When the event occurred in the domain."},
            "recorded_at": {"type": "string", "format": "date-time", "required": True,
                             "description": "When the event was persisted. Must be >= occurred_at."},
            "metadata.correlation_id": {"type": "string", "format": "uuid", "required": True,
                                          "description": "Correlation ID for tracing related events."},
            "metadata.causation_id": {"type": "string", "required": False,
                                        "description": "Causation ID. Nullable — null for root events."},
            "metadata.user_id": {"type": "string", "required": True,
                                   "description": (
                                       "User or service account that triggered the event. "
                                       "May be 'system' for automated events."
                                   )},
            "metadata.source_service": {"type": "string", "required": True,
                                          "description": "Service that produced this event."},
            "payload.confidence_score": {"type": "number", "minimum": 0.0, "maximum": 1.0,
                                           "description": (
                                               "Confidence score for the decision or analysis. "
                                               "MUST be float 0.0-1.0. "
                                               "BREAKING CHANGE if changed to 0-100 scale."
                                           )},
        }
    elif week_key == "week4":
        base = {
            "snapshot_id": {"type": "string", "format": "uuid", "unique": True},
            "git_commit": {"type": "string", "pattern": "^[a-f0-9]{40}$",
                            "description": "Exactly 40 hex characters."},
            "captured_at": {"type": "string", "format": "date-time"},
        }
    elif week_key == "traces":
        base = {
            "id": {"type": "string", "format": "uuid", "unique": True},
            "run_type": {"type": "string",
                          "enum": ["llm", "chain", "tool", "retriever", "embedding"]},
            "start_time": {"type": "string", "format": "date-time"},
            "end_time": {"type": "string", "format": "date-time",
                          "description": "Must be > start_time."},
            "total_tokens": {"type": "integer", "minimum": 0},
            "prompt_tokens": {"type": "integer", "minimum": 0},
            "completion_tokens": {"type": "integer", "minimum": 0,
                                    "description": "total_tokens = prompt_tokens + completion_tokens"},
            "total_cost": {"type": "number", "minimum": 0.0,
                            "description": "Cost in USD. Must be >= 0."},
        }
    return base


def build_quality_clauses(week_key: str, col_profile: dict, row_count: int) -> dict:
    """Build Soda-style quality checks."""
    checks = {}
    id_col = {
        "week1": "intent_id", "week2": "verdict_id",
        "week3": "doc_id", "week4": "snapshot_id",
        "week5": "event_id", "traces": "id"
    }.get(week_key, "id")

    table_name = week_key.replace("week", "week_")
    checks[f"checks for {table_name}"] = [
        f"missing_count({id_col}) = 0",
        f"duplicate_count({id_col}) = 0",
        f"row_count >= {max(1, row_count // 2)}",
    ]

    # Confidence range checks
    conf_cols = [c for c in col_profile if "confidence" in c.lower()]
    for cc in conf_cols:
        checks[f"checks for {table_name}"].extend([
            f"min({cc}) >= 0.0",
            f"max({cc}) <= 1.0",
            f"avg({cc}) between 0.01 and 0.99",
        ])

    # Week-specific checks
    if week_key == "week2":
        checks[f"checks for {table_name}"].append(
            "invalid_values(overall_verdict, ['PASS','FAIL','WARN']) = 0"
        )
    if week_key == "week5":
        checks[f"checks for {table_name}"].append(
            "missing_count(event_type) = 0"
        )
    if week_key == "traces":
        checks[f"checks for {table_name}"].extend([
            "min(total_cost) >= 0.0",
            "missing_count(run_type) = 0",
        ])

    return {"type": "SodaChecks", "specification": checks}


def build_contract(week_key: str, source_path: str,
                   col_profile: dict, downstream: list,
                   row_count: int, llm_annotations: dict) -> dict:
    """Assemble the full Bitol contract dict."""
    contract_id = CONTRACT_IDS.get(week_key, f"{week_key}-records")
    schema = build_schema_clauses(week_key, col_profile)

    # Merge LLM annotations
    for col, ann in llm_annotations.items():
        if col in schema and ann.get("description"):
            schema[col]["description"] = ann["description"]
        if col in schema and ann.get("business_rule"):
            schema[col]["x_business_rule"] = ann["business_rule"]

    contract = {
        "kind": "DataContract",
        "apiVersion": "v3.0.0",
        "id": contract_id,
        "info": {
            "title": CONTRACT_TITLES.get(week_key, f"{week_key} Records"),
            "version": "1.0.0",
            "owner": f"{week_key}-team",
            "description": (
                f"Auto-generated contract for {week_key} outputs. "
                f"Generated at {now_iso()} from {row_count} records."
            )
        },
        "servers": {
            "local": {
                "type": "local",
                "path": source_path,
                "format": "jsonl"
            }
        },
        "terms": {
            "usage": "Internal inter-system data contract. Do not publish.",
            "limitations": (
                "confidence fields must remain float 0.0-1.0. "
                "Breaking changes require blast radius report."
            )
        },
        "schema": schema,
        "quality": build_quality_clauses(week_key, col_profile, row_count),
        "lineage": {
            "upstream": [],
            "downstream": downstream
        }
    }
    return contract

# ---------------------------------------------------------------------------
# dbt schema.yml Builder
# ---------------------------------------------------------------------------

def build_dbt_schema(week_key: str, schema: dict) -> dict:
    """Convert Bitol schema clauses to dbt schema.yml format with substantive tests."""
    name_map = {
        "week1": "week1_intent_records",
        "week2": "week2_verdicts",
        "week3": "week3_extractions",
        "week4": "week4_lineage_snapshots",
        "week5": "week5_events",
        "traces": "langsmith_traces",
    }
    model_name = name_map.get(week_key, week_key)
    columns = []
    model_tests = []  # model-level tests (expression_is_true, etc.)

    for col, clause in schema.items():
        col_def = {"name": col}
        tests = []

        # not_null
        if clause.get("required"):
            tests.append("not_null")

        # unique
        if clause.get("unique"):
            tests.append("unique")

        # accepted_values for enums
        if "enum" in clause:
            tests.append({
                "accepted_values": {
                    "values": clause["enum"]
                }
            })

        # dbt_utils.expression_is_true for range constraints
        mn = clause.get("minimum")
        mx = clause.get("maximum")
        if mn is not None and mx is not None:
            model_tests.append({
                "dbt_utils.expression_is_true": {
                    "expression": f"{col} >= {mn} and {col} <= {mx}",
                    "name": f"{col}_range_{mn}_to_{mx}"
                }
            })
        elif mn is not None:
            model_tests.append({
                "dbt_utils.expression_is_true": {
                    "expression": f"{col} >= {mn}",
                    "name": f"{col}_min_{mn}"
                }
            })
        elif mx is not None:
            model_tests.append({
                "dbt_utils.expression_is_true": {
                    "expression": f"{col} <= {mx}",
                    "name": f"{col}_max_{mx}"
                }
            })

        # Pattern / format hints as column-level description
        if clause.get("format"):
            col_def["description"] = (
                f"{clause.get('description', '')} "
                f"[format: {clause['format']}]"
            ).strip()
        elif clause.get("description"):
            col_def["description"] = clause["description"]

        if tests:
            col_def["tests"] = tests

        columns.append(col_def)

    # Week-specific model-level expression tests
    if week_key == "week5":
        model_tests.append({
            "dbt_utils.expression_is_true": {
                "expression": "recorded_at >= occurred_at",
                "name": "recorded_at_gte_occurred_at"
            }
        })
        model_tests.append({
            "dbt_utils.expression_is_true": {
                "expression": "sequence_number >= 1",
                "name": "sequence_number_positive"
            }
        })

    if week_key == "week3":
        model_tests.append({
            "dbt_utils.expression_is_true": {
                "expression": "processing_time_ms > 0",
                "name": "processing_time_positive"
            }
        })

    if week_key == "traces":
        model_tests.append({
            "dbt_utils.expression_is_true": {
                "expression": "end_time > start_time",
                "name": "end_time_after_start_time"
            }
        })
        model_tests.append({
            "dbt_utils.expression_is_true": {
                "expression": "total_cost >= 0",
                "name": "total_cost_non_negative"
            }
        })

    if week_key == "week2":
        model_tests.append({
            "dbt_utils.expression_is_true": {
                "expression": "overall_score between 1.0 and 5.0",
                "name": "overall_score_valid_range"
            }
        })
        model_tests.append({
            "dbt_utils.expression_is_true": {
                "expression": "confidence between 0.0 and 1.0",
                "name": "confidence_valid_range"
            }
        })

    model_def = {
        "name": model_name,
        "description": CONTRACT_TITLES.get(week_key, f"{week_key} records"),
        "columns": columns
    }
    if model_tests:
        model_def["tests"] = model_tests

    return {
        "version": 2,
        "models": [model_def]
    }

# ---------------------------------------------------------------------------
# Schema Snapshot
# ---------------------------------------------------------------------------

def save_schema_snapshot(week_key: str, schema: dict, source_path: str):
    """Save timestamped schema snapshot for evolution tracking."""
    contract_id = CONTRACT_IDS.get(week_key, week_key)
    snap_dir = f"schema_snapshots/{contract_id}"
    ensure_dir(snap_dir)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    snap_path = f"{snap_dir}/{ts}.yaml"
    snap = {
        "contract_id": contract_id,
        "snapshot_timestamp": now_iso(),
        "source_path": source_path,
        "schema": schema
    }
    with open(snap_path, "w") as f:
        yaml.safe_dump(
            to_builtin(snap),
            f,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
        )
    print(f"  ✓ snapshot → {snap_path}")

# ---------------------------------------------------------------------------
# Main generation pipeline
# ---------------------------------------------------------------------------

def generate_contract(source_path: str, output_dir: str,
                       annotate: bool = False, verbose: bool = False):
    """Full pipeline: profile → lineage → annotate → write contracts."""

    if not Path(source_path).exists():
        print(f"  ✗ Source not found: {source_path}")
        sys.exit(1)

    week_key = detect_week(source_path)
    print(f"\n→ Generating contract for: {source_path} (detected: {week_key})")

    # Load data
    df = load_jsonl_flat(source_path)
    row_count = len(df)
    print(f"  ✓ loaded {row_count} records, {len(df.columns)} top-level columns")

    if df.empty:
        print("  ✗ Empty dataset — aborting")
        return

    # Step 1+2: Profile
    col_profile = profile_dataframe(df)

    # Warn on confidence scale issues
    for col, info in col_profile.items():
        if "confidence_scale_warning" in info:
            print(f"  ⚠  {info['confidence_scale_warning']}")

    # Step 3: Lineage context
    lineage = load_lineage_graph()
    downstream = find_downstream_consumers(week_key, lineage)
    print(f"  ✓ lineage: {len(downstream)} downstream consumers identified")

    # Step 4: LLM annotation (optional, ambiguous columns only)
    llm_annotations = {}
    if annotate and HAS_ANTHROPIC:
        cols = list(df.columns)
        ambiguous = [
            c for c in cols
            if not any(x in c.lower() for x in
                       ["id", "at", "time", "hash", "path", "version",
                        "type", "status", "score", "count", "tokens"])
        ][:3]  # limit to 3 LLM calls to control cost
        for col in ambiguous:
            adjacent = [c for c in cols if c != col][:5]
            ann = llm_annotate_column(
                col, week_key,
                col_profile[col]["sample_values"],
                adjacent
            )
            if ann:
                llm_annotations[col] = ann
                print(f"  ✓ LLM annotated: {col}")

    # Step 5: Build and write contract
    ensure_dir(output_dir)
    contract = build_contract(
        week_key, source_path, col_profile,
        downstream, row_count, llm_annotations
    )

    # Contract YAML
    contract_file = f"{output_dir}/{week_key}_{'traces' if week_key == 'traces' else week_key + '_records' if week_key != 'week3' else 'extractions'}.yaml"
    # Better naming
    name_map = {
        "week1": "week1_intent_records",
        "week2": "week2_verdicts",
        "week3": "week3_extractions",
        "week4": "week4_lineage",
        "week5": "week5_events",
        "traces": "langsmith_traces",
    }
    contract_file = f"{output_dir}/{name_map.get(week_key, week_key)}.yaml"

    with open(contract_file, "w") as f:
        yaml.safe_dump(
            to_builtin(contract),
            f,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
        )
    print(f"  ✓ contract → {contract_file}")

    # dbt schema.yml
    dbt_schema = build_dbt_schema(week_key, contract["schema"])
    dbt_file = f"{output_dir}/{name_map.get(week_key, week_key)}_dbt.yml"
    with open(dbt_file, "w") as f:
        yaml.safe_dump(
            to_builtin(dbt_schema),
            f,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
        )
    print(f"  ✓ dbt schema → {dbt_file}")

    # Schema snapshot
    save_schema_snapshot(week_key, contract["schema"], source_path)

    # Summary
    n_clauses = len(contract["schema"])
    print(f"  ✓ {n_clauses} schema clauses generated")
    if n_clauses < 8:
        print(f"  ⚠  Only {n_clauses} clauses — contract may need manual review")

    return contract

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="ContractGenerator — auto-generate Bitol data contracts from JSONL"
    )
    parser.add_argument(
        "--source", type=str,
        help="Path to a single JSONL file"
    )
    parser.add_argument(
        "--output", type=str, default="generated_contracts",
        help="Output directory for YAML contracts (default: generated_contracts/)"
    )
    parser.add_argument(
        "--all", action="store_true",
        help="Generate contracts for all known sources"
    )
    parser.add_argument(
        "--annotate", action="store_true",
        help="Enable LLM annotation for ambiguous columns (requires ANTHROPIC_API_KEY)"
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="Verbose output"
    )
    args = parser.parse_args()

    if not args.source and not args.all:
        parser.print_help()
        sys.exit(1)

    if args.all:
        print("=== ContractGenerator: all sources ===")
        for key, path in KNOWN_SOURCES.items():
            if Path(path).exists():
                generate_contract(path, args.output,
                                  annotate=args.annotate, verbose=args.verbose)
            else:
                print(f"\n  ⚠  Skipping {key} — {path} not found")
    else:
        generate_contract(args.source, args.output,
                          annotate=args.annotate, verbose=args.verbose)

    print("\n=== ContractGenerator complete ===")


if __name__ == "__main__":
    main()
