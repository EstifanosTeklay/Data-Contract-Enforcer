"""
contracts/ai_extensions.py
============================
AI Contract Extensions — Phase 4A of the Data Contract Enforcer.

Three extensions:
  1. Embedding Drift Detection
     - Embeds extracted_facts[*].text using sentence-transformers
     - Computes cosine distance from stored centroid baseline
     - Alerts if drift > threshold (default 0.15)

  2. Prompt Input Schema Validation
     - Validates document metadata objects against JSON Schema draft-07
     - Quarantines non-conforming records to outputs/quarantine/
     - Never silently drops records

  3. LLM Output Schema Violation Rate
     - Validates Week 2 verdict records against expected output schema
     - Tracks violation_rate per run
     - Emits WARN if rate is rising vs baseline

Usage:
    python contracts/ai_extensions.py --all

    python contracts/ai_extensions.py --embedding-drift
    python contracts/ai_extensions.py --prompt-validation
    python contracts/ai_extensions.py --output-schema

Requirements:
    pip install sentence-transformers numpy pyyaml jsonschema
"""

import argparse
import json
import os
import uuid
import warnings
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import yaml
from ledger_bridge import append_violation_event

warnings.filterwarnings("ignore")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

EXTRACTIONS_PATH = "outputs/week3/extractions.jsonl"
VERDICTS_PATH = "outputs/week2/verdicts.jsonl"
BASELINE_PATH = "schema_snapshots/embedding_baselines.npz"
AI_METRICS_PATH = "validation_reports/ai_metrics.json"
QUARANTINE_DIR = "outputs/quarantine"
VIOLATION_LOG = "violation_log/violations.jsonl"

EMBEDDING_MODEL = "all-MiniLM-L6-v2"
EMBEDDING_DRIFT_THRESHOLD = 0.15
SAMPLE_SIZE = 200
OUTPUT_VIOLATION_WARN_THRESHOLD = 0.02

# Prompt input JSON Schema (draft-07)
PROMPT_INPUT_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["doc_id", "source_path", "content_preview"],
    "properties": {
        "doc_id": {
            "type": "string",
            "minLength": 36,
            "maxLength": 36,
            "pattern": "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        },
        "source_path": {
            "type": "string",
            "minLength": 1
        },
        "content_preview": {
            "type": "string",
            "maxLength": 8000
        }
    },
    "additionalProperties": False
}

# Expected verdict output schema
VERDICT_OUTPUT_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["verdict_id", "overall_verdict", "overall_score", "confidence"],
    "properties": {
        "verdict_id": {"type": "string"},
        "overall_verdict": {
            "type": "string",
            "enum": ["PASS", "FAIL", "WARN"]
        },
        "overall_score": {
            "type": "number",
            "minimum": 1.0,
            "maximum": 5.0
        },
        "confidence": {
            "type": "number",
            "minimum": 0.0,
            "maximum": 1.0
        },
        "scores": {"type": "object"}
    }
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def ensure_dir(p: str):
    Path(p).mkdir(parents=True, exist_ok=True)

def load_jsonl(path: str) -> list:
    records = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return records

def load_ai_metrics() -> dict:
    if Path(AI_METRICS_PATH).exists():
        with open(AI_METRICS_PATH) as f:
            return json.load(f)
    return {}

def save_ai_metrics(metrics: dict):
    ensure_dir(os.path.dirname(AI_METRICS_PATH))
    with open(AI_METRICS_PATH, "w") as f:
        json.dump(metrics, f, indent=2)

def write_violation(violation: dict):
    ensure_dir(os.path.dirname(VIOLATION_LOG))
    with open(VIOLATION_LOG, "a") as f:
        f.write(json.dumps(violation) + "\n")
    version = append_violation_event(violation, require_blame=False)
    print(f"  ✓ appended to ledger stream → audit-contract-violations @ version {version}")

def cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    denom = (np.linalg.norm(a) * np.linalg.norm(b)) + 1e-9
    return float(np.dot(a, b) / denom)

# ---------------------------------------------------------------------------
# Extension 1 — Embedding Drift Detection
# ---------------------------------------------------------------------------

def load_embedding_model():
    """Load sentence-transformers model."""
    try:
        from sentence_transformers import SentenceTransformer
        print(f"  loading embedding model: {EMBEDDING_MODEL}")
        model = SentenceTransformer(EMBEDDING_MODEL)
        return model
    except ImportError:
        print("  ✗ sentence-transformers not installed.")
        print("    Run: pip install sentence-transformers")
        return None


def extract_fact_texts(extractions: list, n: int = SAMPLE_SIZE) -> list:
    """Extract text values from extracted_facts arrays."""
    texts = []
    for record in extractions:
        facts = record.get("extracted_facts", [])
        if isinstance(facts, list):
            for fact in facts:
                if isinstance(fact, dict):
                    text = fact.get("text", "")
                    if text and len(text) > 5:
                        texts.append(text)
        if len(texts) >= n * 2:
            break

    # Sample up to n
    if len(texts) > n:
        import random
        random.seed(42)
        texts = random.sample(texts, n)

    return texts


def check_embedding_drift(model, texts: list) -> dict:
    """
    Compute embedding drift against stored baseline centroid.
    Returns drift result dict.
    """
    if not texts:
        return {
            "status": "ERROR",
            "drift_score": None,
            "message": "No text values found in extracted_facts",
        }

    print(f"  embedding {len(texts)} text samples...")
    embeddings = model.encode(texts, show_progress_bar=False)
    current_centroid = np.mean(embeddings, axis=0)

    baseline_path = Path(BASELINE_PATH)

    if not baseline_path.exists():
        # First run — store baseline
        ensure_dir(str(baseline_path.parent))
        np.savez(str(baseline_path), centroid=current_centroid)
        print(f"  ✓ baseline established → {BASELINE_PATH}")
        return {
            "status": "BASELINE_SET",
            "drift_score": 0.0,
            "threshold": EMBEDDING_DRIFT_THRESHOLD,
            "sample_size": len(texts),
            "message": "First run — embedding baseline established.",
            "baseline_path": str(baseline_path),
        }

    # Load baseline and compute drift
    baseline_data = np.load(str(baseline_path))
    baseline_centroid = baseline_data["centroid"]

    sim = cosine_similarity(current_centroid, baseline_centroid)
    drift = round(1.0 - sim, 4)

    status = "FAIL" if drift > EMBEDDING_DRIFT_THRESHOLD else "PASS"

    result = {
        "status": status,
        "drift_score": drift,
        "cosine_similarity": round(sim, 4),
        "threshold": EMBEDDING_DRIFT_THRESHOLD,
        "sample_size": len(texts),
        "message": (
            f"Embedding drift {'DETECTED' if status == 'FAIL' else 'within bounds'}: "
            f"score={drift:.4f}, threshold={EMBEDDING_DRIFT_THRESHOLD}"
        ),
        "baseline_path": str(baseline_path),
    }

    if status == "FAIL":
        write_violation({
            "violation_id": str(uuid.uuid4()),
            "check_id": "week3.extracted_facts.text.embedding_drift",
            "contract_id": "week3-document-refinery-extractions",
            "detected_at": now_iso(),
            "failing_field": "extracted_facts[*].text",
            "check_type": "embedding_drift",
            "severity": "HIGH",
            "actual_value": f"drift_score={drift:.4f}",
            "expected": f"drift_score<={EMBEDDING_DRIFT_THRESHOLD}",
            "records_failing": len(texts),
            "message": result["message"],
            "blame_chain": [],
            "blast_radius": {
                "registry_subscribers": ["week7-ai-contract-extensions"],
                "affected_nodes": ["file::outputs/week3/extractions.jsonl"],
                "affected_pipelines": [],
                "estimated_records": len(texts),
            }
        })

    return result


def run_embedding_drift() -> dict:
    print("\n── Extension 1: Embedding Drift Detection ──")

    if not Path(EXTRACTIONS_PATH).exists():
        print(f"  ✗ {EXTRACTIONS_PATH} not found")
        return {"status": "ERROR", "message": f"{EXTRACTIONS_PATH} not found"}

    model = load_embedding_model()
    if not model:
        return {"status": "ERROR", "message": "embedding model unavailable"}

    extractions = load_jsonl(EXTRACTIONS_PATH)
    print(f"  loaded {len(extractions)} extraction records")

    texts = extract_fact_texts(extractions)
    print(f"  extracted {len(texts)} fact text samples")

    result = check_embedding_drift(model, texts)

    status_icon = "✓" if result["status"] in ("PASS", "BASELINE_SET") else "✗"
    print(f"  {status_icon} status: {result['status']}")
    print(f"  drift score: {result.get('drift_score', 'N/A')}")
    print(f"  threshold:   {result.get('threshold', EMBEDDING_DRIFT_THRESHOLD)}")
    print(f"  message: {result['message']}")

    return result

# ---------------------------------------------------------------------------
# Extension 2 — Prompt Input Schema Validation
# ---------------------------------------------------------------------------

def build_prompt_input(record: dict) -> dict:
    """Build the prompt input object from an extraction record."""
    # Synthesise content_preview from extracted facts text
    facts = record.get("extracted_facts", [])
    preview_parts = []
    for f in facts[:3]:
        if isinstance(f, dict) and f.get("text"):
            preview_parts.append(f["text"])
    content_preview = " ".join(preview_parts)[:8000]

    return {
        "doc_id": record.get("doc_id", ""),
        "source_path": record.get("source_path", ""),
        "content_preview": content_preview,
    }


def validate_prompt_inputs(extractions: list) -> dict:
    """
    Validate each record's prompt input against PROMPT_INPUT_SCHEMA.
    Quarantine non-conforming records.
    """
    try:
        import jsonschema
    except ImportError:
        print("  ✗ jsonschema not installed. Run: pip install jsonschema")
        return {"status": "ERROR", "message": "jsonschema not installed"}

    total = len(extractions)
    violations = []
    valid_records = []
    quarantined = []

    for record in extractions:
        prompt_input = build_prompt_input(record)
        try:
            jsonschema.validate(instance=prompt_input, schema=PROMPT_INPUT_SCHEMA)
            valid_records.append(record)
        except jsonschema.ValidationError as e:
            violations.append({
                "doc_id": record.get("doc_id", "unknown"),
                "error": e.message,
                "failing_field": list(e.path) if e.path else ["unknown"],
                "prompt_input": prompt_input,
            })
            quarantined.append(record)

    # Write quarantined records
    if quarantined:
        ensure_dir(QUARANTINE_DIR)
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        q_path = f"{QUARANTINE_DIR}/{ts}_prompt_validation.jsonl"
        with open(q_path, "w") as f:
            for r in quarantined:
                f.write(json.dumps(r) + "\n")
        print(f"  ✓ quarantined {len(quarantined)} records → {q_path}")

    violation_rate = round(len(violations) / max(total, 1), 4)

    return {
        "status": "FAIL" if violations else "PASS",
        "total_records": total,
        "valid_records": len(valid_records),
        "violations": len(violations),
        "violation_rate": violation_rate,
        "quarantined_count": len(quarantined),
        "violation_details": violations[:5],  # sample of first 5
        "message": (
            f"{len(violations)} of {total} records failed prompt input schema validation."
            if violations else
            f"All {total} records pass prompt input schema validation."
        )
    }


def run_prompt_validation() -> dict:
    print("\n── Extension 2: Prompt Input Schema Validation ──")

    if not Path(EXTRACTIONS_PATH).exists():
        print(f"  ✗ {EXTRACTIONS_PATH} not found")
        return {"status": "ERROR", "message": f"{EXTRACTIONS_PATH} not found"}

    extractions = load_jsonl(EXTRACTIONS_PATH)
    print(f"  loaded {len(extractions)} extraction records")

    result = validate_prompt_inputs(extractions)

    status_icon = "✓" if result["status"] == "PASS" else "✗"
    print(f"  {status_icon} status: {result['status']}")
    print(f"  total: {result['total_records']}, "
          f"valid: {result['valid_records']}, "
          f"violations: {result['violations']}")
    print(f"  violation rate: {result['violation_rate']:.2%}")
    print(f"  message: {result['message']}")

    if result.get("violation_details"):
        print(f"  sample violations:")
        for v in result["violation_details"][:3]:
            print(f"    doc_id={v['doc_id'][:8]}... field={v['failing_field']} error={v['error'][:60]}")

    return result

# ---------------------------------------------------------------------------
# Extension 3 — LLM Output Schema Violation Rate
# ---------------------------------------------------------------------------

def validate_verdict_record(record: dict) -> list:
    """
    Validate a single verdict record against VERDICT_OUTPUT_SCHEMA.
    Returns list of violation strings (empty if valid).
    """
    try:
        import jsonschema
        jsonschema.validate(instance=record, schema=VERDICT_OUTPUT_SCHEMA)
        return []
    except ImportError:
        # Manual validation fallback
        violations = []
        verdict = record.get("overall_verdict")
        if verdict not in ("PASS", "FAIL", "WARN"):
            violations.append(
                f"overall_verdict '{verdict}' not in PASS|FAIL|WARN"
            )
        score = record.get("overall_score")
        if score is not None:
            try:
                if not (1.0 <= float(score) <= 5.0):
                    violations.append(
                        f"overall_score {score} outside [1.0, 5.0]"
                    )
            except (TypeError, ValueError):
                violations.append(f"overall_score '{score}' is not numeric")
        conf = record.get("confidence")
        if conf is not None:
            try:
                if not (0.0 <= float(conf) <= 1.0):
                    violations.append(
                        f"confidence {conf} outside [0.0, 1.0]"
                    )
            except (TypeError, ValueError):
                violations.append(f"confidence '{conf}' is not numeric")
        return violations
    except Exception as e:
        return [str(e)]


def check_output_schema_violation_rate(verdicts: list,
                                        baseline_rate: float = None) -> dict:
    """
    Track LLM output schema violation rate.
    Emits WARN if trend is rising vs baseline.
    """
    total = len(verdicts)
    violation_records = []

    for v in verdicts:
        errs = validate_verdict_record(v)
        if errs:
            violation_records.append({
                "verdict_id": v.get("verdict_id", "unknown"),
                "errors": errs,
            })

    rate = round(len(violation_records) / max(total, 1), 4)

    trend = "unknown"
    if baseline_rate is not None:
        if rate > baseline_rate * 1.5:
            trend = "rising"
        elif rate < baseline_rate * 0.5:
            trend = "falling"
        else:
            trend = "stable"

    status = "PASS"
    if trend == "rising":
        status = "WARN"
    if rate > OUTPUT_VIOLATION_WARN_THRESHOLD:
        status = "WARN"

    result = {
        "status": status,
        "total_outputs": total,
        "schema_violations": len(violation_records),
        "violation_rate": rate,
        "baseline_violation_rate": baseline_rate,
        "trend": trend,
        "warn_threshold": OUTPUT_VIOLATION_WARN_THRESHOLD,
        "violation_details": violation_records[:5],
        "message": (
            f"LLM output schema violation rate: {rate:.2%} "
            f"({'rising — WARN' if trend == 'rising' else trend}). "
            f"{len(violation_records)} of {total} verdicts failed schema."
        )
    }

    # Write violation to log if WARN
    if status == "WARN":
        write_violation({
            "violation_id": str(uuid.uuid4()),
            "check_id": "week2.overall_verdict.output_schema_violation_rate",
            "contract_id": "week2-verdict-records",
            "detected_at": now_iso(),
            "failing_field": "overall_verdict",
            "check_type": "llm_output_schema",
            "severity": "HIGH",
            "actual_value": f"violation_rate={rate:.4f}, trend={trend}",
            "expected": f"violation_rate<={OUTPUT_VIOLATION_WARN_THRESHOLD}, trend=stable",
            "records_failing": len(violation_records),
            "message": result["message"],
            "blame_chain": [],
            "blast_radius": {
                "registry_subscribers": ["week7-ai-contract-extensions"],
                "affected_nodes": ["file::outputs/week2/verdicts.jsonl"],
                "affected_pipelines": [],
                "estimated_records": len(violation_records),
            }
        })

    return result


def run_output_schema_check() -> dict:
    print("\n── Extension 3: LLM Output Schema Violation Rate ──")

    if not Path(VERDICTS_PATH).exists():
        print(f"  ✗ {VERDICTS_PATH} not found")
        return {"status": "ERROR", "message": f"{VERDICTS_PATH} not found"}

    verdicts = load_jsonl(VERDICTS_PATH)
    print(f"  loaded {len(verdicts)} verdict records")

    # Load baseline rate from ai_metrics
    metrics = load_ai_metrics()
    baseline_rate = metrics.get("output_schema_violation_rate", {}).get(
        "baseline_violation_rate"
    )

    result = check_output_schema_violation_rate(verdicts, baseline_rate)

    status_icon = "✓" if result["status"] == "PASS" else "⚠"
    print(f"  {status_icon} status: {result['status']}")
    print(f"  total outputs:     {result['total_outputs']}")
    print(f"  schema violations: {result['schema_violations']}")
    print(f"  violation rate:    {result['violation_rate']:.2%}")
    print(f"  trend:             {result['trend']}")
    print(f"  message: {result['message']}")

    return result

# ---------------------------------------------------------------------------
# Metrics aggregator
# ---------------------------------------------------------------------------

def save_run_metrics(drift_result: dict, prompt_result: dict,
                     output_result: dict):
    """Save aggregated AI metrics for the report generator."""
    metrics = {
        "run_date": now_iso(),
        "embedding_drift": {
            "status": drift_result.get("status"),
            "drift_score": drift_result.get("drift_score"),
            "threshold": drift_result.get("threshold", EMBEDDING_DRIFT_THRESHOLD),
            "sample_size": drift_result.get("sample_size"),
            "message": drift_result.get("message"),
        },
        "prompt_input_validation": {
            "status": prompt_result.get("status"),
            "total_records": prompt_result.get("total_records"),
            "violations": prompt_result.get("violations"),
            "violation_rate": prompt_result.get("violation_rate"),
            "quarantined_count": prompt_result.get("quarantined_count"),
            "message": prompt_result.get("message"),
        },
        "output_schema_violation_rate": {
            "status": output_result.get("status"),
            "total_outputs": output_result.get("total_outputs"),
            "schema_violations": output_result.get("schema_violations"),
            "violation_rate": output_result.get("violation_rate"),
            "baseline_violation_rate": output_result.get("baseline_violation_rate"),
            "trend": output_result.get("trend"),
            "message": output_result.get("message"),
        },
        "overall_ai_risk": _compute_overall_risk(
            drift_result, prompt_result, output_result
        ),
    }

    save_ai_metrics(metrics)
    print(f"\n  ✓ AI metrics saved → {AI_METRICS_PATH}")
    return metrics


def _compute_overall_risk(drift: dict, prompt: dict, output: dict) -> str:
    statuses = [
        drift.get("status", "PASS"),
        prompt.get("status", "PASS"),
        output.get("status", "PASS"),
    ]
    if "FAIL" in statuses:
        return "HIGH"
    if "WARN" in statuses or "ERROR" in statuses:
        return "MEDIUM"
    if "BASELINE_SET" in statuses:
        return "LOW — baselines being established"
    return "LOW"

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="AI Contract Extensions — embedding drift, prompt schema, output schema"
    )
    parser.add_argument(
        "--all", action="store_true",
        help="Run all three extensions"
    )
    parser.add_argument(
        "--embedding-drift", action="store_true",
        help="Run Extension 1: embedding drift detection"
    )
    parser.add_argument(
        "--prompt-validation", action="store_true",
        help="Run Extension 2: prompt input schema validation"
    )
    parser.add_argument(
        "--output-schema", action="store_true",
        help="Run Extension 3: LLM output schema violation rate"
    )
    args = parser.parse_args()

    if not any([args.all, args.embedding_drift,
                args.prompt_validation, args.output_schema]):
        parser.print_help()
        return

    print("\n=== AI Contract Extensions ===")

    drift_result = {"status": "SKIPPED"}
    prompt_result = {"status": "SKIPPED"}
    output_result = {"status": "SKIPPED"}

    if args.all or args.embedding_drift:
        drift_result = run_embedding_drift()

    if args.all or args.prompt_validation:
        prompt_result = run_prompt_validation()

    if args.all or args.output_schema:
        output_result = run_output_schema_check()

    if args.all:
        metrics = save_run_metrics(drift_result, prompt_result, output_result)
        print(f"\n  overall AI risk: {metrics['overall_ai_risk']}")

    print("\n=== AI Contract Extensions complete ===")


if __name__ == "__main__":
    main()
