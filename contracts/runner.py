"""
contracts/runner.py
===================
ValidationRunner — Phase 2A of the Data Contract Enforcer.

Executes every clause in a contract YAML file against a data snapshot
and produces a structured JSON validation report.

Usage:
    python contracts/runner.py \
        --contract generated_contracts/week3_extractions.yaml \
        --data outputs/week3/extractions.jsonl

    python contracts/runner.py \
        --contract generated_contracts/week5_events.yaml \
        --data outputs/week5/events.jsonl \
        --output validation_reports/week5_custom.json

Requirements:
    pip install pandas pyyaml
"""

import argparse
import hashlib
import json
import os
import re
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml
from attributor import attribute_report

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

def load_jsonl_flat(path: str) -> pd.DataFrame:
    records = load_jsonl(path)
    if not records:
        return pd.DataFrame()
    return pd.json_normalize(records, max_level=1)

def load_contract(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)

def load_baselines(baseline_path: str) -> dict:
    if Path(baseline_path).exists():
        with open(baseline_path) as f:
            return json.load(f)
    return {}

def save_baselines(baseline_path: str, baselines: dict):
    ensure_dir(os.path.dirname(baseline_path))
    with open(baseline_path, "w") as f:
        json.dump(baselines, f, indent=2)

# ---------------------------------------------------------------------------
# Check result builder
# ---------------------------------------------------------------------------

SEVERITY_ORDER = {"CRITICAL": 4, "HIGH": 3, "MEDIUM": 2, "LOW": 1, "WARNING": 0}

def make_result(check_id, column_name, check_type, status,
                actual_value, expected, severity, records_failing=0,
                sample_failing=None, message=""):
    return {
        "check_id": check_id,
        "column_name": column_name,
        "check_type": check_type,
        "status": status,          # PASS | FAIL | WARN | ERROR
        "actual_value": str(actual_value),
        "expected": str(expected),
        "severity": severity,      # CRITICAL | HIGH | MEDIUM | LOW | WARNING
        "records_failing": records_failing,
        "sample_failing": sample_failing or [],
        "message": message
    }

# ---------------------------------------------------------------------------
# Individual check implementations
# ---------------------------------------------------------------------------

def check_not_null(df, col, contract_id) -> dict:
    check_id = f"{contract_id}.{col}.not_null"
    if col not in df.columns:
        return make_result(check_id, col, "not_null", "ERROR",
                           "column missing", "column exists",
                           "CRITICAL", message=f"Column '{col}' not found in dataset.")
    null_count = int(df[col].isna().sum())
    if null_count > 0:
        sample = df[df[col].isna()].index[:3].tolist()
        return make_result(check_id, col, "not_null", "FAIL",
                           f"null_count={null_count}", "null_count=0",
                           "CRITICAL", null_count,
                           [str(i) for i in sample],
                           f"{null_count} null values found in required column '{col}'.")
    return make_result(check_id, col, "not_null", "PASS",
                       "null_count=0", "null_count=0", "LOW")


def check_unique(df, col, contract_id) -> dict:
    check_id = f"{contract_id}.{col}.unique"
    if col not in df.columns:
        return make_result(check_id, col, "unique", "ERROR",
                           "column missing", "column exists",
                           "CRITICAL", message=f"Column '{col}' not found.")
    try:
        dup_count = int(df[col].duplicated().sum())
    except TypeError:
        return make_result(check_id, col, "unique", "ERROR",
                           "unhashable values", "unique values",
                           "HIGH", message=f"Column '{col}' contains unhashable values.")
    if dup_count > 0:
        try:
            dups = df[df[col].duplicated(keep=False)][col].head(3).tolist()
            sample = [str(d) for d in dups]
        except Exception:
            sample = []
        return make_result(check_id, col, "unique", "FAIL",
                           f"duplicate_count={dup_count}", "duplicate_count=0",
                           "CRITICAL", dup_count, sample,
                           f"{dup_count} duplicate values in '{col}'.")
    return make_result(check_id, col, "unique", "PASS",
                       "duplicate_count=0", "duplicate_count=0", "LOW")


def check_range(df, col, minimum, maximum, contract_id) -> dict:
    check_id = f"{contract_id}.{col}.range"
    if col not in df.columns:
        return make_result(check_id, col, "range", "ERROR",
                           "column missing", f"column exists with range [{minimum},{maximum}]",
                           "CRITICAL", message=f"Column '{col}' not found.")
    numeric = pd.to_numeric(df[col], errors="coerce")
    fail_mask = pd.Series([False] * len(df))
    if minimum is not None:
        fail_mask = fail_mask | (numeric < minimum)
    if maximum is not None:
        fail_mask = fail_mask | (numeric > maximum)
    fail_mask = fail_mask & numeric.notna()
    fail_count = int(fail_mask.sum())

    actual_min = float(numeric.min()) if not numeric.empty else None
    actual_max = float(numeric.max()) if not numeric.empty else None
    actual_mean = float(numeric.mean()) if not numeric.empty else None

    # Statistical drift detection for confidence columns
    severity = "CRITICAL"
    if actual_max is not None and maximum is not None and actual_max > maximum:
        # Likely scale change (0-1 → 0-100)
        if actual_max > 1.5 and maximum <= 1.0:
            severity = "CRITICAL"
            msg = (f"confidence is in 0-100 range, not 0.0-1.0. "
                   f"Breaking change detected. "
                   f"actual max={actual_max:.2f}, mean={actual_mean:.2f}")
        else:
            msg = f"Values exceed maximum {maximum}. actual max={actual_max:.4f}"
    elif fail_count > 0:
        msg = (f"{fail_count} records outside [{minimum},{maximum}]. "
               f"actual min={actual_min:.4f}, max={actual_max:.4f}")
    else:
        msg = f"All values within [{minimum},{maximum}]."

    if fail_count > 0:
        try:
            sample = df[fail_mask][col].head(3).tolist()
            sample = [str(s) for s in sample]
        except Exception:
            sample = []
        return make_result(check_id, col, "range", "FAIL",
                           f"min={actual_min:.4f}, max={actual_max:.4f}, mean={actual_mean:.4f}",
                           f"min>={minimum}, max<={maximum}",
                           severity, fail_count, sample, msg)
    return make_result(check_id, col, "range", "PASS",
                       f"min={actual_min:.4f}, max={actual_max:.4f}",
                       f"[{minimum},{maximum}]", "LOW", message=msg)


def check_enum(df, col, allowed_values, contract_id) -> dict:
    check_id = f"{contract_id}.{col}.enum"
    if col not in df.columns:
        return make_result(check_id, col, "enum", "ERROR",
                           "column missing", f"one of {allowed_values}",
                           "CRITICAL", message=f"Column '{col}' not found.")
    try:
        invalid = df[~df[col].isin(allowed_values) & df[col].notna()]
    except TypeError:
        return make_result(check_id, col, "enum", "ERROR",
                           "type error", f"one of {allowed_values}",
                           "HIGH", message="Could not compare column values to enum list.")
    fail_count = len(invalid)
    if fail_count > 0:
        sample = invalid[col].head(3).tolist()
        return make_result(check_id, col, "enum", "FAIL",
                           f"found: {invalid[col].unique()[:5].tolist()}",
                           f"one of {allowed_values}",
                           "CRITICAL", fail_count, [str(s) for s in sample],
                           f"{fail_count} values not in allowed set {allowed_values}.")
    return make_result(check_id, col, "enum", "PASS",
                       f"all values in {allowed_values}",
                       f"one of {allowed_values}", "LOW")


def check_pattern(df, col, pattern, contract_id) -> dict:
    check_id = f"{contract_id}.{col}.pattern"
    if col not in df.columns:
        return make_result(check_id, col, "pattern", "ERROR",
                           "column missing", f"matches {pattern}",
                           "CRITICAL", message=f"Column '{col}' not found.")
    try:
        str_col = df[col].dropna().astype(str)
        fail_mask = ~str_col.str.match(pattern, na=False)
        fail_count = int(fail_mask.sum())
    except Exception as e:
        return make_result(check_id, col, "pattern", "ERROR",
                           str(e), f"matches {pattern}",
                           "HIGH", message=f"Pattern check error: {e}")
    if fail_count > 0:
        sample = str_col[fail_mask].head(3).tolist()
        return make_result(check_id, col, "pattern", "FAIL",
                           f"{fail_count} non-matching values",
                           f"matches pattern: {pattern}",
                           "HIGH", fail_count, sample,
                           f"{fail_count} values do not match pattern '{pattern}'.")
    return make_result(check_id, col, "pattern", "PASS",
                       "all values match pattern", f"matches {pattern}", "LOW")


def check_format(df, col, fmt, contract_id) -> dict:
    check_id = f"{contract_id}.{col}.format"
    if col not in df.columns:
        return make_result(check_id, col, "format", "ERROR",
                           "column missing", f"format: {fmt}",
                           "CRITICAL", message=f"Column '{col}' not found.")

    if fmt == "uuid":
        uuid_pat = (r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}"
                    r"-[0-9a-f]{4}-[0-9a-f]{12}$")
        return check_pattern(df, col, uuid_pat, contract_id)

    if fmt == "date-time":
        try:
            str_col = df[col].dropna().astype(str)
            iso_pat = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"
            fail_mask = ~str_col.str.match(iso_pat, na=False)
            fail_count = int(fail_mask.sum())
            if fail_count > 0:
                sample = str_col[fail_mask].head(3).tolist()
                return make_result(check_id, col, "format", "FAIL",
                                   f"{fail_count} non-ISO values",
                                   "ISO 8601 date-time",
                                   "HIGH", fail_count, sample,
                                   f"{fail_count} values not ISO 8601 in '{col}'.")
        except Exception as e:
            return make_result(check_id, col, "format", "ERROR",
                               str(e), "ISO 8601 date-time",
                               "HIGH", message=str(e))

    return make_result(check_id, col, "format", "PASS",
                       f"format {fmt} OK", f"format: {fmt}", "LOW")


def check_row_count(df, minimum, contract_id) -> dict:
    check_id = f"{contract_id}.row_count"
    actual = len(df)
    if actual < minimum:
        return make_result(check_id, "table", "row_count", "FAIL",
                           f"row_count={actual}", f"row_count>={minimum}",
                           "HIGH", message=f"Dataset has {actual} rows, expected >= {minimum}.")
    return make_result(check_id, "table", "row_count", "PASS",
                       f"row_count={actual}", f">={minimum}", "LOW")


def check_nested_array_confidence(records: list, contract_id: str) -> dict:
    """
    Validate extracted_facts[*].confidence values are in [0.0, 1.0].
    Expects raw JSONL records (dict objects), not a DataFrame.
    """
    check_id = f"{contract_id}.extracted_facts[*].confidence.range"
    column_name = "extracted_facts[*].confidence"

    values = []
    failing_values = []

    for record in records:
        if not isinstance(record, dict):
            continue
        facts = record.get("extracted_facts", [])
        if not isinstance(facts, list):
            continue
        for fact in facts:
            if not isinstance(fact, dict):
                continue
            conf = fact.get("confidence")
            if conf is None:
                continue
            try:
                val = float(conf)
            except (TypeError, ValueError):
                continue
            values.append(val)
            if val < 0.0 or val > 1.0:
                failing_values.append(val)

    if not values:
        return make_result(
            check_id=check_id,
            column_name=column_name,
            check_type="range",
            status="ERROR",
            actual_value="no confidence values found",
            expected="min>=0.0, max<=1.0",
            severity="CRITICAL",
            records_failing=0,
            sample_failing=[],
            message="No extracted_facts confidence values found."
        )

    min_val = min(values)
    max_val = max(values)
    mean_val = sum(values) / len(values)
    fail_count = len(failing_values)

    if fail_count > 0:
        return make_result(
            check_id=check_id,
            column_name=column_name,
            check_type="range",
            status="FAIL",
            actual_value=f"min={min_val:.4f}, max={max_val:.4f}, mean={mean_val:.4f}",
            expected="min>=0.0, max<=1.0",
            severity="CRITICAL",
            records_failing=fail_count,
            sample_failing=[str(v) for v in failing_values[:3]],
            message="confidence is in 0-100 range, not 0.0-1.0. Breaking change detected."
        )

    return make_result(
        check_id=check_id,
        column_name=column_name,
        check_type="range",
        status="PASS",
        actual_value=f"min={min_val:.4f}, max={max_val:.4f}, mean={mean_val:.4f}",
        expected="min>=0.0, max<=1.0",
        severity="CRITICAL",
        records_failing=0,
        sample_failing=[],
        message="All extracted_facts confidence values within [0.0, 1.0]."
    )


def check_statistical_drift(df, col, baseline: dict, contract_id) -> list:
    """
    Compare current numeric distribution to stored baseline.
    Emit WARNING at >2 stddev, FAIL at >3 stddev.
    Also catches the 0.0-1.0 → 0-100 confidence scale change.
    """
    results = []
    check_id = f"{contract_id}.{col}.statistical_drift"

    if col not in df.columns:
        return results

    numeric = pd.to_numeric(df[col], errors="coerce").dropna()
    if len(numeric) < 5:
        return results

    current_mean = float(numeric.mean())
    current_std = float(numeric.std())

    if col not in baseline:
        # First run — store baseline, no check to perform yet
        baseline[col] = {"mean": current_mean, "std": current_std}
        results.append(make_result(
            check_id, col, "statistical_drift", "PASS",
            f"baseline established: mean={current_mean:.4f}",
            "baseline run", "LOW",
            message="First run — baseline established."
        ))
        return results

    base_mean = baseline[col]["mean"]
    base_std = baseline[col].get("std", 1.0) or 1.0
    deviation = abs(current_mean - base_mean) / base_std

    # Special case: confidence scale flip (0.0-1.0 → 0-100)
    if "confidence" in col.lower() and current_mean > 1.5 and base_mean <= 1.0:
        results.append(make_result(
            check_id, col, "statistical_drift", "FAIL",
            f"mean={current_mean:.2f} (was {base_mean:.4f})",
            f"mean near {base_mean:.4f} (±3σ)",
            "CRITICAL", message=(
                f"CRITICAL: confidence scale change detected. "
                f"mean jumped from {base_mean:.4f} to {current_mean:.2f}. "
                f"Likely 0.0-1.0 → 0-100 breaking change."
            )
        ))
        return results

    if deviation > 3.0:
        status, severity = "FAIL", "HIGH"
        msg = (f"Statistical drift FAIL: mean={current_mean:.4f}, "
               f"baseline={base_mean:.4f}, deviation={deviation:.1f}σ > 3σ threshold.")
    elif deviation > 2.0:
        status, severity = "WARN", "MEDIUM"
        msg = (f"Statistical drift WARNING: mean={current_mean:.4f}, "
               f"baseline={base_mean:.4f}, deviation={deviation:.1f}σ > 2σ threshold.")
    else:
        status, severity = "PASS", "LOW"
        msg = (f"Statistical drift OK: mean={current_mean:.4f}, "
               f"baseline={base_mean:.4f}, deviation={deviation:.1f}σ.")

    results.append(make_result(
        check_id, col, "statistical_drift", status,
        f"mean={current_mean:.4f}, std={current_std:.4f}",
        f"within 2σ of baseline mean={base_mean:.4f}",
        severity, message=msg
    ))
    return results


def check_temporal_ordering(df, col_a, col_b, contract_id) -> dict:
    """Check that col_b >= col_a (e.g. recorded_at >= occurred_at)."""
    check_id = f"{contract_id}.{col_a}_vs_{col_b}.temporal_order"
    if col_a not in df.columns or col_b not in df.columns:
        missing = col_a if col_a not in df.columns else col_b
        return make_result(check_id, missing, "temporal_order", "ERROR",
                           "column missing", f"{col_b} >= {col_a}",
                           "CRITICAL", message=f"Column '{missing}' not found.")
    try:
        a = pd.to_datetime(df[col_a], errors="coerce", utc=True)
        b = pd.to_datetime(df[col_b], errors="coerce", utc=True)
        fail_mask = (b < a) & a.notna() & b.notna()
        fail_count = int(fail_mask.sum())
        if fail_count > 0:
            return make_result(check_id, col_b, "temporal_order", "FAIL",
                               f"{fail_count} records where {col_b} < {col_a}",
                               f"{col_b} >= {col_a}",
                               "HIGH", fail_count, [],
                               f"{fail_count} records violate {col_b} >= {col_a}.")
    except Exception as e:
        return make_result(check_id, col_b, "temporal_order", "ERROR",
                           str(e), f"{col_b} >= {col_a}",
                           "MEDIUM", message=str(e))
    return make_result(check_id, col_b, "temporal_order", "PASS",
                       f"all records: {col_b} >= {col_a}",
                       f"{col_b} >= {col_a}", "LOW")


def check_token_arithmetic(df, contract_id) -> dict:
    """total_tokens = prompt_tokens + completion_tokens (LangSmith traces)."""
    check_id = f"{contract_id}.token_arithmetic"
    cols = ["total_tokens", "prompt_tokens", "completion_tokens"]
    for c in cols:
        if c not in df.columns:
            return make_result(check_id, c, "token_arithmetic", "ERROR",
                               "column missing", "column exists",
                               "HIGH", message=f"Column '{c}' not found.")
    try:
        tt = pd.to_numeric(df["total_tokens"], errors="coerce")
        pt = pd.to_numeric(df["prompt_tokens"], errors="coerce")
        ct = pd.to_numeric(df["completion_tokens"], errors="coerce")
        diff = (tt - (pt + ct)).abs()
        fail_mask = (diff > 1) & tt.notna() & pt.notna() & ct.notna()
        fail_count = int(fail_mask.sum())
        if fail_count > 0:
            return make_result(check_id, "total_tokens", "token_arithmetic", "FAIL",
                               f"{fail_count} records where total != prompt+completion",
                               "total_tokens = prompt_tokens + completion_tokens",
                               "HIGH", fail_count, [],
                               f"{fail_count} records: total_tokens ≠ prompt+completion.")
    except Exception as e:
        return make_result(check_id, "total_tokens", "token_arithmetic", "ERROR",
                           str(e), "token arithmetic valid", "HIGH", message=str(e))
    return make_result(check_id, "total_tokens", "token_arithmetic", "PASS",
                       "total_tokens = prompt_tokens + completion_tokens",
                       "arithmetic valid", "LOW")


# ---------------------------------------------------------------------------
# Week-specific check suites
# ---------------------------------------------------------------------------

def checks_for_week(week_key: str, df: pd.DataFrame,
                     schema: dict, contract_id: str,
                     baseline: dict) -> list:
    results = []

    # --- Universal: not_null + unique on primary key ---
    pk = {
        "week1": "intent_id", "week2": "verdict_id",
        "week3": "doc_id", "week4": "snapshot_id",
        "week5": "event_id", "traces": "id"
    }.get(week_key, "id")

    results.append(check_not_null(df, pk, contract_id))
    results.append(check_unique(df, pk, contract_id))

    # --- Schema-driven checks ---
    for col, clause in schema.items():
        if col not in df.columns:
            if clause.get("required"):
                results.append(make_result(
                    f"{contract_id}.{col}.exists", col, "exists", "ERROR",
                    "column missing", "column present",
                    "CRITICAL", message=f"Required column '{col}' missing from dataset."
                ))
            continue

        if clause.get("required"):
            results.append(check_not_null(df, col, contract_id))

        if clause.get("unique") and col != pk:
            results.append(check_unique(df, col, contract_id))

        if "minimum" in clause or "maximum" in clause:
            mn = clause.get("minimum")
            mx = clause.get("maximum")
            results.append(check_range(df, col, mn, mx, contract_id))

        if "enum" in clause:
            results.append(check_enum(df, col, clause["enum"], contract_id))

        if "pattern" in clause:
            results.append(check_pattern(df, col, clause["pattern"], contract_id))

        if "format" in clause:
            results.append(check_format(df, col, clause["format"], contract_id))

        # Statistical drift on all numeric columns
        if clause.get("type") in ("number", "integer"):
            drift_results = check_statistical_drift(df, col, baseline, contract_id)
            results.extend(drift_results)

    # --- Row count ---
    results.append(check_row_count(df, 1, contract_id))

    # --- Week-specific ---
    if week_key == "week5":
        results.append(check_temporal_ordering(
            df, "occurred_at", "recorded_at", contract_id))

    if week_key == "traces":
        results.append(check_token_arithmetic(df, contract_id))
        results.append(check_temporal_ordering(
            df, "start_time", "end_time", contract_id))

    return results


# ---------------------------------------------------------------------------
# Report builder
# ---------------------------------------------------------------------------

def build_report(contract: dict, data_path: str,
                 results: list, snapshot_id: str) -> dict:
    total = len(results)
    passed = sum(1 for r in results if r["status"] == "PASS")
    failed = sum(1 for r in results if r["status"] == "FAIL")
    warned = sum(1 for r in results if r["status"] == "WARN")
    errored = sum(1 for r in results if r["status"] == "ERROR")

    return {
        "report_id": str(uuid.uuid4()),
        "contract_id": contract.get("id", "unknown"),
        "snapshot_id": snapshot_id,
        "data_path": data_path,
        "run_timestamp": now_iso(),
        "total_checks": total,
        "passed": passed,
        "failed": failed,
        "warned": warned,
        "errored": errored,
        "pass_rate": round(passed / total, 4) if total > 0 else 0.0,
        "results": results
    }


def detect_week_from_contract(contract: dict) -> str:
    cid = contract.get("id", "")
    if "week1" in cid:
        return "week1"
    if "week2" in cid:
        return "week2"
    if "week3" in cid:
        return "week3"
    if "week4" in cid:
        return "week4"
    if "week5" in cid:
        return "week5"
    if "langsmith" in cid or "trace" in cid:
        return "traces"
    return "unknown"


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

def apply_mode_decision(report: dict, mode: str) -> dict:
    """
    Apply enforcement mode decision to the report.
    AUDIT  — log only, never block. Add mode_decision: LOGGED.
    WARN   — block on CRITICAL only. Add mode_decision: BLOCKED or LOGGED.
    ENFORCE— block on CRITICAL and HIGH. Add mode_decision: BLOCKED or LOGGED.
    """
    critical_count = sum(
        1 for r in report["results"]
        if r["status"] == "FAIL" and r["severity"] == "CRITICAL"
    )
    high_count = sum(
        1 for r in report["results"]
        if r["status"] == "FAIL" and r["severity"] == "HIGH"
    )

    if mode == "AUDIT":
        decision = "LOGGED"
        reason = "AUDIT mode — violations logged, pipeline not blocked."
    elif mode == "WARN":
        if critical_count > 0:
            decision = "BLOCKED"
            reason = f"WARN mode — {critical_count} CRITICAL violation(s) detected. Pipeline blocked."
        else:
            decision = "LOGGED"
            reason = "WARN mode — no CRITICAL violations. Pipeline continues with warnings."
    elif mode == "ENFORCE":
        if critical_count > 0 or high_count > 0:
            decision = "BLOCKED"
            reason = (
                f"ENFORCE mode — {critical_count} CRITICAL and {high_count} HIGH "
                f"violation(s) detected. Pipeline blocked."
            )
        else:
            decision = "LOGGED"
            reason = "ENFORCE mode — no CRITICAL or HIGH violations. Pipeline continues."
    else:
        decision = "LOGGED"
        reason = f"Unknown mode '{mode}' — defaulting to AUDIT behaviour."

    report["enforcement_mode"] = mode
    report["mode_decision"] = decision
    report["mode_reason"] = reason
    return report


def run_validation(contract_path: str, data_path: str,
                   output_path: str = None, verbose: bool = False,
                   mode: str = "AUDIT"):

    if not Path(contract_path).exists():
        print(f"✗ Contract not found: {contract_path}")
        sys.exit(1)
    if not Path(data_path).exists():
        print(f"✗ Data not found: {data_path}")
        sys.exit(1)

    print(f"\n→ ValidationRunner")
    print(f"  contract : {contract_path}")
    print(f"  data     : {data_path}")

    # Load inputs
    contract = load_contract(contract_path)
    raw_records = load_jsonl(data_path)
    df = load_jsonl_flat(data_path)
    snapshot_id = sha256_file(data_path)
    contract_id = contract.get("id", "unknown")
    week_key = detect_week_from_contract(contract)

    print(f"  contract_id: {contract_id}")
    print(f"  rows loaded: {len(df)}")
    print(f"  snapshot_id: {snapshot_id[:16]}...")

    # Load / initialise statistical baselines
    baseline_path = "schema_snapshots/baselines.json"
    baselines = load_baselines(baseline_path)
    if contract_id not in baselines:
        baselines[contract_id] = {}
    baseline = baselines[contract_id]

    # Extract schema from contract
    schema = contract.get("schema", {})

    # Run all checks — never crash, always produce complete report
    results = []
    results.append(check_nested_array_confidence(raw_records, contract_id))
    try:
        results.extend(checks_for_week(week_key, df, schema, contract_id, baseline))
    except Exception as e:
        results.append(make_result(
            f"{contract_id}.runner_error", "unknown", "runner",
            "ERROR", str(e), "no exception",
            "CRITICAL", message=f"Runner encountered unexpected error: {e}"
        ))

    # Save updated baselines
    baselines[contract_id] = baseline
    save_baselines(baseline_path, baselines)

    # Build report
    report = build_report(contract, data_path, results, snapshot_id)

    # Apply enforcement mode
    report = apply_mode_decision(report, mode)

    # Determine output path
    if not output_path:
        ensure_dir("validation_reports")
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
        output_path = f"validation_reports/{week_key}_{ts}.json"

    ensure_dir(os.path.dirname(output_path))
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2)

    # Hard requirement: every FAIL must be attributed and persisted to the
    # immutable ledger via the attributor pipeline.
    fail_results = [r for r in results if r["status"] == "FAIL"]
    if fail_results:
        print("\n  invoking ViolationAttributor for immutable persistence...")
        violations = attribute_report(output_path)
        if len(violations) != len(fail_results):
            raise RuntimeError(
                "Hard fail: immutable persistence incomplete. "
                f"expected {len(fail_results)} attributed violations, "
                f"got {len(violations)}."
            )
        print(f"  ✓ immutable persistence complete: {len(violations)} violations appended")

    # Print summary
    print(f"\n  ── Validation Summary ──")
    print(f"  total checks : {report['total_checks']}")
    print(f"  passed       : {report['passed']}")
    print(f"  failed       : {report['failed']}")
    print(f"  warned       : {report['warned']}")
    print(f"  errored      : {report['errored']}")
    print(f"  pass rate    : {report['pass_rate']:.1%}")
    print(f"\n  report → {output_path}")

    # Print failures for visibility
    failures = [r for r in results if r["status"] in ("FAIL", "ERROR")]
    if failures and verbose:
        print(f"\n  ── Failures / Errors ──")
        for r in failures:
            print(f"  [{r['severity']}] {r['check_id']}")
            print(f"    {r['message']}")
    elif failures:
        print(f"\n  ── Failures ({len(failures)}) ──")
        for r in failures[:5]:
            print(f"  [{r['severity']}] {r['check_id']}: {r['message'][:80]}")
        if len(failures) > 5:
            print(f"  ... and {len(failures) - 5} more (use --verbose)")

    return report


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="ValidationRunner — execute contract checks against a dataset"
    )
    parser.add_argument(
        "--contract", required=True,
        help="Path to Bitol contract YAML"
    )
    parser.add_argument(
        "--data", required=True,
        help="Path to JSONL data file to validate"
    )
    parser.add_argument(
        "--output", type=str, default=None,
        help="Output path for validation report JSON (auto-generated if omitted)"
    )
    parser.add_argument(
        "--mode", type=str, default="AUDIT",
        choices=["AUDIT", "WARN", "ENFORCE"],
        help=(
            "Enforcement mode: "
            "AUDIT=log only never block; "
            "WARN=block on CRITICAL; "
            "ENFORCE=block on CRITICAL and HIGH (default: AUDIT)"
        )
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="Print all failures to stdout"
    )
    args = parser.parse_args()

    run_validation(
        contract_path=args.contract,
        data_path=args.data,
        output_path=args.output,
        verbose=args.verbose,
        mode=args.mode
    )


if __name__ == "__main__":
    main()
