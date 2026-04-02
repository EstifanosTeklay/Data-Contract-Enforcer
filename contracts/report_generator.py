"""
contracts/report_generator.py
==============================
ReportGenerator — Phase 4B of the Data Contract Enforcer.

Auto-generates the Enforcer Report from live validation data.
Reads from:
  - violation_log/violations.jsonl
  - validation_reports/*.json
  - validation_reports/ai_metrics.json
  - schema_snapshots/

Produces:
  - enforcer_report/report_data.json  (machine-readable)
  - enforcer_report/report_{date}.md  (human-readable, embeddable in PDF)

Usage:
    python contracts/report_generator.py
    python contracts/report_generator.py --output-dir enforcer_report/

Requirements:
    pip install pyyaml
"""

import argparse
import json
import os
import glob
from datetime import datetime, timezone
from pathlib import Path

import yaml

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

VIOLATION_LOG = "violation_log/violations.jsonl"
VALIDATION_REPORTS_DIR = "validation_reports"
AI_METRICS_PATH = "validation_reports/ai_metrics.json"
SCHEMA_SNAPSHOTS_DIR = "schema_snapshots"
OUTPUT_DIR = "enforcer_report"

SEVERITY_WEIGHTS = {
    "CRITICAL": 20,
    "HIGH": 10,
    "MEDIUM": 5,
    "LOW": 1,
    "WARNING": 2,
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def today_str():
    return datetime.now(timezone.utc).strftime("%Y%m%d")

def ensure_dir(p: str):
    Path(p).mkdir(parents=True, exist_ok=True)

def load_jsonl(path: str) -> list:
    records = []
    if not Path(path).exists():
        return records
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return records

def load_json(path: str) -> dict:
    if not Path(path).exists():
        return {}
    with open(path) as f:
        return json.load(f)

def load_all_validation_reports() -> list:
    """Load all validation report JSONs from validation_reports/."""
    reports = []
    pattern = os.path.join(VALIDATION_REPORTS_DIR, "week*.json")
    for path in sorted(glob.glob(pattern)):
        try:
            with open(path) as f:
                reports.append(json.load(f))
        except Exception:
            continue
    return reports

def load_schema_snapshots_summary() -> list:
    """Summarise schema evolution snapshots across all contracts."""
    summaries = []
    snap_dir = Path(SCHEMA_SNAPSHOTS_DIR)
    if not snap_dir.exists():
        return summaries
    for contract_dir in sorted(snap_dir.iterdir()):
        if not contract_dir.is_dir():
            continue
        snapshots = sorted(contract_dir.glob("*.yaml"))
        summaries.append({
            "contract_id": contract_dir.name,
            "snapshot_count": len(snapshots),
            "latest": str(snapshots[-1]) if snapshots else None,
            "oldest": str(snapshots[0]) if snapshots else None,
        })
    return summaries

# ---------------------------------------------------------------------------
# Section 1 — Data Health Score
# ---------------------------------------------------------------------------

def compute_health_score(violations: list, reports: list) -> dict:
    """
    Formula: (checks_passed / total_checks) * 100
    Adjusted down by 20 points for each CRITICAL violation.
    """
    total_checks = sum(r.get("total_checks", 0) for r in reports)
    total_passed = sum(r.get("passed", 0) for r in reports)
    total_failed = sum(r.get("failed", 0) for r in reports)
    total_warned = sum(r.get("warned", 0) for r in reports)
    total_errored = sum(r.get("errored", 0) for r in reports)

    if total_checks == 0:
        base_score = 0.0
    else:
        base_score = (total_passed / total_checks) * 100

    # Count CRITICAL violations (excluding injected ones)
    critical_violations = [
        v for v in violations
        if v.get("severity") == "CRITICAL" and not v.get("injected", False)
    ]
    critical_penalty = len(critical_violations) * 20
    final_score = max(0.0, min(100.0, base_score - critical_penalty))

    # Narrative
    if final_score >= 90:
        narrative = "Data systems are healthy. No critical violations detected."
    elif final_score >= 70:
        narrative = (
            f"Data systems are mostly healthy with {len(critical_violations)} "
            f"critical violation(s) requiring attention."
        )
    elif final_score >= 50:
        narrative = (
            f"Data quality at risk. {len(critical_violations)} critical violation(s) "
            f"and {total_failed} total failed checks detected."
        )
    else:
        narrative = (
            f"Data systems require immediate attention. "
            f"{len(critical_violations)} critical violations detected across "
            f"{len(reports)} monitored contracts."
        )

    return {
        "data_health_score": round(final_score, 1),
        "base_score": round(base_score, 1),
        "critical_penalty": critical_penalty,
        "narrative": narrative,
        "total_checks": total_checks,
        "total_passed": total_passed,
        "total_failed": total_failed,
        "total_warned": total_warned,
        "total_errored": total_errored,
        "critical_violations": len(critical_violations),
        "contracts_monitored": len(reports),
    }

# ---------------------------------------------------------------------------
# Section 2 — Violations This Week
# ---------------------------------------------------------------------------

def summarise_violations(violations: list) -> dict:
    """
    Count by severity and describe top 3 most significant violations
    in plain language.
    """
    # Count by severity
    by_severity = {}
    for v in violations:
        sev = v.get("severity", "LOW")
        by_severity[sev] = by_severity.get(sev, 0) + 1

    # Sort by severity weight
    sorted_violations = sorted(
        violations,
        key=lambda v: SEVERITY_WEIGHTS.get(v.get("severity", "LOW"), 0),
        reverse=True
    )

    top3 = []
    for v in sorted_violations[:3]:
        contract_id = v.get("contract_id", "unknown")
        field = v.get("failing_field", "unknown")
        severity = v.get("severity", "unknown")
        message = v.get("message", "")
        check_type = v.get("check_type", "unknown")
        injected = v.get("injected", False)

        # Plain-language description
        system_name = _contract_to_system_name(contract_id)
        downstream = v.get("blast_radius", {}).get("affected_pipelines", [])
        downstream_str = (
            f"Downstream systems affected: {', '.join(downstream)}."
            if downstream else
            "No downstream systems directly affected."
        )

        top3.append({
            "violation_id": v.get("violation_id", "unknown"),
            "severity": severity,
            "system": system_name,
            "field": field,
            "check_type": check_type,
            "injected": injected,
            "plain_language": (
                f"{'[INJECTED TEST] ' if injected else ''}"
                f"The {system_name} produced data where the field "
                f"'{field}' failed a {check_type} check. "
                f"{message[:150]} "
                f"{downstream_str}"
            ),
        })

    return {
        "total_violations": len(violations),
        "by_severity": by_severity,
        "top_3_violations": top3,
    }


def _contract_to_system_name(contract_id: str) -> str:
    mapping = {
        "week3-document-refinery-extractions": "Week 3 Document Refinery",
        "week5-event-records": "Week 5 Event Sourcing Platform",
        "week2-verdict-records": "Week 2 Digital Courtroom",
        "week4-lineage-snapshots": "Week 4 Brownfield Cartographer",
        "week1-intent-records": "Week 1 Intent-Code Correlator",
        "langsmith-trace-records": "LangSmith Trace Records",
    }
    return mapping.get(contract_id, contract_id)

# ---------------------------------------------------------------------------
# Section 3 — Schema Changes
# ---------------------------------------------------------------------------

def summarise_schema_changes() -> dict:
    """Load migration impact reports and summarise schema changes."""
    changes = []

    # Look for migration impact reports
    impact_dir = Path("migration_impact_reports")
    if impact_dir.exists():
        for report_file in sorted(impact_dir.glob("*.json")):
            try:
                with open(report_file) as f:
                    impact = json.load(f)
                verdict = impact.get("compatibility_verdict", "UNKNOWN")
                breaking = impact.get("breaking_changes", 0)
                contract = impact.get("contract_id", "unknown")
                changes_list = impact.get("changes_detail",
                               impact.get("changes", []))

                changes.append({
                    "contract_id": contract,
                    "system": _contract_to_system_name(contract),
                    "compatibility_verdict": verdict,
                    "breaking_changes": breaking,
                    "total_changes": impact.get("total_changes", 0),
                    "action_required": (
                        "Immediate action required — breaking changes detected."
                        if verdict == "BREAKING" else
                        "No action required — all changes are backward compatible."
                    ),
                    "change_summary": [
                        f"{c.get('change_type', 'UNKNOWN')}: "
                        f"{c.get('field', 'unknown')} "
                        f"({c.get('old_value', '')} → {c.get('new_value', '')})"
                        for c in changes_list[:5]
                    ],
                })
            except Exception:
                continue

    if not changes:
        changes.append({
            "contract_id": "all",
            "system": "All systems",
            "compatibility_verdict": "NO_CHANGES_DETECTED",
            "breaking_changes": 0,
            "total_changes": 0,
            "action_required": (
                "No schema changes detected in the past 7 days. "
                "Run schema_analyzer.py --inject-change to simulate evolution."
            ),
            "change_summary": [],
        })

    return {"schema_changes": changes}

# ---------------------------------------------------------------------------
# Section 4 — AI System Risk Assessment
# ---------------------------------------------------------------------------

def summarise_ai_risk() -> dict:
    """Summarise AI contract extension results."""
    metrics = load_json(AI_METRICS_PATH)
    if not metrics:
        return {
            "overall_ai_risk": "UNKNOWN",
            "message": "AI metrics not yet generated. Run: python contracts/ai_extensions.py --all",
            "embedding_drift": None,
            "prompt_validation": None,
            "output_schema": None,
        }

    drift = metrics.get("embedding_drift", {})
    prompt = metrics.get("prompt_input_validation", {})
    output = metrics.get("output_schema_violation_rate", {})

    drift_status = drift.get("status", "UNKNOWN")
    drift_score = drift.get("drift_score", None)
    prompt_status = prompt.get("status", "UNKNOWN")
    output_status = output.get("status", "UNKNOWN")
    output_trend = output.get("trend", "unknown")

    # Plain-language assessment
    if drift_status == "BASELINE_SET":
        drift_narrative = (
            "Embedding baseline has been established on this run. "
            "Drift detection will be active from the next run onwards."
        )
    elif drift_status == "PASS":
        drift_narrative = (
            f"Embedding drift is within acceptable bounds "
            f"(score={drift_score:.4f}, threshold={drift.get('threshold', 0.15)})."
        )
    elif drift_status == "FAIL":
        drift_narrative = (
            f"ALERT: Embedding drift detected (score={drift_score:.4f}). "
            f"The semantic distribution of extracted facts has shifted significantly. "
            f"Review recent changes to the document corpus or extraction model."
        )
    else:
        drift_narrative = "Embedding drift status unknown."

    if output_trend == "rising":
        output_narrative = (
            f"WARNING: LLM output schema violation rate is rising "
            f"({output.get('violation_rate', 0):.2%}). "
            f"This may indicate prompt degradation or model behaviour change."
        )
    else:
        output_narrative = (
            f"LLM output schema violation rate is stable "
            f"({output.get('violation_rate', 0):.2%})."
        )

    return {
        "overall_ai_risk": metrics.get("overall_ai_risk", "UNKNOWN"),
        "embedding_drift": {
            "status": drift_status,
            "drift_score": drift_score,
            "threshold": drift.get("threshold", 0.15),
            "narrative": drift_narrative,
        },
        "prompt_validation": {
            "status": prompt_status,
            "violation_rate": prompt.get("violation_rate", 0),
            "quarantined": prompt.get("quarantined_count", 0),
            "narrative": prompt.get("message", ""),
        },
        "output_schema": {
            "status": output_status,
            "violation_rate": output.get("violation_rate", 0),
            "trend": output_trend,
            "narrative": output_narrative,
        },
    }

# ---------------------------------------------------------------------------
# Section 5 — Recommended Actions
# ---------------------------------------------------------------------------

def generate_recommendations(violations: list, health: dict,
                               ai_risk: dict, schema_changes: dict) -> list:
    """
    Generate top 3 prioritised, specific actions.
    Ordered by risk reduction value.
    """
    actions = []

    # Action from CRITICAL violations
    critical = [
        v for v in violations
        if v.get("severity") == "CRITICAL" and not v.get("injected", False)
    ]
    if critical:
        top = critical[0]
        field = top.get("failing_field", "unknown")
        contract = top.get("contract_id", "unknown")
        blame = top.get("blame_chain", [{}])
        file_path = blame[0].get("file_path", "unknown") if blame else "unknown"
        actions.append({
            "priority": 1,
            "risk_level": "CRITICAL",
            "action": (
                f"Fix {field} in {file_path}: update the field to conform to "
                f"contract {contract}. "
                f"Current value: {top.get('actual_value', 'unknown')}. "
                f"Expected: {top.get('expected', 'unknown')}."
            ),
            "system": _contract_to_system_name(contract),
            "estimated_impact": "Eliminates CRITICAL violation affecting downstream consumers.",
        })

    # Action from schema changes
    breaking_changes = [
        c for c in schema_changes.get("schema_changes", [])
        if c.get("compatibility_verdict") == "BREAKING"
    ]
    if breaking_changes:
        bc = breaking_changes[0]
        actions.append({
            "priority": len(actions) + 1,
            "risk_level": "HIGH",
            "action": (
                f"Coordinate schema migration for {bc['system']}: "
                f"{bc['breaking_changes']} breaking change(s) detected. "
                f"Notify registry subscribers before deploying. "
                f"Changes: {'; '.join(bc['change_summary'][:2])}"
            ),
            "system": bc["system"],
            "estimated_impact": "Prevents silent data corruption in downstream consumers.",
        })

    # Action from AI risk
    drift = ai_risk.get("embedding_drift", {})
    output = ai_risk.get("output_schema", {})
    if drift.get("status") == "FAIL":
        actions.append({
            "priority": len(actions) + 1,
            "risk_level": "HIGH",
            "action": (
                f"Investigate embedding drift in Week 3 Document Refinery: "
                f"drift score {drift.get('drift_score', 'unknown')} exceeds threshold "
                f"{drift.get('threshold', 0.15)}. "
                f"Review recent changes to document corpus or extraction model "
                f"(src/agents/extractor.py). Re-establish embedding baseline after fix."
            ),
            "system": "Week 3 Document Refinery",
            "estimated_impact": "Prevents semantic drift from corrupting downstream search and retrieval.",
        })
    elif output.get("trend") == "rising":
        actions.append({
            "priority": len(actions) + 1,
            "risk_level": "MEDIUM",
            "action": (
                f"Investigate rising LLM output schema violation rate in Week 2 "
                f"Digital Courtroom: rate={output.get('violation_rate', 0):.2%}. "
                f"Review recent prompt changes or model version updates."
            ),
            "system": "Week 2 Digital Courtroom",
            "estimated_impact": "Prevents structured output failures from propagating to consumers.",
        })

    # Default action if we have fewer than 3
    defaults = [
        {
            "priority": 99,
            "risk_level": "LOW",
            "action": (
                "Run python contracts/generator.py --all --annotate weekly to keep "
                "contracts current with actual data distributions. "
                "Stale contracts produce false positives that erode team trust in enforcement."
            ),
            "system": "All systems",
            "estimated_impact": "Prevents contract staleness — the most common enforcement failure mode.",
        },
        {
            "priority": 99,
            "risk_level": "LOW",
            "action": (
                "Add real LangSmith traces by enabling tracing in Week 2 and Week 3 "
                "and replacing outputs/traces/runs.jsonl with the exported data. "
                "Current traces are synthetic — AI extension metrics are not production-representative."
            ),
            "system": "LangSmith Traces",
            "estimated_impact": "Makes AI contract extension metrics meaningful for production monitoring.",
        },
        {
            "priority": 99,
            "risk_level": "LOW",
            "action": (
                "Run python contracts/schema_analyzer.py "
                "--contract-id week5-event-records --inject-change "
                "to verify schema evolution detection is working end-to-end."
            ),
            "system": "Week 5 Event Sourcing Platform",
            "estimated_impact": "Validates the SchemaEvolutionAnalyzer is operational before a real change occurs.",
        },
    ]

    while len(actions) < 3:
        actions.append(defaults[len(actions) - len(actions)])
        if not defaults:
            break
        default = defaults.pop(0)
        default["priority"] = len(actions) + 1
        actions.append(default)
        if len(actions) >= 3:
            break

    # Re-number priorities
    for i, a in enumerate(actions[:3]):
        a["priority"] = i + 1

    return actions[:3]

# ---------------------------------------------------------------------------
# Report builder
# ---------------------------------------------------------------------------

def build_report() -> dict:
    """Build the complete enforcer report from live data."""
    print("\n→ ReportGenerator")

    # Load all data sources
    violations = load_jsonl(VIOLATION_LOG)
    reports = load_all_validation_reports()
    snapshots = load_schema_snapshots_summary()
    ai_risk = summarise_ai_risk()
    schema_changes = summarise_schema_changes()

    print(f"  violations loaded:   {len(violations)}")
    print(f"  validation reports:  {len(reports)}")
    print(f"  schema snapshots:    {len(snapshots)} contracts tracked")

    # Build each section
    health = compute_health_score(violations, reports)
    violation_summary = summarise_violations(violations)
    recommendations = generate_recommendations(
        violations, health, ai_risk, schema_changes
    )

    report = {
        "report_id": f"enforcer-report-{today_str()}",
        "generated_at": now_iso(),
        "generated_by": "contracts/report_generator.py",
        "report_period": "past 7 days",
        "section_1_data_health": health,
        "section_2_violations": violation_summary,
        "section_3_schema_changes": schema_changes,
        "section_4_ai_risk": ai_risk,
        "section_5_recommendations": recommendations,
        "metadata": {
            "contracts_monitored": len(reports),
            "schema_snapshots_tracked": len(snapshots),
            "violation_log_entries": len(violations),
            "ai_metrics_available": bool(ai_risk.get("overall_ai_risk") != "UNKNOWN"),
        }
    }

    return report

# ---------------------------------------------------------------------------
# Markdown report writer
# ---------------------------------------------------------------------------

def write_markdown_report(report: dict, output_dir: str):
    """Write a human-readable markdown report."""
    health = report["section_1_data_health"]
    violations = report["section_2_violations"]
    schema = report["section_3_schema_changes"]
    ai = report["section_4_ai_risk"]
    recs = report["section_5_recommendations"]

    score = health["data_health_score"]
    score_emoji = "[HEALTHY]" if score >= 80 else "[AT RISK]" if score >= 60 else "[CRITICAL]"

    lines = [
        f"# Data Contract Enforcer Report",
        f"**Generated:** {report['generated_at']}  ",
        f"**Period:** {report['report_period']}  ",
        f"**Generated by:** {report['generated_by']}",
        f"",
        f"---",
        f"",
        f"## Section 1 — Data Health Score",
        f"",
        f"### {score_emoji} Overall Score: **{score}/100**",
        f"",
        f"{health['narrative']}",
        f"",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Total checks run | {health['total_checks']} |",
        f"| Checks passed | {health['total_passed']} |",
        f"| Checks failed | {health['total_failed']} |",
        f"| Checks warned | {health['total_warned']} |",
        f"| Critical violations | {health['critical_violations']} |",
        f"| Critical penalty | -{health['critical_penalty']} points |",
        f"| Contracts monitored | {health['contracts_monitored']} |",
        f"",
        f"---",
        f"",
        f"## Section 2 — Violations This Week",
        f"",
        f"**Total violations:** {violations['total_violations']}",
        f"",
    ]

    # Severity breakdown
    if violations["by_severity"]:
        lines.append("| Severity | Count |")
        lines.append("|----------|-------|")
        for sev, count in sorted(
            violations["by_severity"].items(),
            key=lambda x: SEVERITY_WEIGHTS.get(x[0], 0), reverse=True
        ):
            lines.append(f"| {sev} | {count} |")
        lines.append("")

    lines.append("### Top 3 Most Significant Violations")
    lines.append("")
    for i, v in enumerate(violations["top_3_violations"], 1):
        injected_tag = " *(injected test)*" if v.get("injected") else ""
        lines.extend([
            f"**{i}. [{v['severity']}] {v['system']} — {v['field']}**{injected_tag}",
            f"",
            f"{v['plain_language']}",
            f"",
        ])

    lines.extend([
        f"---",
        f"",
        f"## Section 3 — Schema Changes Detected",
        f"",
    ])

    for change in schema["schema_changes"]:
        verdict_emoji = "[BREAKING]" if change["compatibility_verdict"] == "BREAKING" else "[COMPATIBLE]"
        lines.extend([
            f"### {verdict_emoji} {change['system']}",
            f"**Compatibility verdict:** {change['compatibility_verdict']}  ",
            f"**Breaking changes:** {change['breaking_changes']}  ",
            f"**Action required:** {change['action_required']}",
            f"",
        ])
        if change.get("change_summary"):
            lines.append("Changes detected:")
            for cs in change["change_summary"]:
                lines.append(f"- {cs}")
            lines.append("")

    lines.extend([
        f"---",
        f"",
        f"## Section 4 — AI System Risk Assessment",
        f"",
        f"**Overall AI Risk:** {ai.get('overall_ai_risk', 'UNKNOWN')}",
        f"",
    ])

    if ai.get("embedding_drift"):
        d = ai["embedding_drift"]
        lines.extend([
            f"### Embedding Drift (Week 3 Document Refinery)",
            f"**Status:** {d['status']}  ",
            f"**Drift score:** {d.get('drift_score', 'N/A')} "
            f"(threshold: {d.get('threshold', 0.15)})  ",
            f"{d['narrative']}",
            f"",
        ])

    if ai.get("prompt_validation"):
        p = ai["prompt_validation"]
        lines.extend([
            f"### Prompt Input Schema Validation",
            f"**Status:** {p['status']}  ",
            f"**Violation rate:** {p.get('violation_rate', 0):.2%}  ",
            f"**Quarantined records:** {p.get('quarantined', 0)}  ",
            f"{p['narrative']}",
            f"",
        ])

    if ai.get("output_schema"):
        o = ai["output_schema"]
        lines.extend([
            f"### LLM Output Schema Violation Rate",
            f"**Status:** {o['status']}  ",
            f"**Violation rate:** {o.get('violation_rate', 0):.2%}  ",
            f"**Trend:** {o.get('trend', 'unknown')}  ",
            f"{o['narrative']}",
            f"",
        ])

    lines.extend([
        f"---",
        f"",
        f"## Section 5 — Recommended Actions",
        f"",
    ])

    for rec in recs:
        lines.extend([
            f"### Priority {rec['priority']} — [{rec['risk_level']}] {rec['system']}",
            f"",
            f"**Action:** {rec['action']}",
            f"",
            f"**Expected impact:** {rec['estimated_impact']}",
            f"",
        ])

    lines.extend([
        f"---",
        f"",
        f"*This report was auto-generated by contracts/report_generator.py "
        f"from live validation data. It was not hand-written.*",
    ])

    md_path = os.path.join(output_dir, f"report_{today_str()}.md")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"  ✓ markdown report → {md_path}")
    return md_path

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="ReportGenerator — auto-generate Enforcer Report from live data"
    )
    parser.add_argument(
        "--output-dir", type=str, default=OUTPUT_DIR,
        help=f"Output directory (default: {OUTPUT_DIR}/)"
    )
    args = parser.parse_args()

    ensure_dir(args.output_dir)

    # Build report
    report = build_report()

    # Write report_data.json
    json_path = os.path.join(args.output_dir, "report_data.json")
    with open(json_path, "w") as f:
        json.dump(report, f, indent=2, default=str)

    score = report["section_1_data_health"]["data_health_score"]
    print(f"  ✓ report_data.json → {json_path}")
    print(f"  data_health_score: {score}/100")

    # Write markdown report
    write_markdown_report(report, args.output_dir)

    # Summary
    print(f"\n  ── Enforcer Report Summary ──")
    print(f"  health score    : {score}/100")
    print(f"  violations      : {report['section_2_violations']['total_violations']}")
    print(f"  ai risk         : {report['section_4_ai_risk'].get('overall_ai_risk', 'UNKNOWN')}")
    print(f"  recommendations : {len(report['section_5_recommendations'])}")
    print(f"\n=== ReportGenerator complete ===")


if __name__ == "__main__":
    main()
