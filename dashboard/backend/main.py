import asyncio
import json
import os
from datetime import datetime
from pathlib import Path
from time import monotonic
from typing import Any, AsyncGenerator

import yaml
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.responses import JSONResponse, StreamingResponse
try:
    from watchfiles import awatch
except Exception:  # pragma: no cover
    awatch = None

try:
    import asyncpg
except Exception:  # pragma: no cover
    asyncpg = None


app = FastAPI(title="Data Contract Enforcer API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ROOT_DIR = Path(__file__).resolve().parents[2]
VIOLATIONS_FILE = ROOT_DIR / "violation_log" / "violations.jsonl"
VALIDATION_DIR = ROOT_DIR / "validation_reports"
MIGRATION_DIR = ROOT_DIR / "migration_impact_reports"
CONTRACTS_DIR = ROOT_DIR / "generated_contracts"
ENFORCER_REPORT_DIR = ROOT_DIR / "enforcer_report"

_VIOLATIONS_CACHE_TTL_SEC = 5.0
_violations_cache: dict[str, Any] = {
    "expires_at": 0.0,
    "items": [],
}
_db_retry_after = 0.0


# Fall back to parsed datetime minimum when timestamps are absent.
def _dt(value: str | None) -> datetime:
    if not value:
        return datetime.min
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return datetime.min


def _read_json(path: Path, fallback: Any) -> Any:
    if not path.exists() or not path.is_file():
        return fallback
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return fallback


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or not path.is_file():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                if isinstance(data, dict):
                    rows.append(data)
            except json.JSONDecodeError:
                continue
    return rows


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _normalize_violation(v: dict[str, Any]) -> dict[str, Any]:
    violation_id = str(v.get("violation_id") or v.get("id") or "")
    detected_at = v.get("detected_at") or v.get("timestamp")
    blast_radius = v.get("blast_radius") or {}
    affected_pipelines = blast_radius.get("affected_pipelines") or v.get("affected_pipelines") or []
    if not isinstance(affected_pipelines, list):
        affected_pipelines = [str(affected_pipelines)]

    raw_affected_nodes = blast_radius.get("affected_nodes_count") or blast_radius.get("affected_nodes")
    if isinstance(raw_affected_nodes, list):
        affected_nodes_count = len(raw_affected_nodes)
    else:
        affected_nodes_count = _safe_int(raw_affected_nodes, len(affected_pipelines))

    raw_records_failing = v.get("records_failing")
    if isinstance(raw_records_failing, list):
        records_failing = len(raw_records_failing)
    else:
        records_failing = _safe_int(raw_records_failing, 0)

    return {
        "violation_id": violation_id or f"anon-{hash(json.dumps(v, sort_keys=True, default=str))}",
        "severity": str(v.get("severity") or "LOW").upper(),
        "system": v.get("system") or v.get("domain") or "unknown",
        "failing_field": v.get("failing_field") or v.get("field") or "unknown",
        "check_type": v.get("check_type") or v.get("check") or "unknown",
        "records_failing": records_failing,
        "detected_at": detected_at,
        "injected": bool(v.get("injected", False)),
        "message": v.get("message") or v.get("violation_message") or "No description provided.",
        "blame_chain": v.get("blame_chain") or [],
        "blast_radius": {
            "affected_pipelines": affected_pipelines,
            "affected_nodes_count": affected_nodes_count,
            "mode": blast_radius.get("mode") or v.get("mode") or "ENFORCE",
        },
    }


async def _read_db_violations() -> list[dict[str, Any]]:
    global _db_retry_after

    database_url = os.getenv("DATABASE_URL")
    if not database_url or asyncpg is None:
        return []

    if monotonic() < _db_retry_after:
        return []

    connection: asyncpg.Connection | None = None
    try:
        connection = await asyncpg.connect(database_url, timeout=0.8, command_timeout=1)
        rows = await connection.fetch("SELECT to_jsonb(t) AS row FROM event_ledger t LIMIT 5000")

        normalized: list[dict[str, Any]] = []
        for item in rows:
            raw = dict(item.get("row") or {})
            payload = raw.get("payload") or raw.get("data") or raw

            if not isinstance(payload, dict):
                continue

            if not (
                payload.get("violation_id")
                or str(payload.get("event_type", "")).lower().find("violation") >= 0
            ):
                continue

            normalized.append(_normalize_violation(payload))

        return normalized
    except Exception:
        # Avoid repeated connection attempts on every request when DB is unreachable.
        _db_retry_after = monotonic() + 15
        return []
    finally:
        if connection is not None:
            await connection.close()


def _dedupe_and_sort_violations(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    for row in rows:
        normalized = _normalize_violation(row)
        deduped[normalized["violation_id"]] = normalized

    violations = list(deduped.values())
    violations.sort(key=lambda x: _dt(x.get("detected_at")), reverse=True)
    return violations


def _latest_validation_report() -> dict[str, Any]:
    if not VALIDATION_DIR.exists():
        return {}
    files = sorted(
        [p for p in VALIDATION_DIR.glob("*.json") if p.name != "ai_metrics.json"],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not files:
        return {}
    return _read_json(files[0], {})


def _latest_file(paths: list[Path]) -> Path | None:
    if not paths:
        return None
    return sorted(paths, key=lambda p: p.stat().st_mtime, reverse=True)[0]


def _load_enforcement_report() -> dict[str, Any]:
    if not ENFORCER_REPORT_DIR.exists():
        return {"generated_at": None, "data": {}, "markdown": ""}

    report_json = _latest_file(
        [p for p in ENFORCER_REPORT_DIR.glob("*.json") if p.is_file()]
    )
    report_md = _latest_file(
        [p for p in ENFORCER_REPORT_DIR.glob("*.md") if p.is_file()]
    )

    data = _read_json(report_json, {}) if report_json else {}
    markdown = ""
    if report_md:
        try:
            markdown = report_md.read_text(encoding="utf-8")
        except Exception:
            markdown = ""

    return {
        "generated_at": data.get("generated_at") if isinstance(data, dict) else None,
        "report_id": data.get("report_id") if isinstance(data, dict) else None,
        "data": data if isinstance(data, dict) else {},
        "markdown": markdown,
        "source_json": report_json.name if report_json else None,
        "source_markdown": report_md.name if report_md else None,
    }


def _load_schema_changes() -> list[dict[str, Any]]:
    if not MIGRATION_DIR.exists():
        return []

    out: list[dict[str, Any]] = []
    for path in sorted(MIGRATION_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True):
        report = _read_json(path, {})
        if isinstance(report, dict) and isinstance(report.get("changes"), list):
            for idx, change in enumerate(report["changes"]):
                out.append(
                    {
                        "id": f"{path.stem}-{idx}",
                        "contract_name": change.get("contract_name") or report.get("contract") or path.stem,
                        "change_type": change.get("change_type") or change.get("type") or "UPDATED",
                        "field_changed": change.get("field_changed") or change.get("field") or "unknown",
                        "before": change.get("before"),
                        "after": change.get("after"),
                        "compatibility": (
                            "BREAKING"
                            if bool(change.get("breaking", False))
                            else str(change.get("compatibility") or "COMPATIBLE").upper()
                        ),
                        "affected_consumers": int(change.get("affected_consumers") or 0),
                        "migration_checklist": change.get("migration_checklist")
                        or report.get("migration_checklist")
                        or [],
                        "timestamp": change.get("timestamp") or report.get("generated_at"),
                    }
                )
        elif isinstance(report, dict):
            out.append(
                {
                    "id": path.stem,
                    "contract_name": report.get("contract") or path.stem,
                    "change_type": report.get("change_type") or "UPDATED",
                    "field_changed": report.get("field") or "unknown",
                    "before": report.get("before"),
                    "after": report.get("after"),
                    "compatibility": str(report.get("compatibility") or "COMPATIBLE").upper(),
                    "affected_consumers": int(report.get("affected_consumers") or 0),
                    "migration_checklist": report.get("migration_checklist") or [],
                    "timestamp": report.get("generated_at"),
                }
            )

    out.sort(key=lambda x: _dt(x.get("timestamp")), reverse=True)
    return out


def _load_contracts() -> list[dict[str, Any]]:
    if not CONTRACTS_DIR.exists():
        return []

    contracts: list[dict[str, Any]] = []
    candidates = list(CONTRACTS_DIR.glob("*.yaml")) + list(CONTRACTS_DIR.glob("*.yml"))

    for path in sorted(candidates):
        parsed: dict[str, Any] = {}
        try:
            yaml_text = path.read_text(encoding="utf-8")
            parsed = yaml.safe_load(yaml_text) or {}
            owner = parsed.get("owner") or parsed.get("metadata", {}).get("owner") or "unknown"
            clauses = parsed.get("clauses") or parsed.get("checks") or []
            pass_rate = float(parsed.get("pass_rate") or parsed.get("stats", {}).get("pass_rate") or 0)
            last_validated = parsed.get("last_validated") or parsed.get("updated_at")
        except Exception:
            yaml_text = path.read_text(encoding="utf-8", errors="ignore")
            owner = "unknown"
            clauses = []
            pass_rate = 0
            last_validated = None

        clause_preview: list[str] = []
        if isinstance(clauses, list):
            for clause in clauses[:6]:
                if isinstance(clause, dict):
                    field_name = clause.get("field") or clause.get("column") or clause.get("name") or "field"
                    check_type = clause.get("check") or clause.get("type") or "rule"
                    clause_preview.append(f"{field_name}: {check_type}")
                else:
                    clause_preview.append(str(clause))

        summary_lines = [
            f"Contract {path.stem} is owned by {owner}.",
            f"It contains {len(clauses)} clause(s) and current pass rate is {round(pass_rate * 100)}%.",
        ]
        if last_validated:
            summary_lines.append(f"Last validated at {last_validated}.")
        if clause_preview:
            summary_lines.append("Key rules:")
            summary_lines.extend([f"- {item}" for item in clause_preview])
        human_summary = "\n".join(summary_lines)

        dbt_counterpart = None
        counterpart_path = path.with_name(f"{path.stem}_dbt.yml")
        if counterpart_path.exists():
            dbt_counterpart = counterpart_path.name

        contracts.append(
            {
                "contract_id": path.stem,
                "owner": owner,
                "clause_count": len(clauses),
                "last_validated": last_validated,
                "pass_rate": max(0, min(pass_rate, 1.0)),
                "yaml": yaml_text,
                "human_summary": human_summary,
                "dbt_counterpart": dbt_counterpart,
            }
        )

    return contracts


async def _merged_violations() -> list[dict[str, Any]]:
    now = monotonic()
    if _violations_cache["expires_at"] > now:
        return _violations_cache["items"]

    file_violations = _read_jsonl(VIOLATIONS_FILE)
    db_violations = await _read_db_violations()
    merged = _dedupe_and_sort_violations([*file_violations, *db_violations])
    _violations_cache["items"] = merged
    _violations_cache["expires_at"] = now + _VIOLATIONS_CACHE_TTL_SEC
    return merged


@app.get("/api/health")
async def get_health() -> JSONResponse:
    violations = await _merged_violations()
    latest_report = _latest_validation_report()
    contracts = _load_contracts()

    failed = len(violations)
    total_checks = int(latest_report.get("total_checks") or (failed + int(latest_report.get("passed", 0))))
    passed = int(latest_report.get("passed") or max(total_checks - failed, 0))
    score = int(latest_report.get("health_score") or (100 if total_checks == 0 else round((passed / max(total_checks, 1)) * 100)))

    severity_counts = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0}
    for violation in violations:
        severity = violation.get("severity", "LOW")
        severity_counts[severity] = severity_counts.get(severity, 0) + 1

    payload = {
        "health_score": max(0, min(score, 100)),
        "summary": {
            "total_checks": total_checks,
            "passed": passed,
            "failed": failed,
            "contracts_monitored": len(contracts),
        },
        "violations_by_severity": severity_counts,
        "last_updated": datetime.utcnow().isoformat() + "Z",
    }
    return JSONResponse(payload)


@app.get("/api/violations")
async def get_violations() -> JSONResponse:
    violations = await _merged_violations()
    return JSONResponse({"items": violations})


@app.get("/api/schema-changes")
async def get_schema_changes() -> JSONResponse:
    return JSONResponse({"items": _load_schema_changes()})


@app.get("/api/ai-metrics")
async def get_ai_metrics() -> JSONResponse:
    metrics = _read_json(VALIDATION_DIR / "ai_metrics.json", {})

    embedding = metrics.get("embedding_drift") or {}
    prompt_input = metrics.get("prompt_input_validation") or {}
    output_schema = metrics.get("llm_output_schema") or {}

    payload = {
        "overall_risk": metrics.get("overall_risk") or "LOW",
        "embedding_drift": {
            "score": float(embedding.get("score") or 0.0),
            "threshold": float(embedding.get("threshold") or 0.15),
            "status": embedding.get("status") or "BASELINE_SET",
        },
        "prompt_input_validation": {
            "violation_rate": float(prompt_input.get("violation_rate") or 0.0),
            "total_records": int(prompt_input.get("total_records") or 0),
            "quarantined": int(prompt_input.get("quarantined") or 0),
            "status": prompt_input.get("status") or "PASS",
        },
        "llm_output_schema": {
            "violation_rate": float(output_schema.get("violation_rate") or 0.0),
            "trend": output_schema.get("trend") or "STABLE",
            "total_outputs_checked": int(output_schema.get("total_outputs_checked") or 0),
            "status": output_schema.get("status") or "PASS",
        },
    }
    return JSONResponse(payload)


@app.get("/api/contracts")
async def get_contracts() -> JSONResponse:
    return JSONResponse({"items": _load_contracts()})


@app.get("/api/enforcement-report")
async def get_enforcement_report() -> JSONResponse:
    return JSONResponse(_load_enforcement_report())


@app.get("/api/contracts/{contract_id}/dbt")
async def download_dbt_counterpart(contract_id: str) -> FileResponse:
    contract_path = CONTRACTS_DIR / f"{contract_id}.yaml"
    if not contract_path.exists():
        contract_path = CONTRACTS_DIR / f"{contract_id}.yml"

    if not contract_path.exists():
        raise HTTPException(status_code=404, detail="Contract not found")

    counterpart = contract_path.with_name(f"{contract_path.stem}_dbt.yml")
    if not counterpart.exists():
        raise HTTPException(status_code=404, detail="dbt counterpart not found")

    return FileResponse(counterpart, filename=counterpart.name, media_type="application/x-yaml")


async def _stream_file_lines(path: Path) -> AsyncGenerator[str, None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch(exist_ok=True)
    position = path.stat().st_size

    # Instruct browser EventSource to reconnect quickly if disconnected.
    yield "retry: 1500\n\n"

    if awatch is None:
        # Fallback mode for environments where watchfiles cannot be compiled/installed.
        while True:
            if path.exists():
                current_size = path.stat().st_size
                if current_size > position:
                    with path.open("r", encoding="utf-8") as f:
                        f.seek(position)
                        new_data = f.read()
                        position = f.tell()

                    for raw_line in new_data.splitlines():
                        line = raw_line.strip()
                        if not line:
                            continue
                        try:
                            payload = json.loads(line)
                            normalized = _normalize_violation(payload)
                            yield f"data: {json.dumps(normalized)}\n\n"
                        except json.JSONDecodeError:
                            continue

            await asyncio.sleep(1.5)

    else:
        async for changes in awatch(path.parent):
            changed = {Path(item[1]) for item in changes}
            if path not in changed:
                continue

            with path.open("r", encoding="utf-8") as f:
                f.seek(position)
                new_data = f.read()
                position = f.tell()

            for raw_line in new_data.splitlines():
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                    normalized = _normalize_violation(payload)
                    yield f"data: {json.dumps(normalized)}\n\n"
                except json.JSONDecodeError:
                    continue


@app.get("/api/live-violations")
async def live_violations() -> StreamingResponse:
    return StreamingResponse(_stream_file_lines(VIOLATIONS_FILE), media_type="text/event-stream")


@app.get("/api/ping")
async def ping() -> JSONResponse:
    return JSONResponse({"status": "ok"})


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
