"""
contracts/ledger_bridge.py
==========================
Bridge Data Contract Enforcer writes to the Agentic Event Ledger EventStore.

This module is intentionally strict: when called, persistence failures raise
exceptions so callers can hard-fail instead of silently degrading.
"""

import asyncio
import atexit
import importlib
import sys
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

AUDIT_VIOLATIONS_STREAM = "audit-contract-violations"
SCHEMA_STREAM_PREFIX = "audit-schema-snapshots-"


_LOOP_THREAD: threading.Thread | None = None
_LOOP: asyncio.AbstractEventLoop | None = None
_LOOP_READY = threading.Event()


@dataclass(frozen=True)
class _LedgerPayloadEvent:
    """Minimal event object compatible with EventStore.append()."""
    event_type: str
    payload: dict[str, Any]
    event_version: int = 1

    def get_payload(self) -> dict[str, Any]:
        return self.payload


def _ledger_root() -> Path:
    # contracts/ -> Data-Contract-Enforcer/ -> Projects/ -> agentic-event-ledger/
    return Path(__file__).resolve().parent.parent.parent / "agentic-event-ledger"


def _add_ledger_venv_site_packages(root: Path) -> None:
    """
    Ensure imports like asyncpg resolve even when Data-Contract-Enforcer is run
    with a different Python interpreter than the ledger project's interpreter.
    """
    candidates = [
        root / ".venv" / "Lib" / "site-packages",  # Windows venv
        root / ".venv" / "lib" / "site-packages",  # generic fallback
    ]

    # Also handle POSIX-style versioned site-packages directories.
    posix_lib = root / ".venv" / "lib"
    if posix_lib.exists():
        for py_dir in posix_lib.glob("python*"):
            candidates.append(py_dir / "site-packages")

    for site_packages in candidates:
        if site_packages.exists():
            sp = str(site_packages)
            if sp not in sys.path:
                sys.path.insert(0, sp)


def _ensure_ledger_import_path() -> None:
    root = _ledger_root()
    if not root.exists():
        raise RuntimeError(
            f"agentic-event-ledger project not found at {root}"
        )

    _add_ledger_venv_site_packages(root)

    root_s = str(root)
    if root_s not in sys.path:
        sys.path.insert(0, root_s)


def _load_ledger_symbols():
    _ensure_ledger_import_path()
    try:
        event_store_mod = importlib.import_module("src.event_store")
        events_mod = importlib.import_module("src.models.events")
        EventStore = event_store_mod.EventStore
        OptimisticConcurrencyError = events_mod.OptimisticConcurrencyError
        StreamNotFoundError = events_mod.StreamNotFoundError
        return EventStore, OptimisticConcurrencyError, StreamNotFoundError
    except Exception as exc:
        raise RuntimeError(
            "Failed to import EventStore symbols from agentic-event-ledger. "
            "Ensure ledger dependencies are installed in the active environment."
        ) from exc


async def _append_event(stream_id: str, event_type: str, payload: dict[str, Any]) -> int:
    EventStore, OptimisticConcurrencyError, StreamNotFoundError = _load_ledger_symbols()
    store = EventStore()
    event = _LedgerPayloadEvent(event_type=event_type, payload=payload)

    for attempt in range(5):
        try:
            try:
                expected_version = await store.stream_version(stream_id)
            except StreamNotFoundError:
                expected_version = -1

            return await store.append(
                stream_id=stream_id,
                events=[event],
                expected_version=expected_version,
            )
        except OptimisticConcurrencyError:
            if attempt == 4:
                raise

    raise RuntimeError("Failed to append event after optimistic concurrency retries")


def _loop_worker() -> None:
    global _LOOP
    loop = asyncio.new_event_loop()
    _LOOP = loop
    asyncio.set_event_loop(loop)
    _LOOP_READY.set()
    loop.run_forever()


def _ensure_background_loop() -> asyncio.AbstractEventLoop:
    global _LOOP_THREAD

    if _LOOP_THREAD is None or not _LOOP_THREAD.is_alive():
        _LOOP_READY.clear()
        _LOOP_THREAD = threading.Thread(
            target=_loop_worker,
            name="ledger-bridge-loop",
            daemon=True,
        )
        _LOOP_THREAD.start()
        _LOOP_READY.wait(timeout=5)

    if _LOOP is None:
        raise RuntimeError("Failed to initialize background event loop for ledger bridge")
    return _LOOP


def _shutdown_background_loop() -> None:
    if _LOOP and _LOOP.is_running():
        _LOOP.call_soon_threadsafe(_LOOP.stop)


def _append_event_sync(stream_id: str, event_type: str, payload: dict[str, Any]) -> int:
    loop = _ensure_background_loop()
    future = asyncio.run_coroutine_threadsafe(
        _append_event(stream_id, event_type, payload),
        loop,
    )
    return future.result()


def append_violation_event(violation: dict, require_blame: bool = True) -> int:
    blame_chain = violation.get("blame_chain", [])
    if require_blame and not blame_chain:
        raise RuntimeError(
            "Hard fail: violation is missing blame_chain; refusing immutable append."
        )

    normalized_blame_chain = []
    for b in blame_chain:
        if not isinstance(b, dict):
            continue
        normalized_blame_chain.append(
            {
                "author": b.get("author", "unknown"),
                "commit_hash": b.get("commit_hash", "unknown"),
                "author_name": b.get("author_name", ""),
                "file_path": b.get("file_path", ""),
                "rank": b.get("rank"),
                "lineage_hop": b.get("lineage_hop"),
                "confidence_score": b.get("confidence_score"),
            }
        )

    payload = {
        "violation_id": violation.get("violation_id"),
        "check_id": violation.get("check_id"),
        "contract_id": violation.get("contract_id"),
        "failing_field": violation.get("failing_field"),
        "severity": violation.get("severity", "HIGH"),
        "message": violation.get("message", ""),
        "detected_at": violation.get("detected_at"),
        "actual_value": violation.get("actual_value", ""),
        "expected": violation.get("expected", ""),
        "records_failing": violation.get("records_failing", 0),
        "blame_chain": normalized_blame_chain,
        "blast_radius": violation.get("blast_radius", {}),
    }

    return _append_event_sync(
        stream_id=AUDIT_VIOLATIONS_STREAM,
        event_type="ContractViolationDetected",
        payload=payload,
    )


def append_schema_snapshot_event(contract_id: str, snapshot: dict[str, Any]) -> int:
    stream_id = f"{SCHEMA_STREAM_PREFIX}{contract_id}"
    payload = {
        "contract_id": contract_id,
        "snapshot_timestamp": snapshot.get("snapshot_timestamp"),
        "source_path": snapshot.get("source_path"),
        "schema": snapshot.get("schema", {}),
    }
    return _append_event_sync(
        stream_id=stream_id,
        event_type="SchemaSnapshotPersisted",
        payload=payload,
    )


atexit.register(_shutdown_background_loop)
