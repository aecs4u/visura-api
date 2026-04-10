"""Tests for the CLI commands (cli.py).

Uses Typer's CliRunner to invoke commands without a real server.
API calls are mocked via monkeypatch on the VisuraClient methods.
"""

import asyncio
import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from cli import app

runner = CliRunner()


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _patch_client(monkeypatch, method_name, return_value=None, side_effect=None):
    """Patch a VisuraClient async method to return a canned value."""
    from client import VisuraClient

    if side_effect:
        async def fake(*args, **kwargs):
            raise side_effect
    else:
        async def fake(*args, **kwargs):
            return return_value

    monkeypatch.setattr(VisuraClient, method_name, fake)


# ---------------------------------------------------------------------------
# queries
# ---------------------------------------------------------------------------


def test_queries_lists_endpoints():
    result = runner.invoke(app, ["queries"])
    assert result.exit_code == 0
    assert "/visura" in result.output
    assert "/health" in result.output
    assert "search" in result.output
    assert "intestati" in result.output


# ---------------------------------------------------------------------------
# search
# ---------------------------------------------------------------------------


def test_search_dry_run():
    result = runner.invoke(app, [
        "search", "-P", "Trieste", "-C", "TRIESTE", "-F", "9", "-p", "166",
        "--dry-run",
    ])
    assert result.exit_code == 0
    assert "DRY RUN" in result.output
    assert "POST" in result.output
    assert "Trieste" in result.output


def test_search_submits_and_prints_ids(monkeypatch):
    _patch_client(monkeypatch, "search", {
        "request_ids": ["req_T_001", "req_F_002"],
        "status": "queued",
    })

    result = runner.invoke(app, [
        "search", "-P", "Trieste", "-C", "TRIESTE", "-F", "9", "-p", "166",
    ])
    assert result.exit_code == 0
    assert "req_T_001" in result.output
    assert "req_F_002" in result.output
    assert "queued" in result.output


def test_search_with_tipo_catasto(monkeypatch):
    _patch_client(monkeypatch, "search", {
        "request_ids": ["req_F_only"],
        "status": "queued",
    })

    result = runner.invoke(app, [
        "search", "-P", "Roma", "-C", "ROMA", "-F", "1", "-p", "1", "-t", "F",
    ])
    assert result.exit_code == 0
    assert "req_F_only" in result.output


def test_search_handles_api_error(monkeypatch):
    from client import VisuraAPIError

    _patch_client(monkeypatch, "search", side_effect=VisuraAPIError(503, "Service down"))

    result = runner.invoke(app, [
        "search", "-P", "X", "-C", "X", "-F", "1", "-p", "1",
    ])
    assert result.exit_code == 1
    assert "503" in result.output


def test_search_missing_required_option():
    result = runner.invoke(app, ["search", "-P", "Trieste"])
    assert result.exit_code != 0


# ---------------------------------------------------------------------------
# intestati
# ---------------------------------------------------------------------------


def test_intestati_dry_run():
    result = runner.invoke(app, [
        "intestati", "-P", "Trieste", "-C", "TRIESTE", "-F", "9", "-p", "166",
        "-t", "F", "-sub", "3", "--dry-run",
    ])
    assert result.exit_code == 0
    assert "DRY RUN" in result.output
    assert "/visura/intestati" in result.output


def test_intestati_submits_and_prints_id(monkeypatch):
    _patch_client(monkeypatch, "intestati", {
        "request_id": "intestati_F_abc",
        "status": "queued",
    })

    result = runner.invoke(app, [
        "intestati", "-P", "Trieste", "-C", "TRIESTE", "-F", "9", "-p", "166",
        "-t", "F", "-sub", "3",
    ])
    assert result.exit_code == 0
    assert "intestati_F_abc" in result.output
    assert "queued" in result.output


# ---------------------------------------------------------------------------
# get
# ---------------------------------------------------------------------------


def test_get_completed(monkeypatch):
    _patch_client(monkeypatch, "get_result", {
        "request_id": "req_F_done",
        "status": "completed",
        "tipo_catasto": "F",
        "data": {"immobili": [{"Foglio": "9", "Particella": "166"}]},
        "timestamp": "2025-01-01T12:00:00",
    })

    result = runner.invoke(app, ["get", "req_F_done"])
    assert result.exit_code == 0
    assert "Completed" in result.output
    assert "req_F_done" in result.output


def test_get_processing(monkeypatch):
    _patch_client(monkeypatch, "get_result", {
        "request_id": "req_F_proc",
        "status": "processing",
    })

    result = runner.invoke(app, ["get", "req_F_proc"])
    assert result.exit_code == 0
    assert "processing" in result.output


def test_get_expired(monkeypatch):
    _patch_client(monkeypatch, "get_result", {
        "request_id": "req_F_exp",
        "status": "expired",
    })

    result = runner.invoke(app, ["get", "req_F_exp"])
    assert result.exit_code == 0
    assert "expired" in result.output


def test_get_error_status(monkeypatch):
    _patch_client(monkeypatch, "get_result", {
        "request_id": "req_F_err",
        "status": "error",
        "error": "Session lost",
    })

    result = runner.invoke(app, ["get", "req_F_err"])
    assert result.exit_code == 0
    assert "Session lost" in result.output


def test_get_writes_output(monkeypatch, tmp_path):
    _patch_client(monkeypatch, "get_result", {
        "request_id": "req_F_out",
        "status": "completed",
        "data": {"ok": True},
    })

    out_file = tmp_path / "result.json"
    result = runner.invoke(app, ["get", "req_F_out", "-o", str(out_file)])
    assert result.exit_code == 0

    written = json.loads(out_file.read_text())
    assert written["request_id"] == "req_F_out"


def test_get_404(monkeypatch):
    from client import VisuraAPIError

    _patch_client(monkeypatch, "get_result", side_effect=VisuraAPIError(404, "Not found"))

    result = runner.invoke(app, ["get", "nonexistent"])
    assert result.exit_code == 1
    assert "404" in result.output


# ---------------------------------------------------------------------------
# wait
# ---------------------------------------------------------------------------


def test_wait_completes(monkeypatch):
    _patch_client(monkeypatch, "wait_for_result", {
        "request_id": "req_wait",
        "status": "completed",
        "data": {"immobili": []},
    })

    result = runner.invoke(app, ["wait", "req_wait"])
    assert result.exit_code == 0
    assert "Completed" in result.output


def test_wait_timeout(monkeypatch):
    _patch_client(
        monkeypatch, "wait_for_result",
        side_effect=TimeoutError("Timed out after 10s waiting for req_slow"),
    )

    result = runner.invoke(app, ["wait", "req_slow"])
    assert result.exit_code == 1
    assert "Timed out" in result.output


# ---------------------------------------------------------------------------
# history
# ---------------------------------------------------------------------------


def test_history_empty(monkeypatch):
    _patch_client(monkeypatch, "history", {"results": [], "count": 0})

    result = runner.invoke(app, ["history"])
    assert result.exit_code == 0
    assert "No history records found" in result.output


def test_history_with_results(monkeypatch):
    _patch_client(monkeypatch, "history", {
        "results": [
            {
                "request_id": "r1",
                "tipo_catasto": "T",
                "provincia": "Trieste",
                "comune": "TRIESTE",
                "foglio": "9",
                "particella": "166",
                "success": True,
                "requested_at": "2025-01-01T12:00:00",
            },
        ],
        "count": 1,
    })

    result = runner.invoke(app, ["history", "-P", "Trieste"])
    assert result.exit_code == 0
    assert "r1" in result.output
    assert "1 results" in result.output


def test_history_with_output(monkeypatch, tmp_path):
    _patch_client(monkeypatch, "history", {
        "results": [{"request_id": "r1"}],
        "count": 1,
    })

    out_file = tmp_path / "history.json"
    result = runner.invoke(app, ["history", "-o", str(out_file)])
    assert result.exit_code == 0

    written = json.loads(out_file.read_text())
    assert written["count"] == 1


# ---------------------------------------------------------------------------
# health
# ---------------------------------------------------------------------------


def test_health_healthy(monkeypatch):
    _patch_client(monkeypatch, "health", {
        "status": "healthy",
        "authenticated": True,
        "queue_size": 0,
        "pending_requests": 0,
        "cached_responses": 5,
        "queue_max_size": 100,
        "response_ttl_seconds": 21600,
        "database": {
            "total_requests": 42,
            "total_responses": 40,
            "successful": 38,
            "failed": 2,
        },
    })

    result = runner.invoke(app, ["health"])
    assert result.exit_code == 0
    assert "healthy" in result.output
    assert "True" in result.output
    assert "42" in result.output


def test_health_unreachable(monkeypatch):
    _patch_client(monkeypatch, "health", side_effect=ConnectionError("refused"))

    result = runner.invoke(app, ["health"])
    assert result.exit_code == 1
    assert "Cannot reach" in result.output
