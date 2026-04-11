"""Tests for the workflow module (workflows.py).

Tests cover:
  - WorkflowInput validation
  - Property normalization and deduplication
  - Aggregation logic
  - Workflow API endpoint via TestClient
  - CLI workflow command via CliRunner
"""

import pytest
from pydantic import ValidationError

from sister.models import WORKFLOW_PRESETS, WorkflowInput
from sister.workflows import (
    _build_aggregate,
    _deduplicate_properties,
    _normalize_property,
    _step_key,
)


# ---------------------------------------------------------------------------
# WorkflowInput validation
# ---------------------------------------------------------------------------


class TestWorkflowInput:
    def test_valid_due_diligence(self):
        wf = WorkflowInput(
            preset="due-diligence", provincia="Roma", comune="ROMA",
            foglio="100", particella="50",
        )
        assert wf.preset == "due-diligence"
        assert wf.include_paid_steps is False

    def test_preset_normalized(self):
        wf = WorkflowInput(
            preset="Due-Diligence", provincia="Roma", comune="ROMA",
            foglio="100", particella="50",
        )
        assert wf.preset == "due-diligence"

    def test_unknown_preset_rejected(self):
        with pytest.raises(ValidationError):
            WorkflowInput(preset="nonexistent", provincia="Roma")

    def test_patrimonio_preset(self):
        wf = WorkflowInput(preset="patrimonio", codice_fiscale="RSSMRI85E28H501E")
        assert wf.preset == "patrimonio"
        assert wf.codice_fiscale == "RSSMRI85E28H501E"

    def test_include_paid_steps_flag(self):
        wf = WorkflowInput(
            preset="due-diligence", provincia="Roma", comune="ROMA",
            foglio="100", particella="50",
            include_paid_steps=True, auto_confirm=True,
        )
        assert wf.include_paid_steps is True
        assert wf.auto_confirm is True

    def test_all_presets_defined(self):
        expected = {"due-diligence", "patrimonio", "fondiario", "aziendale", "storico", "indirizzo", "cross-reference"}
        assert set(WORKFLOW_PRESETS.keys()) == expected

    def test_each_preset_has_steps_and_requires(self):
        for name, defn in WORKFLOW_PRESETS.items():
            assert "steps" in defn, f"Preset '{name}' missing 'steps'"
            assert "requires" in defn, f"Preset '{name}' missing 'requires'"
            assert isinstance(defn["steps"], list)
            assert isinstance(defn["requires"], list)
            assert len(defn["steps"]) > 0

    def test_due_diligence_includes_elaborato(self):
        steps = WORKFLOW_PRESETS["due-diligence"]["steps"]
        assert "elaborato_planimetrico" in steps

    def test_patrimonio_includes_drill_and_address(self):
        steps = WORKFLOW_PRESETS["patrimonio"]["steps"]
        assert "drill_intestati" in steps
        assert "indirizzo_reverse" in steps

    def test_fondiario_includes_export_mappa(self):
        steps = WORKFLOW_PRESETS["fondiario"]["steps"]
        assert "export_mappa" in steps
        assert "elaborato_planimetrico" in steps

    def test_storico_includes_nota(self):
        steps = WORKFLOW_PRESETS["storico"]["steps"]
        assert "nota" in steps

    def test_cross_reference_includes_cross_property(self):
        steps = WORKFLOW_PRESETS["cross-reference"]["steps"]
        assert "cross_property_intestati" in steps

    def test_depth_filtering(self):
        """Verify that depth tiers are correctly assigned to steps."""
        from sister.models import STEP_METADATA, _DEPTH_ORDER
        # Paid steps should be deep
        for step_name, meta in STEP_METADATA.items():
            if meta["paid"]:
                assert meta["depth"] == "deep", f"Paid step '{step_name}' should be depth=deep"

    def test_depth_validation(self):
        wf = WorkflowInput(
            preset="due-diligence", provincia="Roma", comune="ROMA",
            foglio="1", particella="1", depth="light",
        )
        assert wf.depth == "light"

    def test_invalid_depth_rejected(self):
        with pytest.raises(ValidationError):
            WorkflowInput(preset="due-diligence", provincia="X", depth="ultra")


# ---------------------------------------------------------------------------
# Property normalization
# ---------------------------------------------------------------------------


class TestNormalizeProperty:
    def test_full_row(self):
        row = {"Provincia": "Roma", "Comune": "ROMA", "Foglio": "100", "Particella": "50", "Sub": "3"}
        result = _normalize_property(row)
        assert result == {
            "provincia": "Roma", "comune": "ROMA", "foglio": "100",
            "particella": "50", "subalterno": "3", "tipo_catasto": None, "sezione": None,
        }

    def test_missing_foglio_returns_none(self):
        row = {"Provincia": "Roma", "Comune": "ROMA", "Foglio": "", "Particella": "50"}
        assert _normalize_property(row) is None

    def test_empty_row_returns_none(self):
        assert _normalize_property({}) is None

    def test_strips_whitespace(self):
        row = {"Provincia": " Roma ", "Comune": " ROMA ", "Foglio": " 100 ", "Particella": " 50 "}
        result = _normalize_property(row)
        assert result["provincia"] == "Roma"
        assert result["foglio"] == "100"


class TestDeduplicateProperties:
    def test_removes_duplicates(self):
        props = [
            {"provincia": "Roma", "comune": "ROMA", "foglio": "1", "particella": "1", "subalterno": None},
            {"provincia": "Roma", "comune": "ROMA", "foglio": "1", "particella": "1", "subalterno": None},
        ]
        result = _deduplicate_properties(props)
        assert len(result) == 1

    def test_keeps_different_subalterni(self):
        props = [
            {"provincia": "Roma", "comune": "ROMA", "foglio": "1", "particella": "1", "subalterno": "1"},
            {"provincia": "Roma", "comune": "ROMA", "foglio": "1", "particella": "1", "subalterno": "2"},
        ]
        result = _deduplicate_properties(props)
        assert len(result) == 2

    def test_empty_input(self):
        assert _deduplicate_properties([]) == []


# ---------------------------------------------------------------------------
# Step key
# ---------------------------------------------------------------------------


class TestStepKey:
    def test_simple(self):
        assert _step_key("search") == "search"

    def test_with_context(self):
        key = _step_key("intestati", tipo_catasto="F", subalterno="3")
        assert "intestati" in key
        assert "subalterno=3" in key
        assert "tipo_catasto=F" in key

    def test_none_values_excluded(self):
        key = _step_key("search", foglio=None, particella="50")
        assert "foglio" not in key
        assert "particella=50" in key


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------


class TestBuildAggregate:
    def test_empty_steps(self):
        result = _build_aggregate([])
        assert result["properties"] == []
        assert result["owners"] == []
        assert result["links"] == []
        assert result["addresses"] == []
        assert result["risk_flags"] == []

    def test_collects_immobili(self):
        steps = [
            {"step": "search", "status": "completed", "data": {
                "immobili": [
                    {"Foglio": "1", "Particella": "10", "Sub": ""},
                    {"Foglio": "1", "Particella": "20", "Sub": "3"},
                ],
            }},
        ]
        result = _build_aggregate(steps)
        assert len(result["properties"]) == 2

    def test_deduplicates_immobili(self):
        steps = [
            {"step": "search", "status": "completed", "data": {
                "immobili": [
                    {"Foglio": "1", "Particella": "10", "Sub": ""},
                    {"Foglio": "1", "Particella": "10", "Sub": ""},
                ],
            }},
        ]
        result = _build_aggregate(steps)
        assert len(result["properties"]) == 1

    def test_collects_intestati(self):
        steps = [
            {"step": "intestati", "status": "completed", "data": {
                "intestati": [
                    {"Codice fiscale": "ABC123", "Nominativo": "Mario Rossi"},
                ],
            }},
        ]
        result = _build_aggregate(steps)
        assert len(result["owners"]) == 1
        assert len(result["links"]) == 1

    def test_skips_error_steps(self):
        steps = [
            {"step": "search", "status": "error", "data": None, "error": "fail"},
        ]
        result = _build_aggregate(steps)
        assert result["properties"] == []
        assert result["owners"] == []
        assert result["links"] == []

    def test_risk_flag_missing_cf(self):
        steps = [
            {"step": "intestati", "status": "completed", "data": {
                "intestati": [
                    {"Nominativo": "Mario Rossi"},  # no CF
                ],
            }},
        ]
        result = _build_aggregate(steps)
        assert any(f["type"] == "missing_cf" for f in result["risk_flags"])

    def test_risk_flag_multiple_owners(self):
        steps = [
            {"step": "drill_intestati", "status": "completed", "data": {
                "drill_results": [
                    {
                        "property": {"foglio": "1", "particella": "10", "subalterno": ""},
                        "intestati": [
                            {"Codice fiscale": "AAA", "Nominativo": "A"},
                            {"Codice fiscale": "BBB", "Nominativo": "B"},
                        ],
                        "status": "completed",
                    },
                ],
            }},
        ]
        result = _build_aggregate(steps)
        assert any(f["type"] == "multiple_owners" for f in result["risk_flags"])

    def test_aggregate_includes_addresses(self):
        steps = [
            {"step": "indirizzo_reverse", "status": "completed", "data": {
                "addresses": [
                    {"property": {"foglio": "1", "particella": "10"}, "addresses": ["VIA ROMA 1"]},
                ],
            }},
        ]
        result = _build_aggregate(steps)
        assert len(result["addresses"]) == 1

    def test_aggregate_collects_owner_portfolios(self):
        steps = [
            {"step": "cross_property_intestati", "status": "completed", "data": {
                "owner_portfolios": [
                    {
                        "codice_fiscale": "ABC123",
                        "immobili": [
                            {"Foglio": "5", "Particella": "20", "Sub": ""},
                        ],
                        "status": "completed",
                    },
                ],
            }},
        ]
        result = _build_aggregate(steps)
        assert len(result["properties"]) == 1
        assert any(l["owner_key"] == "ABC123" for l in result["links"])

    def test_drill_down_results(self):
        steps = [
            {"step": "drill_intestati", "status": "completed", "data": {
                "drill_results": [
                    {
                        "property": {"foglio": "1", "particella": "10", "subalterno": "3"},
                        "intestati": [{"Codice fiscale": "XYZ789", "Nominativo": "Luigi Bianchi"}],
                        "status": "completed",
                    },
                ],
            }},
        ]
        result = _build_aggregate(steps)
        assert len(result["properties"]) == 1
        assert len(result["owners"]) == 1
        assert any("drill:" in link["source_step"] for link in result["links"])


# ---------------------------------------------------------------------------
# Workflow preset validation (without server)
# ---------------------------------------------------------------------------


class TestStepExecutorRegistry:
    """Verify all steps referenced in presets have executors."""

    def test_all_preset_steps_have_executors(self):
        from sister.workflows import _STEP_EXECUTORS

        for name, defn in WORKFLOW_PRESETS.items():
            for step in defn["steps"]:
                assert step in _STEP_EXECUTORS, f"Step '{step}' in preset '{name}' has no executor"

    def test_executor_count(self):
        from sister.workflows import _STEP_EXECUTORS
        # All executors: search, intestati, soggetto, azienda, drill_intestati,
        # elenco, mappa, fiduciali, originali, ispezioni, ispezioni_cart,
        # indirizzo_search, elaborato_planimetrico, export_mappa, nota,
        # cross_property_intestati, indirizzo_reverse, ispezione_ipotecaria
        assert len(_STEP_EXECUTORS) >= 18


class TestWorkflowPresetValidation:
    """Verify preset validation without a running server."""

    def test_unknown_preset_rejected_by_model(self):
        with pytest.raises(ValidationError, match="Unknown preset"):
            WorkflowInput(preset="nonexistent")

    def test_due_diligence_includes_paid_in_steps(self):
        """Paid steps are in the steps list but filtered by depth/paid flags at runtime."""
        defn = WORKFLOW_PRESETS["due-diligence"]
        assert "ispezione_ipotecaria" in defn["steps"]

    def test_storico_includes_paid_in_steps(self):
        defn = WORKFLOW_PRESETS["storico"]
        assert "ispezione_ipotecaria" in defn["steps"]


# ---------------------------------------------------------------------------
# CLI workflow command
# ---------------------------------------------------------------------------


class TestWorkflowCLI:
    """Test CLI workflow command via CliRunner (mocked client)."""

    def test_dry_run_with_preset(self):
        from typer.testing import CliRunner
        from sister.cli import app

        runner = CliRunner()
        result = runner.invoke(app, [
            "query", "workflow", "--preset", "due-diligence",
            "-P", "Roma", "-C", "ROMA", "-F", "100", "-p", "50",
            "--dry-run",
        ])
        assert result.exit_code == 0
        assert "DRY RUN" in result.output
        assert "due-diligence" in result.output

    def test_unknown_preset_exits_1(self):
        from typer.testing import CliRunner
        from sister.cli import app

        runner = CliRunner()
        result = runner.invoke(app, [
            "query", "workflow", "--preset", "nonexistent",
        ])
        assert result.exit_code == 1
        assert "Unknown preset" in result.output

    def test_preset_calls_server(self, monkeypatch):
        """Verify preset workflow calls client.workflow()."""
        from typer.testing import CliRunner
        from sister.cli import app
        from sister.client import VisuraClient

        called_with = {}

        async def mock_workflow(self, **kwargs):
            called_with.update(kwargs)
            return {
                "preset": kwargs.get("preset"),
                "steps": [{"step": "search", "status": "completed", "data": {"immobili": [], "total": 0}}],
                "summary": {"completed": 1, "failed": 0, "skipped": 0, "total_steps": 1},
            }

        monkeypatch.setattr(VisuraClient, "workflow", mock_workflow)
        runner = CliRunner()
        result = runner.invoke(app, [
            "query", "workflow", "--preset", "fondiario",
            "-P", "Roma", "-C", "ROMA",
        ])
        assert result.exit_code == 0
        assert called_with.get("preset") == "fondiario"
