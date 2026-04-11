"""Workflow orchestrator for multi-step SISTER queries.

Owns preset definitions, step execution, drill-down, aggregation, and
persistence. Both the CLI and the API route delegate to `run_workflow()`.

Workflow state is persisted in `workflow_runs` / `workflow_steps` tables
so that interrupted runs can be resumed.
"""

import json
import logging
from datetime import datetime
from typing import Optional
from uuid import uuid4

from .models import STEP_METADATA, WORKFLOW_PRESETS, WorkflowInput, _DEPTH_ORDER
from .services import VisuraService
from .utils import (
    run_elaborato_planimetrico,
    run_elenco_immobili,
    run_export_mappa,
    run_ispezione_ipotecaria,
    run_ispezioni,
    run_ispezioni_cartacee,
    run_originali_impianto,
    run_punti_fiduciali,
    run_ricerca_indirizzo,
    run_ricerca_mappa,
    run_ricerca_nota,
    run_visura,
    run_visura_immobile,
    run_visura_persona_giuridica,
    run_visura_soggetto,
)

logger = logging.getLogger("sister")

MAX_DRILL_DOWN = 20  # Safety limit for fan-out

# ---------------------------------------------------------------------------
# Property normalization (for drill-down)
# ---------------------------------------------------------------------------


def _normalize_property(row: dict) -> Optional[dict]:
    """Extract a normalized property dict from a soggetto/azienda result row.

    Returns None if the row doesn't contain enough location data.
    """
    prov = (row.get("Provincia") or "").strip()
    com = (row.get("Comune") or "").strip()
    fog = (row.get("Foglio") or "").strip()
    par = (row.get("Particella") or "").strip()
    if not (prov and com and fog and par):
        return None
    return {
        "provincia": prov,
        "comune": com,
        "foglio": fog,
        "particella": par,
        "subalterno": (row.get("Sub") or "").strip() or None,
        "tipo_catasto": (row.get("Tipo") or "").strip() or None,
        "sezione": (row.get("Sezione") or "").strip() or None,
    }


def _deduplicate_properties(props: list[dict]) -> list[dict]:
    """Deduplicate properties by (provincia, comune, foglio, particella, subalterno)."""
    seen: set[tuple] = set()
    result: list[dict] = []
    for p in props:
        key = (p["provincia"], p["comune"], p["foglio"], p["particella"], p.get("subalterno"))
        if key not in seen:
            seen.add(key)
            result.append(p)
    return result


def _step_key(step_name: str, **ctx) -> str:
    """Build a deterministic step key for persistence."""
    parts = [step_name]
    for k in sorted(ctx):
        v = ctx[k]
        if v:
            parts.append(f"{k}={v}")
    return ":".join(parts)


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------


def _build_aggregate(step_results: list[dict]) -> dict:
    """Build a normalized aggregate from step results.

    Returns:
        {
            "properties": [...],
            "owners": [...],
            "links": [{"property_key": ..., "owner_key": ..., "source_step": ...}],
            "addresses": [...],
            "risk_flags": [...],
        }
    """
    properties: list[dict] = []
    owners: list[dict] = []
    links: list[dict] = []
    addresses: list[dict] = []
    risk_flags: list[dict] = []
    prop_keys_seen: set[str] = set()
    owner_keys_seen: set[str] = set()

    def _owner_key(owner: dict) -> str:
        cf = (owner.get("Codice fiscale") or owner.get("Codice Fiscale") or "").strip()
        nome = (owner.get("Nominativo o denominazione") or owner.get("Nominativo") or "").strip()
        return cf or nome

    def _collect_owner(owner: dict, step_name: str, property_key: str = None):
        ok = _owner_key(owner)
        if not ok:
            return
        if ok not in owner_keys_seen:
            owner_keys_seen.add(ok)
            owners.append(owner)
        links.append({"property_key": property_key, "owner_key": ok, "source_step": step_name})
        # Risk: owner without CF
        cf = (owner.get("Codice fiscale") or owner.get("Codice Fiscale") or "").strip()
        if not cf:
            risk_flags.append({"type": "missing_cf", "owner": ok, "source_step": step_name})

    for step in step_results:
        if step["status"] != "completed" or not step.get("data"):
            continue
        data = step["data"]
        step_name = step["step"]

        # Collect immobili
        for imm in data.get("immobili", []):
            pk = f"{imm.get('Foglio', '')}:{imm.get('Particella', '')}:{imm.get('Sub', '')}"
            if pk not in prop_keys_seen:
                prop_keys_seen.add(pk)
                properties.append(imm)

        # Collect intestati
        for owner in data.get("intestati", []):
            _collect_owner(owner, step_name)

        # Drill-down results
        for drill in data.get("drill_results", []):
            prop = drill.get("property", {})
            pk = f"{prop.get('foglio', '')}:{prop.get('particella', '')}:{prop.get('subalterno', '')}"
            if pk not in prop_keys_seen:
                prop_keys_seen.add(pk)
                properties.append(prop)
            for owner in drill.get("intestati", []):
                _collect_owner(owner, f"drill:{pk}", property_key=pk)

        # Cross-property intestati (owner portfolios)
        for portfolio in data.get("owner_portfolios", []):
            cf = portfolio.get("codice_fiscale", "")
            for imm in portfolio.get("immobili", []):
                pk = f"{imm.get('Foglio', '')}:{imm.get('Particella', '')}:{imm.get('Sub', '')}"
                if pk not in prop_keys_seen:
                    prop_keys_seen.add(pk)
                    properties.append(imm)
                links.append({"property_key": pk, "owner_key": cf, "source_step": f"cross_portfolio:{cf}"})

        # Addresses
        for addr in data.get("addresses", []):
            addresses.append(addr)

    # Risk: properties with multiple owners
    owner_count_by_prop: dict[str, int] = {}
    for link in links:
        pk = link.get("property_key")
        if pk:
            owner_count_by_prop[pk] = owner_count_by_prop.get(pk, 0) + 1
    for pk, count in owner_count_by_prop.items():
        if count > 1:
            risk_flags.append({"type": "multiple_owners", "property_key": pk, "count": count})

    return {
        "properties": properties,
        "owners": owners,
        "links": links,
        "addresses": addresses,
        "risk_flags": risk_flags,
    }


# ---------------------------------------------------------------------------
# DB persistence helpers
# ---------------------------------------------------------------------------


async def _save_workflow_run(workflow_id: str, preset: str, input_data: dict, status: str = "running"):
    """Persist a workflow run to the database."""
    from .database import _get_session_factory

    async with _get_session_factory()() as session:
        from sqlalchemy import text
        await session.execute(text(
            "INSERT OR REPLACE INTO workflow_runs (workflow_id, preset, status, input_json, created_at, updated_at) "
            "VALUES (:wid, :preset, :status, :input_json, :now, :now)"
        ), {"wid": workflow_id, "preset": preset, "status": status,
            "input_json": json.dumps(input_data, default=str), "now": datetime.now().isoformat()})
        await session.commit()


async def _save_workflow_step(workflow_id: str, step_key: str, status: str,
                              result_json: Optional[dict] = None, error: Optional[str] = None):
    """Persist a workflow step result."""
    from .database import _get_session

    now = datetime.now().isoformat()
    async with _get_session() as session:
        from sqlalchemy import text
        await session.execute(text(
            "INSERT OR REPLACE INTO workflow_steps "
            "(workflow_id, step_key, status, result_json, error, started_at, finished_at) "
            "VALUES (:wid, :sk, :status, :rj, :err, :now, :now)"
        ), {"wid": workflow_id, "sk": step_key, "status": status,
            "rj": json.dumps(result_json, default=str) if result_json else None,
            "err": error, "now": now})
        await session.commit()


async def _load_completed_steps(workflow_id: str) -> dict[str, dict]:
    """Load completed steps for a workflow (for resume)."""
    from .database import _get_session_factory

    async with _get_session_factory()() as session:
        from sqlalchemy import text
        result = await session.execute(text(
            "SELECT step_key, status, result_json FROM workflow_steps "
            "WHERE workflow_id = :wid AND status = 'completed'"
        ), {"wid": workflow_id})
        rows = result.fetchall()

    completed: dict[str, dict] = {}
    for row in rows:
        try:
            data = json.loads(row[2]) if row[2] else None
        except (json.JSONDecodeError, TypeError):
            data = None
        completed[row[0]] = {"status": row[1], "data": data}
    return completed


async def _finish_workflow_run(workflow_id: str, status: str, output_json: Optional[dict] = None):
    """Mark a workflow run as completed/failed."""
    from .database import _get_session_factory

    async with _get_session_factory()() as session:
        from sqlalchemy import text
        await session.execute(text(
            "UPDATE workflow_runs SET status = :status, output_json = :oj, updated_at = :now "
            "WHERE workflow_id = :wid"
        ), {"wid": workflow_id, "status": status,
            "oj": json.dumps(output_json, default=str) if output_json else None,
            "now": datetime.now().isoformat()})
        await session.commit()


# ---------------------------------------------------------------------------
# Step executors
# ---------------------------------------------------------------------------


async def _exec_search(page, params: dict, step_results: list[dict]) -> dict:
    """Run property search (one or both catasto types)."""
    tipos = [params["tipo_catasto"]] if params.get("tipo_catasto") and params["tipo_catasto"] != "E" else ["T", "F"]
    all_immobili = []
    for tc in tipos:
        result = await run_visura(
            page, params["provincia"], params["comune"], params.get("sezione"),
            params["foglio"], params["particella"], tc,
            extract_intestati=False, subalterno=params.get("subalterno"),
        )
        if isinstance(result, dict):
            for imm in result.get("immobili", []):
                imm["_tipo_catasto"] = tc
            all_immobili.extend(result.get("immobili", []))
    return {"immobili": all_immobili, "total": len(all_immobili)}


async def _exec_intestati(page, params: dict, step_results: list[dict]) -> dict:
    """Run intestati lookup, using sub-units from previous search if available."""
    prev_search = next((s for s in step_results if s["step"] == "search" and s["status"] == "completed"), None)
    all_intestati = []

    if prev_search and prev_search.get("data"):
        immobili = prev_search["data"].get("immobili", [])
        targets = []
        t_found = any(i.get("_tipo_catasto") == "T" for i in immobili)
        f_subs = {i.get("Sub", "").strip() for i in immobili if i.get("_tipo_catasto") == "F" and i.get("Sub", "").strip()}

        if t_found:
            targets.append(("T", None))
        if params.get("subalterno"):
            targets.append(("F", params["subalterno"]))
        elif f_subs:
            targets.extend(("F", sub) for sub in sorted(f_subs))

        for tc, sub in targets:
            try:
                if tc == "F" and sub:
                    res = await run_visura_immobile(
                        page, params["provincia"], params["comune"], params.get("sezione"),
                        params["foglio"], params["particella"], sub,
                    )
                else:
                    res = await run_visura(
                        page, params["provincia"], params["comune"], params.get("sezione"),
                        params["foglio"], params["particella"], tc,
                        extract_intestati=True,
                    )
                intestati = res.get("intestati", []) if isinstance(res, dict) else []
                for intest in intestati:
                    intest["_tipo_catasto"] = tc
                    intest["_subalterno"] = sub
                all_intestati.extend(intestati)
            except Exception as e:
                logger.warning("Intestati %s/%s failed: %s", tc, sub, e)
    else:
        tc = params.get("tipo_catasto") or "T"
        res = await run_visura(
            page, params["provincia"], params["comune"], params.get("sezione"),
            params["foglio"], params["particella"], tc, extract_intestati=True,
        )
        all_intestati = res.get("intestati", []) if isinstance(res, dict) else []

    return {"intestati": all_intestati, "total": len(all_intestati)}


async def _exec_soggetto(page, params: dict, step_results: list[dict]) -> dict:
    """Run national subject search."""
    return await run_visura_soggetto(
        page, codice_fiscale=params["codice_fiscale"],
        tipo_catasto=params.get("tipo_catasto", "E"),
        provincia=params.get("provincia"),
    )


async def _exec_azienda(page, params: dict, step_results: list[dict]) -> dict:
    """Run company search."""
    return await run_visura_persona_giuridica(
        page, identificativo=params["identificativo"],
        tipo_catasto=params.get("tipo_catasto", "E"),
        provincia=params.get("provincia"),
    )


async def _exec_drill_intestati(page, params: dict, step_results: list[dict]) -> dict:
    """Auto drill-down: extract properties from soggetto/azienda, then get intestati for each."""
    # Collect discovered properties from previous steps
    discovered: list[dict] = []
    for s in step_results:
        if s["status"] != "completed" or not s.get("data"):
            continue
        if s["step"] in ("soggetto", "azienda"):
            for imm in s["data"].get("immobili", []):
                prop = _normalize_property(imm)
                if prop:
                    discovered.append(prop)

    discovered = _deduplicate_properties(discovered)
    if not discovered:
        return {"drill_results": [], "total_properties": 0, "truncated": False}

    all_drill = []
    limit = params.get("max_fanout", MAX_DRILL_DOWN)
    for prop in discovered[:limit]:
        tc = prop.get("tipo_catasto") or ("F" if prop.get("subalterno") else "T")
        try:
            if tc == "F" and prop.get("subalterno"):
                res = await run_visura_immobile(
                    page, prop["provincia"], prop["comune"], prop.get("sezione"),
                    prop["foglio"], prop["particella"], prop["subalterno"],
                )
            else:
                res = await run_visura(
                    page, prop["provincia"], prop["comune"], prop.get("sezione"),
                    prop["foglio"], prop["particella"], tc, extract_intestati=True,
                )
            intestati = res.get("intestati", []) if isinstance(res, dict) else []
            all_drill.append({"property": prop, "intestati": intestati, "status": "completed"})
        except Exception as e:
            logger.warning("Drill-down failed for %s: %s", prop, e)
            all_drill.append({"property": prop, "intestati": [], "status": "error", "error": str(e)})

    return {
        "drill_results": all_drill,
        "total_properties": len(discovered),
        "truncated": len(discovered) > limit,
    }


async def _exec_elenco(page, params: dict, step_results: list[dict]) -> dict:
    return await run_elenco_immobili(
        page, params["provincia"], params["comune"],
        tipo_catasto=params.get("tipo_catasto", "T"),
        foglio=params.get("foglio"), sezione=params.get("sezione"),
    )


async def _exec_mappa(page, params: dict, step_results: list[dict]) -> dict:
    return await run_ricerca_mappa(
        page, params["provincia"], comune=params["comune"],
        tipo_catasto=params.get("tipo_catasto", "T"), foglio=params.get("foglio"),
    )


async def _exec_fiduciali(page, params: dict, step_results: list[dict]) -> dict:
    return await run_punti_fiduciali(
        page, params["provincia"], comune=params["comune"],
        tipo_catasto=params.get("tipo_catasto", "T"), foglio=params.get("foglio"),
    )


async def _exec_originali(page, params: dict, step_results: list[dict]) -> dict:
    return await run_originali_impianto(
        page, params["provincia"], comune=params["comune"],
        tipo_catasto=params.get("tipo_catasto", "T"), foglio=params.get("foglio"),
    )


async def _exec_ispezioni(page, params: dict, step_results: list[dict]) -> dict:
    return await run_ispezioni(
        page, params["provincia"], comune=params["comune"],
        tipo_catasto=params.get("tipo_catasto", "T"),
        foglio=params.get("foglio"), particella=params.get("particella"),
    )


async def _exec_ispezioni_cart(page, params: dict, step_results: list[dict]) -> dict:
    return await run_ispezioni_cartacee(
        page, params["provincia"], comune=params["comune"],
        tipo_catasto=params.get("tipo_catasto", "T"),
        foglio=params.get("foglio"), particella=params.get("particella"),
    )


async def _exec_indirizzo_search(page, params: dict, step_results: list[dict]) -> dict:
    result = await run_ricerca_indirizzo(
        page, params["provincia"], comune=params["comune"],
        tipo_catasto=params.get("tipo_catasto", "T"), indirizzo=params["indirizzo"],
    )
    # Extract foglio/particella from results for follow-up steps
    if isinstance(result, dict) and not params.get("foglio"):
        immobili = result.get("risultati", result.get("immobili", []))
        if immobili and isinstance(immobili, list) and len(immobili) > 0:
            first = immobili[0]
            fog = first.get("Foglio", "").strip()
            par = first.get("Particella", "").strip()
            if fog:
                params["foglio"] = fog
            if par:
                params["particella"] = par
    return result


async def _exec_elaborato_planimetrico(page, params: dict, step_results: list[dict]) -> dict:
    """Retrieve planimetric floor plan documents (ELPL)."""
    return await run_elaborato_planimetrico(
        page, params["provincia"], params["comune"],
        tipo_catasto=params.get("tipo_catasto", "F"),
        foglio=params.get("foglio"),
    )


async def _exec_export_mappa(page, params: dict, step_results: list[dict]) -> dict:
    """Export full cadastral map data (EXPM)."""
    return await run_export_mappa(
        page, params["provincia"], params["comune"],
        foglio=params.get("foglio", ""),
        tipo_catasto=params.get("tipo_catasto", "T"),
        sezione=params.get("sezione"),
    )


async def _exec_nota(page, params: dict, step_results: list[dict]) -> dict:
    """Search by annotation/note reference."""
    numero_nota = params.get("numero_nota")
    if not numero_nota:
        return {"risultati": [], "total_results": 0, "skipped": "no numero_nota provided"}
    return await run_ricerca_nota(
        page, params["provincia"],
        numero_nota=numero_nota,
        anno_nota=params.get("anno_nota"),
        tipo_catasto=params.get("tipo_catasto", "T"),
    )


async def _exec_cross_property_intestati(page, params: dict, step_results: list[dict]) -> dict:
    """For each unique owner CF found in intestati, run soggetto to discover their full portfolio.

    This turns a single-property workflow into a portfolio discovery.
    Fan-out limited by MAX_DRILL_DOWN.
    """
    # Collect unique codice_fiscale values from all intestati steps
    owner_cfs: set[str] = set()
    for s in step_results:
        if s["status"] != "completed" or not s.get("data"):
            continue
        data = s["data"]
        for owner in data.get("intestati", []):
            cf = (owner.get("Codice fiscale") or owner.get("Codice Fiscale") or "").strip()
            if cf and len(cf) >= 11:
                owner_cfs.add(cf.upper())
        # Also from drill-down results
        for drill in data.get("drill_results", []):
            for owner in drill.get("intestati", []):
                cf = (owner.get("Codice fiscale") or owner.get("Codice Fiscale") or "").strip()
                if cf and len(cf) >= 11:
                    owner_cfs.add(cf.upper())

    if not owner_cfs:
        return {"owner_portfolios": [], "total_owners": 0, "skipped": "no owner CFs found"}

    limit = params.get("max_fanout", MAX_DRILL_DOWN)
    portfolios = []
    for cf in sorted(owner_cfs)[:limit]:
        try:
            # Determine if CF is person (16 chars) or company (11 digits)
            if len(cf) == 11 and cf.isdigit():
                result = await run_visura_persona_giuridica(
                    page, identificativo=cf,
                    tipo_catasto=params.get("tipo_catasto", "E"),
                    provincia=params.get("provincia"),
                )
            else:
                result = await run_visura_soggetto(
                    page, codice_fiscale=cf,
                    tipo_catasto=params.get("tipo_catasto", "E"),
                    provincia=params.get("provincia"),
                )
            immobili = result.get("immobili", []) if isinstance(result, dict) else []
            portfolios.append({
                "codice_fiscale": cf, "status": "completed",
                "immobili": immobili, "total_properties": len(immobili),
            })
        except Exception as e:
            logger.warning("Cross-property lookup failed for CF=%s: %s", cf, e)
            portfolios.append({"codice_fiscale": cf, "status": "error", "error": str(e), "immobili": []})

    return {
        "owner_portfolios": portfolios,
        "total_owners": len(owner_cfs),
        "truncated": len(owner_cfs) > limit,
    }


async def _exec_indirizzo_reverse(page, params: dict, step_results: list[dict]) -> dict:
    """For discovered properties, look up their street addresses.

    Useful after patrimonio/aziendale to get human-readable locations.
    """
    # Collect properties from previous steps
    properties: list[dict] = []
    for s in step_results:
        if s["status"] != "completed" or not s.get("data"):
            continue
        data = s["data"]
        for imm in data.get("immobili", []):
            prop = _normalize_property(imm)
            if prop:
                properties.append(prop)
        for drill in data.get("drill_results", []):
            prop = drill.get("property")
            if prop and prop.get("provincia"):
                properties.append(prop)
        for portfolio in data.get("owner_portfolios", []):
            for imm in portfolio.get("immobili", []):
                prop = _normalize_property(imm)
                if prop:
                    properties.append(prop)

    properties = _deduplicate_properties(properties)
    if not properties:
        return {"addresses": [], "total": 0, "skipped": "no properties to look up"}

    # Group by (provincia, comune) to minimize navigation
    limit = params.get("max_fanout", MAX_DRILL_DOWN)
    addresses = []
    for prop in properties[:limit]:
        if not prop.get("provincia") or not prop.get("comune"):
            continue
        try:
            result = await run_ricerca_indirizzo(
                page, prop["provincia"], comune=prop["comune"],
                tipo_catasto=prop.get("tipo_catasto", "T"),
                indirizzo="",  # empty = list all for the parcel
            )
            # Try to match our parcel in results
            found_addresses = []
            risultati = result.get("risultati", []) if isinstance(result, dict) else []
            for r in risultati:
                if r.get("Foglio", "").strip() == prop.get("foglio") and r.get("Particella", "").strip() == prop.get("particella"):
                    found_addresses.append(r.get("Indirizzo", "").strip())
            addresses.append({
                "property": prop,
                "addresses": found_addresses or ["(not found)"],
            })
        except Exception as e:
            logger.warning("Address reverse lookup failed for %s: %s", prop, e)
            addresses.append({"property": prop, "addresses": [], "error": str(e)})

    return {"addresses": addresses, "total": len(addresses), "truncated": len(properties) > limit}


async def _exec_ispezione_ipotecaria(page, params: dict, step_results: list[dict]) -> dict:
    """Run paid Ispezione Ipotecaria (requires explicit opt-in via include_paid_steps + auto_confirm)."""
    return await run_ispezione_ipotecaria(
        page, provincia=params["provincia"], comune=params.get("comune"),
        tipo_ricerca="immobile",
        foglio=params.get("foglio"), particella=params.get("particella"),
        tipo_catasto=params.get("tipo_catasto", "T"),
        auto_confirm=params.get("auto_confirm", False),
    )


_STEP_EXECUTORS = {
    "search": _exec_search,
    "intestati": _exec_intestati,
    "soggetto": _exec_soggetto,
    "azienda": _exec_azienda,
    "drill_intestati": _exec_drill_intestati,
    "elenco": _exec_elenco,
    "mappa": _exec_mappa,
    "fiduciali": _exec_fiduciali,
    "originali": _exec_originali,
    "ispezioni": _exec_ispezioni,
    "ispezioni_cart": _exec_ispezioni_cart,
    "indirizzo_search": _exec_indirizzo_search,
    "elaborato_planimetrico": _exec_elaborato_planimetrico,
    "export_mappa": _exec_export_mappa,
    "nota": _exec_nota,
    "cross_property_intestati": _exec_cross_property_intestati,
    "indirizzo_reverse": _exec_indirizzo_reverse,
    "ispezione_ipotecaria": _exec_ispezione_ipotecaria,
}


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


async def run_workflow(
    service: VisuraService,
    wf: WorkflowInput,
    workflow_id: Optional[str] = None,
    resume: bool = False,
) -> dict:
    """Execute a multi-step workflow.

    This is the single shared entry point used by both the API endpoint
    and the CLI.

    Args:
        service: The VisuraService with an authenticated browser session.
        wf: Workflow input parameters.
        workflow_id: Optional ID for persistence/resume. Generated if not given.
        resume: If True, skip steps already completed for this workflow_id.

    Returns:
        {
            "workflow_id": str,
            "preset": str,
            "description": str,
            "steps": [...],
            "aggregate": {"properties": [...], "owners": [...], "links": [...]},
            "summary": {...},
        }
    """
    preset_def = WORKFLOW_PRESETS[wf.preset]
    max_depth = _DEPTH_ORDER.get(wf.depth, 1)

    # Filter steps by depth tier and paid flag
    steps_to_run = []
    for step_name in preset_def["steps"]:
        meta = STEP_METADATA.get(step_name, {"depth": "standard", "paid": False})
        step_depth = _DEPTH_ORDER.get(meta["depth"], 1)
        if step_depth > max_depth:
            continue
        if meta["paid"] and not (wf.include_paid_steps and wf.auto_confirm):
            continue
        steps_to_run.append(step_name)

    # Validate required fields
    field_map = {
        "provincia": wf.provincia, "comune": wf.comune, "foglio": wf.foglio,
        "particella": wf.particella, "codice_fiscale": wf.codice_fiscale,
        "identificativo": wf.identificativo, "indirizzo": wf.indirizzo,
    }
    missing = [f for f in preset_def["requires"] if not field_map.get(f)]
    if missing:
        return {"preset": wf.preset, "error": f"Missing required fields: {', '.join(missing)}", "steps": []}

    if not workflow_id:
        workflow_id = f"wf_{wf.preset}_{uuid4().hex[:12]}"

    # Build mutable params dict that steps can update (e.g. indirizzo_search sets foglio)
    params = {
        "provincia": wf.provincia, "comune": wf.comune,
        "foglio": wf.foglio, "particella": wf.particella,
        "tipo_catasto": wf.tipo_catasto or "T",
        "sezione": wf.sezione, "subalterno": wf.subalterno,
        "codice_fiscale": wf.codice_fiscale,
        "identificativo": wf.identificativo,
        "indirizzo": wf.indirizzo,
        "auto_confirm": wf.auto_confirm,
        "max_fanout": wf.max_fanout,
    }

    # Persist workflow run
    try:
        await _save_workflow_run(workflow_id, wf.preset, params)
    except Exception as e:
        logger.warning("Failed to persist workflow run: %s", e)

    # Load completed steps if resuming
    completed_steps: dict[str, dict] = {}
    if resume:
        try:
            completed_steps = await _load_completed_steps(workflow_id)
        except Exception as e:
            logger.warning("Failed to load completed steps for resume: %s", e)

    step_results: list[dict] = []

    async with service.browser_manager._page_lock:
        page = await service.browser_manager._get_authenticated_page()

        for step_name in steps_to_run:
            sk = _step_key(step_name)
            executor = _STEP_EXECUTORS.get(step_name)

            if not executor:
                step_results.append({"step": step_name, "status": "skipped", "data": None, "error": f"Unknown step: {step_name}"})
                continue

            # Resume: skip completed steps
            if sk in completed_steps:
                cached = completed_steps[sk]
                step_results.append({"step": step_name, "status": "completed", "data": cached.get("data"), "resumed": True})
                logger.info("Resumed step '%s' (workflow %s)", step_name, workflow_id)
                continue

            try:
                data = await executor(page, params, step_results)
                step_result = {"step": step_name, "status": "completed", "data": data}
                step_results.append(step_result)

                # Persist step
                try:
                    await _save_workflow_step(workflow_id, sk, "completed", result_json=data)
                except Exception as e:
                    logger.warning("Failed to persist step '%s': %s", step_name, e)

            except Exception as e:
                logger.error("Workflow step '%s' failed: %s", step_name, e)
                step_result = {"step": step_name, "status": "error", "error": str(e)}
                step_results.append(step_result)

                try:
                    await _save_workflow_step(workflow_id, sk, "error", error=str(e))
                except Exception:
                    pass

    # Build aggregate
    aggregate = _build_aggregate(step_results)

    # Summary
    completed_count = sum(1 for s in step_results if s["status"] == "completed")
    failed_count = sum(1 for s in step_results if s["status"] == "error")
    skipped_count = sum(1 for s in step_results if s["status"] == "skipped")

    output = {
        "workflow_id": workflow_id,
        "preset": wf.preset,
        "description": preset_def["description"],
        "steps": step_results,
        "aggregate": aggregate,
        "summary": {
            "total_steps": len(step_results),
            "completed": completed_count,
            "failed": failed_count,
            "skipped": skipped_count,
            "properties": len(aggregate["properties"]),
            "owners": len(aggregate["owners"]),
            "addresses": len(aggregate.get("addresses", [])),
            "risk_flags": len(aggregate.get("risk_flags", [])),
            "links": len(aggregate.get("links", [])),
        },
    }

    # Finalize workflow run
    final_status = "completed" if failed_count == 0 else "partial"
    try:
        await _finish_workflow_run(workflow_id, final_status, output)
    except Exception as e:
        logger.warning("Failed to finalize workflow run: %s", e)

    return output
