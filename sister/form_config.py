"""Form group definitions for the sister web UI.

Defines the query forms rendered on the /web/forms page. Each FormGroup
maps to one or more sister API endpoints.

Categories:
  - "single": Single-step queries (search, soggetto, azienda, etc.)
  - "workflow": Multi-step workflow presets (due-diligence, patrimonio, etc.)

The "batch" mode is a toggle on any form group, not a separate group.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class EndpointParam:
    name: str
    label: str
    placeholder: str
    required: bool = True
    input_type: str = "text"
    help_text: Optional[str] = None
    example: Optional[str] = None
    options: Optional[list[tuple[str, str]]] = None


@dataclass
class EndpointOption:
    id: str
    name: str
    path: str
    description: str
    method: str = "POST"


@dataclass
class FormGroup:
    id: str
    name: str
    description: str
    icon: str
    color: str
    params: list[EndpointParam]
    endpoints: list[EndpointOption]
    default_endpoint_id: str = ""
    category: str = "single"  # "single" or "workflow"
    available: bool = True
    # For workflows: the SVG flowchart name (without .svg)
    flowchart: Optional[str] = None


# ---------------------------------------------------------------------------
# Shared parameter definitions
# ---------------------------------------------------------------------------

_TIPO_CATASTO = EndpointParam(
    name="tipo_catasto", label="Cadastre Type", placeholder="Select type",
    input_type="select", required=False,
    options=[("", "Both (T+F)"), ("T", "Terreni (T)"), ("F", "Fabbricati (F)")],
    help_text="T = Land, F = Buildings. Leave blank for both.",
)

_TIPO_CATASTO_TF = EndpointParam(
    name="tipo_catasto", label="Cadastre Type", placeholder="Select type",
    input_type="select", required=False,
    options=[("T", "Terreni (T)"), ("F", "Fabbricati (F)")],
)

_TIPO_CATASTO_TFE = EndpointParam(
    name="tipo_catasto", label="Cadastre Type", placeholder="Select type",
    input_type="select", required=False,
    options=[("", "Both (E)"), ("T", "Terreni (T)"), ("F", "Fabbricati (F)")],
)

_PROVINCIA = EndpointParam(
    name="provincia", label="Province", placeholder="e.g. Roma",
    help_text="Province name", example="Roma",
)

_COMUNE = EndpointParam(
    name="comune", label="Municipality", placeholder="e.g. ROMA",
    help_text="Municipality name (uppercase)", example="ROMA",
)

_FOGLIO = EndpointParam(
    name="foglio", label="Sheet (Foglio)", placeholder="e.g. 100",
    example="100",
)

_PARTICELLA = EndpointParam(
    name="particella", label="Parcel (Particella)", placeholder="e.g. 50",
    example="50",
)

_SEZIONE = EndpointParam(
    name="sezione", label="Section", placeholder="Optional",
    required=False, help_text="Census section (if applicable)",
)

_SUBALTERNO = EndpointParam(
    name="subalterno", label="Sub-unit (Subalterno)", placeholder="e.g. 3",
    required=False, help_text="Required for Fabbricati intestati",
)

_PROVINCIA_OPT = EndpointParam(
    name="provincia", label="Province", placeholder="Leave blank for national search",
    required=False, help_text="Omit for nationwide search",
)

_FOGLIO_OPT = EndpointParam(
    name="foglio", label="Sheet", placeholder="Required for property presets",
    required=False,
)

_PARTICELLA_OPT = EndpointParam(
    name="particella", label="Parcel", placeholder="Required for property presets",
    required=False,
)


# ---------------------------------------------------------------------------
# Single-step form groups
# ---------------------------------------------------------------------------

SINGLE_STEP_GROUPS: list[FormGroup] = [
    FormGroup(
        id="property-search",
        name="Property Search",
        description="Search for properties by cadastral coordinates (sheet + parcel).",
        icon="fa-search",
        color="primary",
        category="single",
        params=[_TIPO_CATASTO, _PROVINCIA, _COMUNE, _SEZIONE, _FOGLIO, _PARTICELLA, _SUBALTERNO],
        endpoints=[
            EndpointOption(
                id="visura", name="Property Data",
                path="/visura", method="POST",
                description="Find all properties on a parcel (Fase 1)",
            ),
            EndpointOption(
                id="intestati", name="Owner Lookup",
                path="/visura/intestati", method="POST",
                description="Get owners for a specific property (Fase 2)",
            ),
        ],
        default_endpoint_id="visura",
    ),

    FormGroup(
        id="person-search",
        name="Person Search",
        description="National search by codice fiscale.",
        icon="fa-user",
        color="info",
        category="single",
        params=[
            EndpointParam(
                name="codice_fiscale", label="Codice Fiscale",
                placeholder="e.g. RSSMRI85E28H501E",
                help_text="16-character tax code", example="RSSMRI85E28H501E",
            ),
            _TIPO_CATASTO_TFE,
            _PROVINCIA_OPT,
        ],
        endpoints=[
            EndpointOption(
                id="soggetto", name="Person Search",
                path="/visura/soggetto", method="POST",
                description="National search by codice fiscale",
            ),
        ],
        default_endpoint_id="soggetto",
    ),

    FormGroup(
        id="company-search",
        name="Company Search",
        description="Search by P.IVA or company name.",
        icon="fa-building",
        color="warning",
        category="single",
        params=[
            EndpointParam(
                name="identificativo", label="P.IVA or Company Name",
                placeholder="e.g. 02471840997",
                help_text="Enter 11-digit P.IVA or company denomination",
                example="02471840997",
            ),
            _TIPO_CATASTO_TFE,
            _PROVINCIA_OPT,
        ],
        endpoints=[
            EndpointOption(
                id="persona-giuridica", name="Company Search",
                path="/visura/persona-giuridica", method="POST",
                description="Search by P.IVA or denomination",
            ),
        ],
        default_endpoint_id="persona-giuridica",
    ),

    FormGroup(
        id="property-list",
        name="Property List",
        description="List all properties in a municipality.",
        icon="fa-list",
        color="success",
        category="single",
        params=[_PROVINCIA, _COMUNE, _TIPO_CATASTO_TF, EndpointParam(
            name="foglio", label="Sheet (Foglio)", placeholder="Optional — filter by sheet",
            required=False,
        ), _SEZIONE],
        endpoints=[
            EndpointOption(
                id="elenco-immobili", name="Property List",
                path="/visura/elenco-immobili", method="POST",
                description="List all properties in a municipality",
            ),
        ],
        default_endpoint_id="elenco-immobili",
    ),

    FormGroup(
        id="address-search",
        name="Address Search",
        description="Search properties by street address.",
        icon="fa-map-marker-alt",
        color="danger",
        category="single",
        params=[
            _PROVINCIA, _COMUNE, _TIPO_CATASTO_TF,
            EndpointParam(
                name="indirizzo", label="Address",
                placeholder="e.g. VIA ROMA",
                help_text="Street name (partial match supported)",
                example="VIA ROMA",
            ),
        ],
        endpoints=[
            EndpointOption(
                id="indirizzo", name="Address Search",
                path="/visura/indirizzo", method="POST",
                description="Find properties at a given address",
            ),
        ],
        default_endpoint_id="indirizzo",
    ),

    FormGroup(
        id="partita-search",
        name="Partita Search",
        description="Search by partita catastale number.",
        icon="fa-hashtag",
        color="secondary",
        category="single",
        params=[
            _PROVINCIA, _COMUNE, _TIPO_CATASTO_TF,
            EndpointParam(
                name="partita", label="Partita Number",
                placeholder="e.g. 12345",
                help_text="Cadastral partita number",
            ),
        ],
        endpoints=[
            EndpointOption(
                id="partita", name="Partita Search",
                path="/visura/partita", method="POST",
                description="Search by partita catastale number",
            ),
        ],
        default_endpoint_id="partita",
    ),
]


# ---------------------------------------------------------------------------
# Workflow (multi-step) form groups
# ---------------------------------------------------------------------------

WORKFLOW_GROUPS: list[FormGroup] = [
    FormGroup(
        id="wf-due-diligence",
        name="Due Diligence",
        description="Real estate due diligence: search → intestati → ispezioni",
        icon="fa-file-contract",
        color="primary",
        category="workflow",
        flowchart="due-diligence",
        params=[_PROVINCIA, _COMUNE, _FOGLIO, _PARTICELLA, _TIPO_CATASTO, _SEZIONE],
        endpoints=[EndpointOption(
            id="workflow-due-diligence", name="Due Diligence",
            path="/visura", method="POST",
            description="search → intestati → ispezioni",
        )],
        default_endpoint_id="workflow-due-diligence",
    ),

    FormGroup(
        id="wf-patrimonio",
        name="Asset Investigation",
        description="Find all properties owned by a person across Italy.",
        icon="fa-search-dollar",
        color="info",
        category="workflow",
        flowchart="patrimonio",
        params=[
            EndpointParam(name="codice_fiscale", label="Codice Fiscale",
                          placeholder="e.g. RSSMRI85E28H501E", example="RSSMRI85E28H501E"),
            _TIPO_CATASTO_TFE, _PROVINCIA_OPT,
        ],
        endpoints=[EndpointOption(
            id="workflow-patrimonio", name="Asset Investigation",
            path="/visura/soggetto", method="POST",
            description="soggetto (nazionale) → drill-down",
        )],
        default_endpoint_id="workflow-patrimonio",
    ),

    FormGroup(
        id="wf-fondiario",
        name="Land Survey",
        description="Complete cadastral data for a zone: elenco → mappa → fiduciali → originali",
        icon="fa-mountain",
        color="success",
        category="workflow",
        flowchart="fondiario",
        params=[_PROVINCIA, _COMUNE, _FOGLIO_OPT, _TIPO_CATASTO_TF],
        endpoints=[EndpointOption(
            id="workflow-fondiario", name="Land Survey",
            path="/visura/elenco-immobili", method="POST",
            description="elenco → mappa → fiduciali → originali",
        )],
        default_endpoint_id="workflow-fondiario",
    ),

    FormGroup(
        id="wf-aziendale",
        name="Corporate Audit",
        description="Find all properties owned by a company.",
        icon="fa-briefcase",
        color="warning",
        category="workflow",
        flowchart="aziendale",
        params=[
            EndpointParam(name="identificativo", label="P.IVA / Company",
                          placeholder="e.g. 02471840997", example="02471840997"),
            _TIPO_CATASTO_TFE, _PROVINCIA_OPT,
        ],
        endpoints=[EndpointOption(
            id="workflow-aziendale", name="Corporate Audit",
            path="/visura/persona-giuridica", method="POST",
            description="azienda → drill-down",
        )],
        default_endpoint_id="workflow-aziendale",
    ),

    FormGroup(
        id="wf-storico",
        name="Parcel History",
        description="Full historical record: search → intestati → nota → ispezioni → originali",
        icon="fa-history",
        color="dark",
        category="workflow",
        flowchart="storico",
        params=[_PROVINCIA, _COMUNE, _FOGLIO, _PARTICELLA, _TIPO_CATASTO],
        endpoints=[EndpointOption(
            id="workflow-storico", name="Parcel History",
            path="/visura", method="POST",
            description="search → intestati → nota → ispezioni → originali",
        )],
        default_endpoint_id="workflow-storico",
    ),
]


# ---------------------------------------------------------------------------
# Combined list
# ---------------------------------------------------------------------------

FORM_GROUPS: list[FormGroup] = SINGLE_STEP_GROUPS + WORKFLOW_GROUPS


def get_available_form_groups() -> list[FormGroup]:
    """Return form groups that are available."""
    return [fg for fg in FORM_GROUPS if fg.available]


def get_single_step_groups() -> list[FormGroup]:
    """Return single-step form groups."""
    return [fg for fg in FORM_GROUPS if fg.available and fg.category == "single"]


def get_workflow_groups() -> list[FormGroup]:
    """Return workflow (multi-step) form groups."""
    return [fg for fg in FORM_GROUPS if fg.available and fg.category == "workflow"]


def get_form_group_by_id(group_id: str) -> Optional[FormGroup]:
    """Find a form group by ID."""
    return next((fg for fg in FORM_GROUPS if fg.id == group_id), None)


def get_endpoint_by_id(endpoint_id: str) -> Optional[EndpointOption]:
    """Find an endpoint across all form groups."""
    for fg in FORM_GROUPS:
        for ep in fg.endpoints:
            if ep.id == endpoint_id:
                return ep
    return None
