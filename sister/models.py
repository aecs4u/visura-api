"""Data models, exceptions, and Pydantic input schemas for sister."""

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional, Self

from pydantic import BaseModel, Field, field_validator, model_validator

# ---------------------------------------------------------------------------
# Custom Exception Classes
# ---------------------------------------------------------------------------


class VisuraError(Exception):
    """Base exception for visura-related errors"""

    pass


class AuthenticationError(VisuraError):
    """Raised when authentication fails"""

    pass


class BrowserError(VisuraError):
    """Raised when browser operations fail"""

    pass


class QueueFullError(VisuraError):
    """Raised when the request queue is at capacity"""

    pass


# ---------------------------------------------------------------------------
# Internal dataclasses
# ---------------------------------------------------------------------------


@dataclass
class VisuraRequest:
    request_id: str
    tipo_catasto: str
    provincia: str
    comune: str
    foglio: str
    particella: str
    sezione: Optional[str] = None
    subalterno: Optional[str] = None  # Opzionale: restringe la ricerca per fabbricati
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


@dataclass
class VisuraIntestatiRequest:
    """Richiesta per ottenere gli intestati di un immobile specifico"""

    request_id: str
    tipo_catasto: str
    provincia: str
    comune: str
    foglio: str
    particella: str
    subalterno: Optional[str] = None
    sezione: Optional[str] = None
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


@dataclass
class VisuraResponse:
    request_id: str
    success: bool
    tipo_catasto: str
    data: Optional[Dict] = None
    error: Optional[str] = None
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


@dataclass
class SubmitResult:
    """Result of submitting a request — either cached or queued."""

    request_id: str
    cached: bool = False
    response: Optional[VisuraResponse] = None


# ---------------------------------------------------------------------------
# Pydantic input models (API request bodies)
# ---------------------------------------------------------------------------


class VisuraInput(BaseModel):
    """Richiesta per una visura catastale (solo dati catastali, senza intestati)"""

    provincia: str = Field(..., min_length=1, description="Nome della provincia")
    comune: str = Field(..., min_length=1, description="Nome del comune")
    foglio: str = Field(..., min_length=1, description="Numero di foglio")
    particella: str = Field(..., min_length=1, description="Numero di particella")
    sezione: Optional[str] = Field(None, description="Sezione (opzionale)")
    subalterno: Optional[str] = Field(None, description="Subalterno (opzionale, restringe la ricerca per fabbricati)")
    tipo_catasto: Optional[str] = Field(
        None, pattern=r"^[TF]$", description="'T' = Terreni, 'F' = Fabbricati (se omesso esegue entrambi)"
    )

    @field_validator("tipo_catasto", mode="before")
    @classmethod
    def validate_tipo_catasto(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        normalized = value.strip().upper()
        if normalized not in {"T", "F"}:
            raise ValueError(f"tipo_catasto deve essere 'T' o 'F', ricevuto {value}")
        return normalized


class VisuraIntestatiInput(BaseModel):
    """Richiesta per ottenere gli intestati di un immobile specifico"""

    provincia: str = Field(..., min_length=1, description="Nome della provincia")
    comune: str = Field(..., min_length=1, description="Nome del comune")
    foglio: str = Field(..., min_length=1, description="Numero di foglio")
    particella: str = Field(..., min_length=1, description="Numero di particella")
    tipo_catasto: str = Field(..., pattern=r"^[TF]$", description="'T' = Terreni, 'F' = Fabbricati")
    subalterno: Optional[str] = Field(None, description="Numero di subalterno (obbligatorio per Fabbricati)")
    sezione: Optional[str] = Field(None, description="Sezione (opzionale)")

    @field_validator("tipo_catasto", mode="before")
    @classmethod
    def validate_tipo_catasto(cls, value: str) -> str:
        normalized = value.strip().upper()
        if normalized not in {"T", "F"}:
            raise ValueError(f"tipo_catasto deve essere 'T' o 'F', ricevuto {value}")
        return normalized

    @field_validator("subalterno", mode="before")
    @classmethod
    def normalize_subalterno(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None

    @model_validator(mode="after")
    def validate_subalterno(self) -> Self:
        if self.tipo_catasto == "F" and not self.subalterno:
            raise ValueError("subalterno è obbligatorio per i fabbricati (tipo_catasto='F')")
        if self.tipo_catasto == "T" and self.subalterno:
            raise ValueError("subalterno non va indicato per i terreni (tipo_catasto='T')")
        return self


class VisuraSoggettoInput(BaseModel):
    """Richiesta per una ricerca per soggetto (codice fiscale) su SISTER"""

    codice_fiscale: str = Field(..., min_length=11, max_length=16, description="Codice fiscale del soggetto")
    tipo_catasto: Optional[str] = Field(
        None, pattern=r"^[TFE]$", description="'T' = Terreni, 'F' = Fabbricati, 'E' = Entrambi (default)"
    )
    provincia: Optional[str] = Field(None, description="Provincia (ometti per ricerca nazionale)")

    @field_validator("tipo_catasto", mode="before")
    @classmethod
    def validate_tipo_catasto(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        normalized = value.strip().upper()
        if normalized not in {"T", "F", "E"}:
            raise ValueError(f"tipo_catasto deve essere 'T', 'F' o 'E', ricevuto {value}")
        return normalized

    @field_validator("codice_fiscale", mode="before")
    @classmethod
    def normalize_codice_fiscale(cls, value: str) -> str:
        return value.strip().upper()


@dataclass
class VisuraSoggettoRequest:
    """Internal request for soggetto search"""

    request_id: str
    codice_fiscale: str
    tipo_catasto: str = "E"
    provincia: Optional[str] = None
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


class VisuraPersonaGiuridicaInput(BaseModel):
    """Richiesta per ricerca persona giuridica (P.IVA o denominazione)"""

    identificativo: str = Field(..., min_length=1, description="P.IVA (11 cifre) o denominazione azienda")
    tipo_catasto: Optional[str] = Field(
        None, pattern=r"^[TFE]$", description="'T' = Terreni, 'F' = Fabbricati, 'E' = Entrambi"
    )
    provincia: Optional[str] = Field(None, description="Provincia (ometti per ricerca nazionale)")

    @field_validator("tipo_catasto", mode="before")
    @classmethod
    def validate_tipo_catasto(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        normalized = value.strip().upper()
        if normalized not in {"T", "F", "E"}:
            raise ValueError(f"tipo_catasto deve essere 'T', 'F' o 'E', ricevuto {value}")
        return normalized


@dataclass
class VisuraPersonaGiuridicaRequest:
    request_id: str
    identificativo: str
    tipo_catasto: str = "E"
    provincia: Optional[str] = None
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


class ElencoImmobiliInput(BaseModel):
    """Richiesta per elenco immobili di un comune"""

    provincia: str = Field(..., min_length=1, description="Nome della provincia")
    comune: str = Field(..., min_length=1, description="Nome del comune")
    tipo_catasto: Optional[str] = Field(
        None, pattern=r"^[TF]$", description="'T' = Terreni, 'F' = Fabbricati"
    )
    foglio: Optional[str] = Field(None, description="Foglio (opzionale, filtra per foglio)")
    sezione: Optional[str] = Field(None, description="Sezione (opzionale)")

    @field_validator("tipo_catasto", mode="before")
    @classmethod
    def validate_tipo_catasto(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        normalized = value.strip().upper()
        if normalized not in {"T", "F"}:
            raise ValueError(f"tipo_catasto deve essere 'T' o 'F', ricevuto {value}")
        return normalized


@dataclass
class ElencoImmobiliRequest:
    request_id: str
    provincia: str
    comune: str
    tipo_catasto: str = "T"
    foglio: Optional[str] = None
    sezione: Optional[str] = None
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


@dataclass
class GenericSisterRequest:
    """Generic request for SISTER search types (IND, PART, NOTA, EM, EXPM, OOII, FID, ISP, ISPCART)."""

    request_id: str
    search_type: str  # indirizzo, partita, nota, mappa, export_mappa, originali, fiduciali, ispezioni, ispezioni_cart
    provincia: str
    comune: Optional[str] = None
    tipo_catasto: str = "T"
    params: Optional[Dict] = None  # type-specific params (foglio, indirizzo, numero_nota, etc.)
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
        if self.params is None:
            self.params = {}


@dataclass
class IspezioneIpotecariaRequest:
    """Request for Ispezione Ipotecaria (paid inspection service)."""

    request_id: str
    tipo_ricerca: str  # immobile, persona_fisica, persona_giuridica, nota
    provincia: str
    comune: Optional[str] = None
    tipo_catasto: str = "T"
    codice_fiscale: Optional[str] = None
    identificativo: Optional[str] = None
    foglio: Optional[str] = None
    particella: Optional[str] = None
    numero_nota: Optional[str] = None
    anno_nota: Optional[str] = None
    auto_confirm: bool = False
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


class IspezioneIpotecariaInput(BaseModel):
    """API input for Ispezione Ipotecaria (paid inspection)."""

    tipo_ricerca: str = Field(
        ..., description="Search type: immobile, persona_fisica, persona_giuridica, nota"
    )
    provincia: str = Field(..., min_length=1, description="Province name")
    comune: Optional[str] = Field(None, description="Municipality name")
    tipo_catasto: Optional[str] = Field(
        None, pattern=r"^[TF]$", description="'T' = Terreni, 'F' = Fabbricati"
    )
    codice_fiscale: Optional[str] = Field(None, description="Codice fiscale (for persona_fisica)")
    identificativo: Optional[str] = Field(None, description="P.IVA or company name (for persona_giuridica)")
    foglio: Optional[str] = Field(None, description="Sheet number (for immobile)")
    particella: Optional[str] = Field(None, description="Parcel number (for immobile)")
    numero_nota: Optional[str] = Field(None, description="Note number (for nota)")
    anno_nota: Optional[str] = Field(None, description="Note year (for nota)")
    auto_confirm: bool = Field(False, description="Auto-confirm cost without prompting")

    @field_validator("tipo_ricerca", mode="before")
    @classmethod
    def validate_tipo_ricerca(cls, value: str) -> str:
        normalized = value.strip().lower().replace("-", "_")
        valid = {"immobile", "persona_fisica", "persona_giuridica", "nota"}
        if normalized not in valid:
            raise ValueError(f"tipo_ricerca must be one of {valid}, got '{value}'")
        return normalized

    @field_validator("tipo_catasto", mode="before")
    @classmethod
    def validate_tipo_catasto(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        normalized = value.strip().upper()
        if normalized not in {"T", "F"}:
            raise ValueError(f"tipo_catasto deve essere 'T' o 'F', ricevuto {value}")
        return normalized


# Step depth tiers: light < standard < deep
# Each step declares:
#   depth     — minimum depth required to run
#   paid      — requires include_paid_steps + auto_confirm
#   produces  — data keys this step adds (for 'when' clauses)
#   when      — callable(step_results, params) → bool; if False, step is skipped
STEP_METADATA = {
    # Core discovery (light)
    "search":                    {"depth": "light",    "paid": False, "produces": ["immobili"]},
    "intestati":                 {"depth": "light",    "paid": False, "produces": ["intestati"]},
    "soggetto":                  {"depth": "light",    "paid": False, "produces": ["immobili"]},
    "azienda":                   {"depth": "light",    "paid": False, "produces": ["immobili"]},
    "elenco":                    {"depth": "light",    "paid": False, "produces": ["immobili"]},
    "indirizzo_search":          {"depth": "light",    "paid": False, "produces": ["immobili"]},
    # Standard enrichment
    "drill_intestati":           {"depth": "standard", "paid": False, "produces": ["intestati", "drill_results"]},
    "mappa":                     {"depth": "standard", "paid": False, "produces": ["risultati"],
                                  "when": lambda results, params: bool(params.get("foglio"))},
    "fiduciali":                 {"depth": "standard", "paid": False, "produces": ["risultati"],
                                  "when": lambda results, params: bool(params.get("foglio"))},
    "originali":                 {"depth": "standard", "paid": False, "produces": ["risultati"]},
    "ispezioni":                 {"depth": "standard", "paid": False, "produces": ["risultati"]},
    "ispezioni_cart":            {"depth": "standard", "paid": False, "produces": ["risultati"]},
    "elaborato_planimetrico":    {"depth": "standard", "paid": False, "produces": ["risultati"]},
    "export_mappa":              {"depth": "standard", "paid": False, "produces": ["risultati"],
                                  "when": lambda results, params: bool(params.get("foglio"))},
    "nota":                      {"depth": "standard", "paid": False, "produces": ["risultati"],
                                  "when": lambda results, params: bool(params.get("numero_nota"))},
    "indirizzo_reverse":         {"depth": "standard", "paid": False, "produces": ["addresses"]},
    # Deep enrichment (fan-out)
    "cross_property_intestati":  {"depth": "deep",     "paid": False, "produces": ["owner_portfolios"],
                                  "when": lambda results, params: any(
                                      s["status"] == "completed" and s.get("data", {}).get("intestati")
                                      for s in results)},
    # Analytical / post-processing (no browser needed)
    "owner_expand":              {"depth": "deep",     "paid": False, "produces": ["owner_portfolios"],
                                  "when": lambda results, params: any(
                                      s["status"] == "completed" and s.get("data", {}).get("intestati")
                                      for s in results)},
    "timeline_build":            {"depth": "standard", "paid": False, "produces": ["timeline"],
                                  "when": lambda results, params: any(
                                      s["status"] == "completed" and s["step"] in ("nota", "ispezioni", "ispezioni_cart", "originali")
                                      for s in results)},
    "risk_score":                {"depth": "light",    "paid": False, "produces": ["risk_scores"]},
    # Multi-hop (full depth — bounded graph expansion)
    "property_rank":             {"depth": "full",     "paid": False, "produces": ["ranked_properties"],
                                  "when": lambda results, params: any(
                                      s["status"] == "completed" and (
                                          s.get("data", {}).get("discovered_properties")
                                          or s.get("data", {}).get("owner_entities")
                                          or s.get("data", {}).get("drill_results")
                                      ) for s in results)},
    "portfolio_drill_intestati": {"depth": "full",     "paid": False, "produces": ["portfolio_intestati"],
                                  "when": lambda results, params: any(
                                      s["status"] == "completed" and s["step"] == "property_rank"
                                      for s in results)},
    "portfolio_history":         {"depth": "full",     "paid": False, "produces": ["history_results"],
                                  "when": lambda results, params: any(
                                      s["status"] == "completed" and s["step"] == "property_rank"
                                      for s in results)},
    "portfolio_ipotecaria":      {"depth": "full",     "paid": True,  "produces": ["paid_results"],
                                  "when": lambda results, params: any(
                                      s["status"] == "completed" and s["step"] == "property_rank"
                                      for s in results)},
    # Paid
    "ispezione_ipotecaria":      {"depth": "deep",     "paid": True,  "produces": ["risultati"]},
}

_DEPTH_ORDER = {"light": 0, "standard": 1, "deep": 2, "full": 3}

WORKFLOW_PRESETS = {
    "due-diligence": {
        "description": "Real estate due diligence: search → intestati → ispezioni → elaborato → timeline → risk scoring",
        "steps": [
            "search", "intestati", "ispezioni", "elaborato_planimetrico",
            "owner_expand", "timeline_build",
            "ispezione_ipotecaria",
            "risk_score",
        ],
        "requires": ["provincia", "comune", "foglio", "particella"],
    },
    "patrimonio": {
        "description": "Asset investigation: soggetto → drill-down intestati → owner expand → address lookup → risk scoring",
        "steps": [
            "soggetto", "drill_intestati", "indirizzo_reverse",
            "owner_expand",
            "ispezione_ipotecaria",
            "risk_score",
        ],
        "requires": ["codice_fiscale"],
    },
    "fondiario": {
        "description": "Land survey: elenco → mappa → export → fiduciali → originali → elaborato → risk scoring",
        "steps": [
            "elenco", "mappa", "export_mappa", "fiduciali", "originali",
            "elaborato_planimetrico",
            "risk_score",
        ],
        "requires": ["provincia", "comune"],
    },
    "aziendale": {
        "description": "Corporate audit: azienda → drill-down intestati → owner expand → address lookup → risk scoring",
        "steps": [
            "azienda", "drill_intestati", "indirizzo_reverse",
            "owner_expand",
            "ispezione_ipotecaria",
            "risk_score",
        ],
        "requires": ["identificativo"],
    },
    "storico": {
        "description": "Parcel history: search → intestati → nota → ispezioni → originali → elaborato → timeline → risk scoring",
        "steps": [
            "search", "intestati", "nota", "ispezioni", "ispezioni_cart", "originali",
            "elaborato_planimetrico",
            "owner_expand", "timeline_build",
            "ispezione_ipotecaria",
            "risk_score",
        ],
        "requires": ["provincia", "comune", "foglio", "particella"],
    },
    "indirizzo": {
        "description": "Address lookup: indirizzo → search → intestati → owner expand → risk scoring",
        "steps": [
            "indirizzo_search", "search", "intestati",
            "owner_expand",
            "risk_score",
        ],
        "requires": ["provincia", "comune", "indirizzo"],
    },
    "cross-reference": {
        "description": "Cross-reference: soggetto + azienda → cross-property overlap → risk scoring",
        "steps": [
            "soggetto", "azienda", "cross_property_intestati",
            "risk_score",
        ],
        "requires": ["codice_fiscale", "identificativo"],
    },
    "full-due-diligence": {
        "description": "Multi-hop due diligence: seed parcel → owners → portfolios → selective history → encumbrances → risk scoring",
        "steps": [
            # Hop 0: seed parcel
            "search", "intestati",
            # Hop 1: expand parcel variants
            "drill_intestati",
            # Hop 2: expand owners
            "owner_expand",
            # Rank before further expansion
            "property_rank",
            # Hop 3: drill into owner portfolios (top ranked unseen)
            "portfolio_drill_intestati",
            # Hop 4: selective history on ranked properties
            "portfolio_history", "timeline_build",
            # Hop 5: selective paid enrichment on top-risk
            "portfolio_ipotecaria",
            # Final
            "risk_score",
        ],
        "requires": ["provincia", "comune", "foglio", "particella"],
    },
    "full-patrimonio": {
        "description": "Multi-hop portfolio: soggetto → drill-down → owners → portfolios → history → encumbrances → risk scoring",
        "steps": [
            "soggetto", "drill_intestati", "indirizzo_reverse",
            "owner_expand", "property_rank",
            "portfolio_drill_intestati",
            "portfolio_history", "timeline_build",
            "portfolio_ipotecaria",
            "risk_score",
        ],
        "requires": ["codice_fiscale"],
    },
    "full-aziendale": {
        "description": "Multi-hop corporate audit: azienda → drill-down → owners → portfolios → history → encumbrances → risk scoring",
        "steps": [
            "azienda", "drill_intestati", "indirizzo_reverse",
            "owner_expand", "property_rank",
            "portfolio_drill_intestati",
            "portfolio_history", "timeline_build",
            "portfolio_ipotecaria",
            "risk_score",
        ],
        "requires": ["identificativo"],
    },
}


class WorkflowInput(BaseModel):
    """API input for multi-step workflow execution."""

    preset: str = Field(..., description="Workflow preset name")
    provincia: Optional[str] = Field(None, description="Province name")
    comune: Optional[str] = Field(None, description="Municipality name")
    foglio: Optional[str] = Field(None, description="Sheet number")
    particella: Optional[str] = Field(None, description="Parcel number")
    tipo_catasto: Optional[str] = Field(
        None, pattern=r"^[TFE]$", description="'T' = Terreni, 'F' = Fabbricati, 'E' = Both"
    )
    sezione: Optional[str] = Field(None, description="Section (optional)")
    subalterno: Optional[str] = Field(None, description="Sub-unit (optional)")
    codice_fiscale: Optional[str] = Field(None, description="Codice fiscale")
    identificativo: Optional[str] = Field(None, description="P.IVA or company name")
    indirizzo: Optional[str] = Field(None, description="Street address")
    auto_confirm: bool = Field(False, description="Auto-confirm paid service costs")
    include_paid_steps: bool = Field(False, description="Include optional paid steps (e.g. ispezione ipotecaria)")
    depth: str = Field("standard", description="Workflow depth: light, standard, deep, full")
    # Budget controls for multi-hop expansion
    max_fanout: int = Field(20, ge=1, le=100, description="Max properties/owners to fan out to per step")
    max_owners: int = Field(10, ge=1, le=50, description="Max owners to expand in owner_expand")
    max_properties_per_owner: int = Field(20, ge=1, le=100, description="Max properties per owner in portfolio drill")
    max_historical_properties: int = Field(5, ge=1, le=50, description="Max properties to run history bundle on")
    max_paid_steps: int = Field(3, ge=0, le=20, description="Max paid step invocations (ispezione ipotecaria)")
    max_total_steps: int = Field(100, ge=1, le=500, description="Overall circuit breaker for total step executions")

    @field_validator("depth", mode="before")
    @classmethod
    def validate_depth(cls, value: str) -> str:
        normalized = value.strip().lower()
        if normalized not in {"light", "standard", "deep", "full"}:
            raise ValueError(f"depth must be 'light', 'standard', 'deep', or 'full', got '{value}'")
        return normalized

    @field_validator("preset", mode="before")
    @classmethod
    def validate_preset(cls, value: str) -> str:
        normalized = value.strip().lower()
        if normalized not in WORKFLOW_PRESETS:
            raise ValueError(f"Unknown preset '{value}'. Available: {', '.join(WORKFLOW_PRESETS)}")
        return normalized

    @field_validator("tipo_catasto", mode="before")
    @classmethod
    def validate_tipo_catasto(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        normalized = value.strip().upper()
        if normalized not in {"T", "F", "E"}:
            raise ValueError(f"tipo_catasto deve essere 'T', 'F' o 'E', ricevuto {value}")
        return normalized


class SezioniExtractionRequest(BaseModel):
    """Richiesta per l'estrazione delle sezioni territoriali"""

    tipo_catasto: str = Field("T", pattern=r"^[TF]$", description="'T' = Terreni, 'F' = Fabbricati")
    max_province: int = Field(
        200, ge=1, le=200, description="Numero massimo di province da processare (default: tutte)"
    )

    @field_validator("tipo_catasto", mode="before")
    @classmethod
    def validate_tipo_catasto(cls, value: str) -> str:
        normalized = value.strip().upper()
        if normalized not in {"T", "F"}:
            raise ValueError(f"tipo_catasto deve essere 'T' o 'F', ricevuto {value}")
        return normalized
