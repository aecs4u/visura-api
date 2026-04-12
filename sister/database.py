"""SQLite database layer for sister (SQLModel + async SQLAlchemy).

Provides persistent storage for visura requests, responses, and structured
result tables (immobili, intestati). Includes cache lookup for deduplication.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel, select

from sqlalchemy import MetaData

from .db_models import (
    IMMOBILE_FIELD_MAP,
    INTESTATO_FIELD_MAP,
    ImmobileDB,
    IntestatoDB,
    PageVisitDB,
    VisuraDocumentDB,
    VisuraRequestDB,
    VisuraResponseDB,
    WorkflowRunDB,
    WorkflowStepDB,
)

# Collect only sister's tables — avoid creating tables from other packages
# that share the global SQLModel.metadata
_SISTER_TABLES = [
    VisuraRequestDB.__table__,
    VisuraResponseDB.__table__,
    ImmobileDB.__table__,
    IntestatoDB.__table__,
    WorkflowRunDB.__table__,
    WorkflowStepDB.__table__,
    PageVisitDB.__table__,
    VisuraDocumentDB.__table__,
]

logger = logging.getLogger("sister")

DB_PATH = os.getenv("SISTER_DB_PATH", os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "sister.sqlite"))

# ---------------------------------------------------------------------------
# Engine and session
# ---------------------------------------------------------------------------

_engine = None


def _get_engine():
    global _engine
    if _engine is None:
        os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
        url = f"sqlite+aiosqlite:///{DB_PATH}"
        _engine = create_async_engine(url, echo=False)
    return _engine


def _get_session_factory():
    return sessionmaker(_get_engine(), class_=AsyncSession, expire_on_commit=False)


async def init_db() -> None:
    """Create sister tables if they don't exist."""
    engine = _get_engine()
    async with engine.begin() as conn:
        # Only create sister's tables, not all SQLModel-registered tables
        # (avoids conflicts with opendata models in shared venvs)
        def _create_sister_tables(sync_conn):
            for table in _SISTER_TABLES:
                table.create(sync_conn, checkfirst=True)
        await conn.run_sync(_create_sister_tables)
        await conn.execute(text("PRAGMA journal_mode=WAL"))
        await conn.execute(text("PRAGMA foreign_keys=ON"))
    logger.info("Database inizializzato: %s", DB_PATH)


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------


def compute_cache_key(request_type: str, **params) -> str:
    """Deterministic cache key from search parameters."""
    # Filter out None values and sort for determinism
    filtered = {k: v for k, v in params.items() if v is not None}
    canonical = json.dumps({"type": request_type, **filtered}, sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(canonical.encode()).hexdigest()


async def find_cached_response(cache_key: str, ttl_seconds: int) -> Optional[dict]:
    """Find a successful, non-expired response matching the cache key."""
    session_factory = _get_session_factory()
    async with session_factory() as session:
        cutoff = datetime.now() - timedelta(seconds=ttl_seconds)
        stmt = (
            select(VisuraResponseDB)
            .join(VisuraRequestDB)
            .where(
                VisuraRequestDB.cache_key == cache_key,
                VisuraResponseDB.success == True,  # noqa: E712
                VisuraResponseDB.created_at >= cutoff,
            )
            .order_by(VisuraResponseDB.created_at.desc())
            .limit(1)
        )
        result = await session.execute(stmt)
        row = result.scalar_one_or_none()
        if row is None:
            return None
        return {
            "request_id": row.request_id,
            "success": row.success,
            "tipo_catasto": row.tipo_catasto,
            "data": row.data,
            "error": row.error,
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }


# ---------------------------------------------------------------------------
# Request operations (same signatures as before)
# ---------------------------------------------------------------------------


async def save_request(
    request_id: str,
    request_type: str,
    tipo_catasto: str,
    provincia: str,
    comune: str,
    foglio: str,
    particella: str,
    sezione: Optional[str] = None,
    subalterno: Optional[str] = None,
    cache_key: Optional[str] = None,
) -> None:
    """Persist a new request."""
    session_factory = _get_session_factory()
    async with session_factory() as session:
        row = VisuraRequestDB(
            request_id=request_id,
            request_type=request_type,
            tipo_catasto=tipo_catasto,
            provincia=provincia,
            comune=comune,
            foglio=foglio,
            particella=particella,
            sezione=sezione,
            subalterno=subalterno,
            cache_key=cache_key,
        )
        session.add(row)
        await session.commit()


async def save_requests_batch(requests: list[dict]) -> None:
    """Persist multiple requests atomically."""
    if not requests:
        return
    session_factory = _get_session_factory()
    async with session_factory() as session:
        for req in requests:
            row = VisuraRequestDB(
                request_id=req["request_id"],
                request_type=req["request_type"],
                tipo_catasto=req["tipo_catasto"],
                provincia=req["provincia"],
                comune=req["comune"],
                foglio=req["foglio"],
                particella=req["particella"],
                sezione=req.get("sezione"),
                subalterno=req.get("subalterno"),
                cache_key=req.get("cache_key"),
            )
            session.add(row)
        await session.commit()


# ---------------------------------------------------------------------------
# Response operations
# ---------------------------------------------------------------------------


def _parse_immobili(response_id: str, tipo_catasto: str, data: Optional[dict]) -> list[ImmobileDB]:
    """Parse immobili from response JSON into structured rows."""
    if not data or not isinstance(data, dict):
        return []
    rows = []
    for item in data.get("immobili", []):
        if not isinstance(item, dict):
            continue
        kwargs: dict[str, Any] = {"response_id": response_id, "tipo_catasto": tipo_catasto}
        for html_key, db_col in IMMOBILE_FIELD_MAP.items():
            if html_key in item:
                kwargs[db_col] = str(item[html_key]).strip() or None
        rows.append(ImmobileDB(**kwargs))
    return rows


def _parse_intestati(response_id: str, data: Optional[dict]) -> list[IntestatoDB]:
    """Parse intestati from response JSON into structured rows."""
    if not data or not isinstance(data, dict):
        return []
    rows = []
    for item in data.get("intestati", []):
        if not isinstance(item, dict):
            continue
        kwargs: dict[str, Any] = {"response_id": response_id}
        for html_key, db_col in INTESTATO_FIELD_MAP.items():
            if html_key in item:
                val = str(item[html_key]).strip() or None
                # For nominativo, concatenate if Cognome+Nome pattern
                if db_col == "nominativo" and kwargs.get("nominativo") and val:
                    kwargs["nominativo"] = f"{kwargs['nominativo']} {val}"
                else:
                    kwargs[db_col] = val
        rows.append(IntestatoDB(**kwargs))
    return rows


def _parse_page_visits(response_id: str, data: Optional[dict]) -> list[PageVisitDB]:
    """Parse page_visits from response JSON into structured rows."""
    if not data or not isinstance(data, dict):
        return []
    visits = data.get("page_visits", [])
    if not isinstance(visits, list):
        return []
    rows = []
    for item in visits:
        if not isinstance(item, dict):
            continue
        ts = None
        if item.get("timestamp"):
            try:
                ts = datetime.fromisoformat(item["timestamp"])
            except (ValueError, TypeError):
                pass
        rows.append(PageVisitDB(
            response_id=response_id,
            step=item.get("step", ""),
            url=item.get("url"),
            screenshot_url=item.get("screenshot_url"),
            form_elements_json=json.dumps(item.get("form_elements", []), default=str) if item.get("form_elements") else None,
            errors_json=json.dumps(item.get("errors", []), default=str) if item.get("errors") else None,
            timestamp=ts,
        ))
    return rows


async def save_response(
    request_id: str,
    success: bool,
    tipo_catasto: str,
    data: Optional[dict] = None,
    error: Optional[str] = None,
) -> None:
    """Persist a response and populate structured tables."""
    session_factory = _get_session_factory()
    async with session_factory() as session:
        # Delete existing response + related rows if any (upsert)
        await session.execute(text("DELETE FROM page_visits WHERE response_id = :rid"), {"rid": request_id})
        await session.execute(text("DELETE FROM intestati WHERE response_id = :rid"), {"rid": request_id})
        await session.execute(text("DELETE FROM immobili WHERE response_id = :rid"), {"rid": request_id})
        await session.execute(text("DELETE FROM visura_responses WHERE request_id = :rid"), {"rid": request_id})

        resp = VisuraResponseDB(
            request_id=request_id,
            success=success,
            tipo_catasto=tipo_catasto,
            data=data,
            error=error,
        )
        session.add(resp)

        # Populate structured tables from JSON
        for imm in _parse_immobili(request_id, tipo_catasto, data):
            session.add(imm)
        for intest in _parse_intestati(request_id, data):
            session.add(intest)
        for pv in _parse_page_visits(request_id, data):
            session.add(pv)

        await session.commit()

    # Export to outputs/ directory
    _export_response_file(request_id, success, tipo_catasto, data, error)


OUTPUTS_DIR = os.getenv("SISTER_OUTPUTS_DIR", os.path.join(os.path.dirname(os.path.dirname(__file__)), "outputs"))


def _export_response_file(
    request_id: str,
    success: bool,
    tipo_catasto: str,
    data: Optional[dict] = None,
    error: Optional[str] = None,
) -> None:
    """Write response JSON to outputs/ directory."""
    try:
        outputs_dir = Path(OUTPUTS_DIR)
        outputs_dir.mkdir(exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        payload = {
            "request_id": request_id,
            "success": success,
            "tipo_catasto": tipo_catasto,
            "data": data,
            "error": error,
            "exported_at": datetime.now().isoformat(),
        }
        filename = f"{request_id}_{ts}.json"
        (outputs_dir / filename).write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str))
        logger.info("Response exported to outputs/%s", filename)
    except Exception as e:
        logger.warning("Failed to export response file: %s", e)


async def get_response(request_id: str) -> Optional[dict]:
    """Fetch a stored response by request_id. Returns None if not found."""
    session_factory = _get_session_factory()
    async with session_factory() as session:
        row = await session.get(VisuraResponseDB, request_id)
        if row is None:
            return None
        return {
            "request_id": row.request_id,
            "success": row.success,
            "tipo_catasto": row.tipo_catasto,
            "data": row.data,
            "error": row.error,
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }


async def get_result_record(request_id: str) -> Optional[dict]:
    """Fetch joined request/response data for the web results detail page.

    Returns None only when the request itself does not exist. Requests without a
    response are returned with ``status='pending'`` so the UI can distinguish
    pending work from a genuinely unknown request id.
    """
    session_factory = _get_session_factory()
    async with session_factory() as session:
        stmt = (
            select(VisuraRequestDB, VisuraResponseDB)
            .outerjoin(VisuraResponseDB, VisuraRequestDB.request_id == VisuraResponseDB.request_id)
            .where(VisuraRequestDB.request_id == request_id)
        )
        result = await session.execute(stmt)
        row = result.one_or_none()
        if row is None:
            return None

        req, resp = row
        status = "pending"
        if resp is not None:
            status = "completed" if resp.success else "failed"

        return {
            "request_id": req.request_id,
            "request_type": req.request_type,
            "tipo_catasto": req.tipo_catasto,
            "provincia": req.provincia,
            "comune": req.comune,
            "foglio": req.foglio,
            "particella": req.particella,
            "sezione": req.sezione,
            "subalterno": req.subalterno,
            "cost_text": req.cost_text,
            "cost_value": req.cost_value,
            "requested_at": req.created_at.isoformat() if req.created_at else None,
            "responded_at": resp.created_at.isoformat() if resp and resp.created_at else None,
            "success": resp.success if resp else None,
            "status": status,
            "data": resp.data if resp else None,
            "error": resp.error if resp else None,
            "page_visits": (
                resp.data.get("page_visits", [])
                if resp and isinstance(resp.data, dict) and isinstance(resp.data.get("page_visits"), list)
                else []
            ),
        }


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------


async def find_responses(
    provincia: Optional[str] = None,
    comune: Optional[str] = None,
    foglio: Optional[str] = None,
    particella: Optional[str] = None,
    tipo_catasto: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
) -> list[dict]:
    """Search stored responses by cadastral coordinates."""
    session_factory = _get_session_factory()
    async with session_factory() as session:
        stmt = (
            select(VisuraRequestDB, VisuraResponseDB)
            .outerjoin(VisuraResponseDB, VisuraRequestDB.request_id == VisuraResponseDB.request_id)
        )
        if provincia:
            stmt = stmt.where(VisuraRequestDB.provincia == provincia)
        if comune:
            stmt = stmt.where(VisuraRequestDB.comune == comune)
        if foglio:
            stmt = stmt.where(VisuraRequestDB.foglio == foglio)
        if particella:
            stmt = stmt.where(VisuraRequestDB.particella == particella)
        if tipo_catasto:
            stmt = stmt.where(VisuraRequestDB.tipo_catasto == tipo_catasto)

        stmt = stmt.order_by(VisuraRequestDB.created_at.desc()).limit(limit).offset(offset)

        result = await session.execute(stmt)
        rows = result.all()

        return [
            {
                "request_id": req.request_id,
                "request_type": req.request_type,
                "tipo_catasto": req.tipo_catasto,
                "provincia": req.provincia,
                "comune": req.comune,
                "foglio": req.foglio,
                "particella": req.particella,
                "sezione": req.sezione,
                "subalterno": req.subalterno,
                "requested_at": req.created_at.isoformat() if req.created_at else None,
                "success": resp.success if resp else None,
                "data": resp.data if resp else None,
                "error": resp.error if resp else None,
                "responded_at": resp.created_at.isoformat() if resp and resp.created_at else None,
            }
            for req, resp in rows
        ]


async def cleanup_old_responses(ttl_seconds: int) -> int:
    """Delete responses older than ttl_seconds. Returns count of deleted rows."""
    session_factory = _get_session_factory()
    async with session_factory() as session:
        cutoff = datetime.now() - timedelta(seconds=ttl_seconds)

        # Find expired responses
        stmt = select(VisuraResponseDB).where(VisuraResponseDB.created_at < cutoff)
        result = await session.execute(stmt)
        expired = result.scalars().all()
        deleted = len(expired)

        for resp in expired:
            await session.delete(resp)

        if deleted:
            # Also delete orphaned requests
            orphan_stmt = select(VisuraRequestDB).where(
                VisuraRequestDB.created_at < cutoff,
                ~VisuraRequestDB.request_id.in_(
                    select(VisuraResponseDB.request_id)
                ),
            )
            orphan_result = await session.execute(orphan_stmt)
            for req in orphan_result.scalars().all():
                await session.delete(req)

        await session.commit()

        # Checkpoint WAL so writes flush to the main .sqlite file
        await session.execute(text("PRAGMA wal_checkpoint(TRUNCATE)"))

        return deleted


async def count_responses() -> dict:
    """Return basic stats about stored data."""
    session_factory = _get_session_factory()
    async with session_factory() as session:
        total_requests = (await session.execute(
            select(text("count(*)")).select_from(VisuraRequestDB)
        )).scalar() or 0

        total_responses = (await session.execute(
            select(text("count(*)")).select_from(VisuraResponseDB)
        )).scalar() or 0

        successful = (await session.execute(
            select(text("count(*)")).select_from(VisuraResponseDB).where(VisuraResponseDB.success == True)  # noqa: E712
        )).scalar() or 0

        failed = (await session.execute(
            select(text("count(*)")).select_from(VisuraResponseDB).where(VisuraResponseDB.success == False)  # noqa: E712
        )).scalar() or 0

        return {
            "total_requests": total_requests,
            "total_responses": total_responses,
            "successful": successful,
            "failed": failed,
            "pending": max(total_requests - total_responses, 0),
        }
