"""CLI for the visura-api service (SISTER cadastral data).

Provides subcommands to submit cadastral searches, poll for results,
query history, and check service health.

Usage:
    visura-api search --provincia Trieste --comune TRIESTE --foglio 9 --particella 166
    visura-api get <request_id>
    visura-api wait <request_id>
    visura-api intestati -P Trieste -C TRIESTE -F 9 -p 166 -t F -sub 3
    visura-api history --provincia Trieste --limit 20
    visura-api health
"""

import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from client import VisuraAPIError, VisuraClient

app = typer.Typer(
    name="visura-api",
    help="SISTER cadastral visura service CLI",
    no_args_is_help=True,
)
console = Console()


# -- helpers ------------------------------------------------------------------


def _handle_api_error(e: VisuraAPIError) -> None:
    """Print a styled error from the visura-api and exit."""
    console.print(f"[bold red]Error:[/bold red] visura-api returned HTTP {e.status_code}: {e.detail}")
    raise typer.Exit(1)


def _write_output(data: dict | list, path: str) -> None:
    """Write data to a file, auto-detecting format from the extension."""
    p = Path(path)
    content = json.dumps(data, indent=2, ensure_ascii=False)
    p.write_text(content, encoding="utf-8")
    console.print(f"[dim]Output written to {p}[/dim]")


def _print_result(result: dict) -> None:
    """Pretty-print a visura result with status-aware formatting."""
    status = result.get("status", "unknown")
    request_id = result.get("request_id", "?")

    if status == "processing":
        console.print(
            f"[yellow]Request {request_id} is still processing.[/yellow]\n"
            f"[dim]Run again later or use: visura-api wait {request_id}[/dim]"
        )
        return

    if status == "expired":
        console.print(f"[red]Request {request_id} has expired (cache evicted).[/red]")
        return

    if status == "error":
        error = result.get("error", "unknown error")
        console.print(f"[red]Request {request_id} failed:[/red] {error}")
        return

    if status == "completed":
        tipo = result.get("tipo_catasto", "")
        data = result.get("data", {})
        timestamp = result.get("timestamp", "")

        console.print(
            f"[bold green]Completed[/bold green] {request_id}"
            + (f"  [dim]({tipo})[/dim]" if tipo else "")
            + (f"  [dim]{timestamp}[/dim]" if timestamp else "")
        )

        # Display immobili table if present
        immobili = data.get("immobili", []) if isinstance(data, dict) else []
        if immobili:
            table = Table(title=f"Immobili ({len(immobili)})", header_style="bold cyan")
            cols = list(immobili[0].keys())
            for col in cols:
                table.add_column(col, no_wrap=(col in ("Foglio", "Particella", "Sub")))
            for row in immobili:
                table.add_row(*[str(row.get(c, "")) for c in cols])
            console.print(table)

        # Display intestati table if present
        intestati = data.get("intestati", []) if isinstance(data, dict) else []
        if intestati:
            table = Table(title=f"Intestati ({len(intestati)})", header_style="bold cyan")
            cols = list(intestati[0].keys())
            for col in cols:
                table.add_column(col)
            for row in intestati:
                table.add_row(*[str(row.get(c, "")) for c in cols])
            console.print(table)

        # Fall back to JSON if no structured tables
        if not immobili and not intestati and data:
            console.print(json.dumps(data, indent=2, ensure_ascii=False))

        return

    # Unknown status — dump full result
    console.print(json.dumps(result, indent=2, ensure_ascii=False))


# -- commands -----------------------------------------------------------------


@app.command()
def queries():
    """List available visura-api endpoints."""
    table = Table(title="Visura API endpoints", header_style="bold cyan")
    table.add_column("Command", style="cyan", no_wrap=True)
    table.add_column("Method", style="dim", no_wrap=True)
    table.add_column("Endpoint", style="white")
    table.add_column("Description")

    rows = [
        ("search", "POST", "/visura", "Submit immobili search (Fase 1)"),
        ("intestati", "POST", "/visura/intestati", "Submit owners lookup (Fase 2)"),
        ("get", "GET", "/visura/{request_id}", "Poll for a single result"),
        ("wait", "GET", "/visura/{request_id}", "Poll until complete or timeout"),
        ("history", "GET", "/visura/history", "Query response history"),
        ("health", "GET", "/health", "Service health check"),
    ]
    for cmd, method, ep, desc in rows:
        table.add_row(cmd, method, ep, desc)

    console.print(table)

    client = VisuraClient()
    console.print(f"[dim]Service URL: {client.base_url}[/dim]")


@app.command()
def search(
    provincia: str = typer.Option(..., "--provincia", "-P", help="Province name (e.g. Trieste)"),
    comune: str = typer.Option(..., "--comune", "-C", help="Municipality name (e.g. TRIESTE)"),
    foglio: str = typer.Option(..., "--foglio", "-F", help="Sheet number"),
    particella: str = typer.Option(..., "--particella", "-p", help="Parcel number"),
    tipo_catasto: Optional[str] = typer.Option(
        None, "--tipo-catasto", "-t", help="'T' = Terreni, 'F' = Fabbricati (omit for both)"
    ),
    sezione: Optional[str] = typer.Option(None, "--sezione", help="Section (optional)"),
    subalterno: Optional[str] = typer.Option(None, "--subalterno", "-sub", help="Sub-unit (optional)"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Output file path (.json)"),
    wait: bool = typer.Option(False, "--wait", "-w", help="Wait for results instead of returning immediately"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview request without executing"),
):
    """Submit an immobili search on SISTER (POST /visura).

    By default returns the queued request IDs. Use --wait to poll
    until results are ready.
    """
    payload = {
        "provincia": provincia,
        "comune": comune,
        "foglio": foglio,
        "particella": particella,
    }
    if tipo_catasto:
        payload["tipo_catasto"] = tipo_catasto.upper()
    if sezione:
        payload["sezione"] = sezione
    if subalterno:
        payload["subalterno"] = subalterno

    client = VisuraClient()

    if dry_run:
        console.print("[bold yellow]DRY RUN[/bold yellow] — request will not be sent")
        console.print(f"  POST {client.base_url}/visura")
        console.print(f"  Body: {json.dumps(payload, ensure_ascii=False)}")
        return

    try:
        result = asyncio.run(
            client.search(
                provincia=provincia,
                comune=comune,
                foglio=foglio,
                particella=particella,
                tipo_catasto=tipo_catasto,
                sezione=sezione,
                subalterno=subalterno,
            )
        )
    except VisuraAPIError as e:
        _handle_api_error(e)
        return  # unreachable, keeps type checker happy

    request_ids = result.get("request_ids", [])
    status = result.get("status", "unknown")

    console.print(f"[bold green]Request submitted[/bold green] (status: {status})")
    for rid in request_ids:
        console.print(f"  ID: [cyan]{rid}[/cyan]")

    if not wait:
        console.print(
            "[dim]Poll results with:[/dim]\n"
            + "\n".join(f"  [bold]visura-api get {rid}[/bold]" for rid in request_ids)
        )
        console.print(
            "[dim]Or wait automatically:[/dim]\n"
            + "\n".join(f"  [bold]visura-api wait {rid}[/bold]" for rid in request_ids)
        )
        return

    # --wait: poll each request_id until done
    all_results = {}
    for rid in request_ids:
        console.print(f"\n[dim]Waiting for {rid}...[/dim]")
        try:
            res = asyncio.run(client.wait_for_result(rid))
            all_results[rid] = res
            _print_result(res)
        except TimeoutError as e:
            console.print(f"[yellow]{e}[/yellow]")
        except VisuraAPIError as e:
            console.print(f"[red]{rid}: HTTP {e.status_code}: {e.detail}[/red]")

    if output and all_results:
        merged = all_results if len(all_results) > 1 else next(iter(all_results.values()))
        _write_output(merged, output)


@app.command()
def intestati(
    provincia: str = typer.Option(..., "--provincia", "-P", help="Province name"),
    comune: str = typer.Option(..., "--comune", "-C", help="Municipality name"),
    foglio: str = typer.Option(..., "--foglio", "-F", help="Sheet number"),
    particella: str = typer.Option(..., "--particella", "-p", help="Parcel number"),
    tipo_catasto: str = typer.Option(..., "--tipo-catasto", "-t", help="'T' = Terreni, 'F' = Fabbricati"),
    subalterno: Optional[str] = typer.Option(None, "--subalterno", "-sub", help="Sub-unit (required for Fabbricati)"),
    sezione: Optional[str] = typer.Option(None, "--sezione", help="Section (optional)"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Output file path (.json)"),
    wait: bool = typer.Option(False, "--wait", "-w", help="Wait for result instead of returning immediately"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview request without executing"),
):
    """Submit an owners (intestati) lookup on SISTER (POST /visura/intestati).

    For Fabbricati (tipo_catasto=F), --subalterno is required.
    For Terreni (tipo_catasto=T), --subalterno must not be provided.
    """
    payload = {
        "provincia": provincia,
        "comune": comune,
        "foglio": foglio,
        "particella": particella,
        "tipo_catasto": tipo_catasto.upper(),
    }
    if subalterno:
        payload["subalterno"] = subalterno
    if sezione:
        payload["sezione"] = sezione

    client = VisuraClient()

    if dry_run:
        console.print("[bold yellow]DRY RUN[/bold yellow] — request will not be sent")
        console.print(f"  POST {client.base_url}/visura/intestati")
        console.print(f"  Body: {json.dumps(payload, ensure_ascii=False)}")
        return

    try:
        result = asyncio.run(
            client.intestati(
                provincia=provincia,
                comune=comune,
                foglio=foglio,
                particella=particella,
                tipo_catasto=tipo_catasto,
                subalterno=subalterno,
                sezione=sezione,
            )
        )
    except VisuraAPIError as e:
        _handle_api_error(e)
        return

    request_id = result.get("request_id", "")
    status = result.get("status", "unknown")

    console.print(f"[bold green]Request submitted[/bold green] (status: {status})")
    console.print(f"  ID: [cyan]{request_id}[/cyan]")

    if not wait:
        console.print(f"[dim]Poll result with:[/dim]\n  [bold]visura-api get {request_id}[/bold]")
        return

    console.print(f"\n[dim]Waiting for {request_id}...[/dim]")
    try:
        res = asyncio.run(client.wait_for_result(request_id))
        _print_result(res)
        if output:
            _write_output(res, output)
    except TimeoutError as e:
        console.print(f"[yellow]{e}[/yellow]")
    except VisuraAPIError as e:
        _handle_api_error(e)


@app.command("get")
def get_result(
    request_id: str = typer.Argument(help="Request ID to retrieve"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Output file path (.json)"),
):
    """Get the result of a visura request by ID (GET /visura/{request_id}).

    Returns the current status: processing, completed, error, or expired.
    """
    client = VisuraClient()

    try:
        result = asyncio.run(client.get_result(request_id))
    except VisuraAPIError as e:
        _handle_api_error(e)
        return

    _print_result(result)

    if output:
        _write_output(result, output)


@app.command("wait")
def wait_cmd(
    request_id: str = typer.Argument(help="Request ID to wait for"),
    timeout: Optional[float] = typer.Option(None, "--timeout", "-T", help="Max seconds to wait (default: from env)"),
    interval: Optional[float] = typer.Option(None, "--interval", help="Seconds between polls (default: from env)"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Output file path (.json)"),
):
    """Poll a request until it completes or times out.

    Continuously polls GET /visura/{request_id} until status is
    'completed' or 'error', then prints the result.
    """
    client = VisuraClient()
    start = time.monotonic()

    console.print(f"[dim]Waiting for {request_id}...[/dim]")

    try:
        result = asyncio.run(
            client.wait_for_result(request_id, poll_interval=interval, poll_timeout=timeout)
        )
    except TimeoutError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise typer.Exit(1) from None
    except VisuraAPIError as e:
        _handle_api_error(e)
        return

    elapsed = time.monotonic() - start
    console.print(f"[dim]Completed in {elapsed:.1f}s[/dim]")
    _print_result(result)

    if output:
        _write_output(result, output)


@app.command()
def history(
    provincia: Optional[str] = typer.Option(None, "--provincia", "-P", help="Filter by province"),
    comune: Optional[str] = typer.Option(None, "--comune", "-C", help="Filter by municipality"),
    foglio: Optional[str] = typer.Option(None, "--foglio", "-F", help="Filter by sheet number"),
    particella: Optional[str] = typer.Option(None, "--particella", "-p", help="Filter by parcel"),
    tipo_catasto: Optional[str] = typer.Option(None, "--tipo-catasto", "-t", help="Filter by type (T/F)"),
    limit: int = typer.Option(50, "--limit", "-n", help="Max results to return"),
    offset: int = typer.Option(0, "--offset", help="Offset for pagination"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Output file path (.json)"),
):
    """Query visura response history from the database."""
    client = VisuraClient()

    try:
        result = asyncio.run(
            client.history(
                provincia=provincia,
                comune=comune,
                foglio=foglio,
                particella=particella,
                tipo_catasto=tipo_catasto,
                limit=limit,
                offset=offset,
            )
        )
    except VisuraAPIError as e:
        _handle_api_error(e)
        return

    items = result.get("results", [])
    count = result.get("count", len(items))

    if not items:
        console.print("[dim]No history records found.[/dim]")
        return

    table = Table(title=f"Visura history ({count} results)", header_style="bold cyan")
    table.add_column("#", style="dim", justify="right", no_wrap=True)
    table.add_column("Request ID", style="green", no_wrap=True)
    table.add_column("Type", style="cyan", no_wrap=True)
    table.add_column("Provincia", style="white")
    table.add_column("Comune", style="white")
    table.add_column("Foglio", style="white", no_wrap=True)
    table.add_column("Particella", style="white", no_wrap=True)
    table.add_column("Success", style="yellow", no_wrap=True)
    table.add_column("Created", style="dim", no_wrap=True)

    for i, r in enumerate(items, 1 + offset):
        success = r.get("success")
        success_str = (
            "[green]yes[/green]" if success else "[red]no[/red]" if success is not None else "[dim]-[/dim]"
        )
        table.add_row(
            str(i),
            r.get("request_id", "-"),
            r.get("tipo_catasto", "-"),
            r.get("provincia", "-"),
            r.get("comune", "-"),
            r.get("foglio", "-"),
            r.get("particella", "-"),
            success_str,
            r.get("requested_at", r.get("created_at", "-")),
        )

    console.print(table)

    if output:
        _write_output(result, output)


@app.command()
def health():
    """Check visura-api service health (GET /health)."""
    client = VisuraClient()

    try:
        result = asyncio.run(client.health())
    except VisuraAPIError as e:
        _handle_api_error(e)
        return
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] Cannot reach visura-api at {client.base_url}: {e}")
        raise typer.Exit(1) from None

    status = result.get("status", "unknown")
    authenticated = result.get("authenticated", False)
    queue_size = result.get("queue_size", "?")
    cached = result.get("cached_responses", "?")
    pending = result.get("pending_requests", "?")
    db_stats = result.get("database", {})

    status_style = "green" if status == "healthy" else "red"
    auth_style = "green" if authenticated else "red"

    table = Table(title="Visura API Health", header_style="bold cyan")
    table.add_column("Metric", style="cyan", no_wrap=True)
    table.add_column("Value", style="white")

    table.add_row("Status", f"[{status_style}]{status}[/{status_style}]")
    table.add_row("Authenticated", f"[{auth_style}]{authenticated}[/{auth_style}]")
    table.add_row("Queue size", str(queue_size))
    table.add_row("Pending requests", str(pending))
    table.add_row("Cached responses", str(cached))
    table.add_row("Queue max size", str(result.get("queue_max_size", "?")))
    table.add_row("Response TTL", f"{result.get('response_ttl_seconds', '?')}s")

    if db_stats:
        table.add_section()
        table.add_row("DB total requests", str(db_stats.get("total_requests", "?")))
        table.add_row("DB total responses", str(db_stats.get("total_responses", "?")))
        table.add_row("DB successful", str(db_stats.get("successful", "?")))
        table.add_row("DB failed", str(db_stats.get("failed", "?")))

    console.print(table)
    console.print(f"[dim]Service URL: {client.base_url}[/dim]")


def run():
    """Entry point for the visura-api CLI."""
    app()


if __name__ == "__main__":
    run()
