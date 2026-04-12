"""Tests for web UI route handlers."""

from types import SimpleNamespace

import pytest

import sister.web as web


class _FakeTheme:
    def render(self, template_name, request, **context):
        return {
            "template": template_name,
            "request": request,
            "context": context,
        }


def _fake_request():
    return SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(theme_setup=_FakeTheme())),
        state=SimpleNamespace(user=None),
        url=SimpleNamespace(path="/web/results"),
    )


@pytest.mark.asyncio
async def test_web_results_passes_extended_filters_and_urls(monkeypatch):
    request = _fake_request()

    async def _find_responses(**kwargs):
        assert kwargs["foglio"] == "9"
        assert kwargs["particella"] == "166"
        return [{
            "request_id": "req_1",
            "request_type": "visura",
            "tipo_catasto": "F",
            "provincia": "Trieste",
            "comune": "TRIESTE",
            "foglio": "9",
            "particella": "166",
            "requested_at": "2026-04-12T10:30:00",
            "responded_at": None,
            "success": None,
        }]

    async def _count_responses():
        return {"total_requests": 1, "total_responses": 0, "successful": 0, "failed": 0, "pending": 1}

    monkeypatch.setattr(web, "find_responses", _find_responses)
    monkeypatch.setattr(web, "count_responses", _count_responses)

    response = await web.web_results(
        request,
        user=None,
        provincia="Trieste",
        comune="TRIESTE",
        foglio="9",
        particella="166",
        tipo_catasto="F",
        limit=1,
        offset=1,
    )

    context = response["context"]
    assert context["results"][0]["status"] == "pending"
    assert context["results"][0]["requested_at_display"] == "2026-04-12 10:30"
    assert context["prev_url"] is not None
    assert "foglio=9" in context["prev_url"]
    assert "particella=166" in context["prev_url"]
    assert "tipo_catasto=F" in context["prev_url"]


@pytest.mark.asyncio
async def test_web_result_detail_renders_pending_request(monkeypatch):
    request = _fake_request()

    async def _get_result_record(_request_id):
        return {
            "request_id": "req_pending",
            "request_type": "visura",
            "tipo_catasto": "T",
            "provincia": "Roma",
            "comune": "ROMA",
            "foglio": "1",
            "particella": "2",
            "sezione": None,
            "subalterno": None,
            "requested_at": "2026-04-12T10:30:00",
            "responded_at": None,
            "success": None,
            "status": "pending",
            "data": None,
            "error": None,
            "page_visits": [],
        }

    monkeypatch.setattr(web, "get_result_record", _get_result_record)

    response = await web.web_result_detail(request, "req_pending", user=None)

    context = response["context"]
    assert context["result"]["status"] == "pending"
    assert context["result"]["requested_at_display"] == "2026-04-12 10:30"
    assert context["result"]["sections"] == []


@pytest.mark.asyncio
async def test_web_result_detail_marks_not_found(monkeypatch):
    request = _fake_request()

    async def _get_result_record(_request_id):
        return None

    monkeypatch.setattr(web, "get_result_record", _get_result_record)

    response = await web.web_result_detail(request, "missing_id", user=None)

    assert response["context"]["result"] is None
    assert response["context"]["not_found"] is True
