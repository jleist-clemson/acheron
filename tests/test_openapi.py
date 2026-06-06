"""The events API advertises typed response schemas in its OpenAPI document.

Container-free: asserts the response models from app.api.schemas are wired as
each route's response_model — the point of typed responses is an accurate
``/docs`` contract, so this guards against a route silently reverting to an
untyped ``dict``.
"""
from __future__ import annotations

import json


def test_response_models_are_wired_into_openapi() -> None:
    import app.main as main

    schema = main.app.openapi()
    components = schema["components"]["schemas"]
    for model in (
        "IngestResponse",
        "EventListResponse",
        "StatsResponse",
        "RealtimeStatsResponse",
        "SearchResponse",
        "DeadLetterListResponse",
        "ReplayResponse",
    ):
        assert model in components, f"{model} missing from OpenAPI components"

    paths = schema["paths"]

    def response_for(path: str, method: str, code: str) -> str:
        return json.dumps(paths[path][method]["responses"][code])

    # Each route's success response references its typed model.
    assert "EventListResponse" in response_for("/events", "get", "200")
    assert "IngestResponse" in response_for("/events", "post", "202")
    assert "StatsResponse" in response_for("/events/stats", "get", "200")
    assert "RealtimeStatsResponse" in response_for("/events/stats/realtime", "get", "200")
    assert "SearchResponse" in response_for("/events/search", "get", "200")
    assert "DeadLetterListResponse" in response_for("/events/dlq", "get", "200")
    assert "ReplayResponse" in response_for("/events/dlq/{event_id}/replay", "post", "200")
