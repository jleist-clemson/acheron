"""Shared helpers for the Docker-backed integration suites.

Both integration modules (with and without a live Elasticsearch) need the same
Docker-socket autodetection and the same "poll a GET until a predicate holds"
loop. Keeping them here stops the two copies from drifting.
"""
from __future__ import annotations

import asyncio
import json
import os
import subprocess
from typing import Any, Callable


def ensure_docker_host() -> None:
    """Point the Docker SDK at the active socket if ``DOCKER_HOST`` isn't set.

    The Python Docker SDK only honors ``DOCKER_HOST`` (it ignores docker CLI
    contexts), so on Rancher Desktop / Colima — where the socket isn't at the
    default ``/var/run/docker.sock`` — testcontainers can't find Docker. Derive
    the endpoint from the active context as a fallback; leave it unset on failure
    so container startup fails and the suite skips.
    """
    if os.environ.get("DOCKER_HOST") or os.path.exists("/var/run/docker.sock"):
        return
    try:
        out = subprocess.check_output(
            ["docker", "context", "inspect"], text=True, stderr=subprocess.DEVNULL
        )
        os.environ["DOCKER_HOST"] = json.loads(out)[0]["Endpoints"]["docker"]["Host"]
    except Exception:
        pass  # leave unset; container startup will fail and the test will skip


async def poll_until(
    client: Any,
    path: str,
    params: dict,
    predicate: Callable[[dict], bool],
    timeout: float = 10.0,
) -> dict:
    """Poll a GET endpoint until *predicate* holds or *timeout* elapses.

    The async pipeline (queue → worker → store, outbox → ES) is eventually
    consistent, so reads are polled rather than asserted once.

    Args:
        client: An httpx async client bound to the app.
        path: The endpoint to GET.
        params: Query parameters for the request.
        predicate: Returns True once the parsed JSON response is satisfactory.
        timeout: Maximum seconds to poll before giving up.

    Returns:
        The JSON response satisfying *predicate*, or the last response on timeout
        (so the caller's assertions still produce a useful diff).
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    data: dict = {}
    while loop.time() < deadline:
        data = (await client.get(path, params=params)).json()
        if predicate(data):
            return data
        await asyncio.sleep(0.05)
    return data
