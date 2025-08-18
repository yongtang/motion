import io
import json
import os
import subprocess
import time
import uuid
import zipfile

import pytest
import requests


def pytest_addoption(parser):
    g = parser.getgroup("pytest")
    g.addoption("--keep", action="store_true", default=False)


@pytest.fixture(scope="session")
def scope(request):
    return f"test-{uuid.uuid4().hex[:8]}"


@pytest.fixture(scope="session")
def docker_compose(scope, request):
    projects = {
        f"{scope}-motion": "docker/docker-compose.yml",
        f"{scope}-tests": "tests/docker-compose.yml",
    }

    def ready(project, timeout=180, poll_every=1.0):
        deadline = time.time() + timeout
        while True:
            # NOTE: use a line-per-object JSON format
            ps_out = subprocess.run(
                ["docker", "compose", "-p", project, "ps", "--format", "{{json .}}"],
                check=True,
                capture_output=True,
                text=True,
            ).stdout

            lines = [ln for ln in ps_out.splitlines() if ln.strip()]
            if not lines:
                if time.time() > deadline:
                    raise TimeoutError(f"No containers found for project '{project}'.")
                time.sleep(poll_every)
                continue

            services = [json.loads(ln) for ln in lines]
            all_ready = all(
                (s.get("State", "").lower() == "running")
                and (s.get("Health", "").lower() == "healthy")
                for s in services
            )
            if all_ready:
                return services

            if time.time() > deadline:
                raise TimeoutError(f"Project '{project}' not healthy before timeout.")
            time.sleep(poll_every)

    # Bring both up (no --build), remove orphans; pass SCOPE only
    env = {**os.environ, "SCOPE": scope}
    for project, compose_file in projects.items():
        subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                compose_file,
                "-p",
                project,
                "up",
                "-d",
                "--remove-orphans",
            ],
            check=True,
            env=env,
        )
        ready(project)

    try:
        # Collect {service_name: ip} for BOTH projects (service names must be unique across stacks)
        merged = {}
        for project in projects:
            ps_out = subprocess.run(
                ["docker", "compose", "-p", project, "ps", "--format", "{{json .}}"],
                check=True,
                capture_output=True,
                text=True,
            ).stdout

            for ln in (ln for ln in ps_out.splitlines() if ln.strip()):
                s = json.loads(ln)
                cid = s["ID"]
                ip = subprocess.run(
                    [
                        "docker",
                        "inspect",
                        "-f",
                        "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                        cid,
                    ],
                    check=True,
                    capture_output=True,
                    text=True,
                ).stdout.strip()

                name = s["Service"]  # plain service name (you said no prefix)
                if name in merged:
                    # If you *truly* guarantee no collisions, this shouldn't happen.
                    # Guard anyway to avoid silent overwrite.
                    raise RuntimeError(
                        f"Service name collision: '{name}' from project '{project}'"
                    )
                merged[name] = ip

        yield merged

    finally:
        if not request.config.getoption("--keep"):
            for project, compose_file in projects.items():
                subprocess.run(
                    [
                        "docker",
                        "compose",
                        "-f",
                        compose_file,
                        "-p",
                        project,
                        "down",
                        "-v",
                        "--remove-orphans",
                    ],
                    check=False,
                    env=env,
                )


@pytest.fixture(scope="session")
def browser_run(docker_compose, scope):
    def f(target: str):
        base = f"http://{docker_compose['server']}:5173"

        cmd = [
            "docker",
            "exec",
            "-w",
            "/app/tests",
            "-e",
            f"BASE_URL={base}",
            f"{scope}-browser",
            "npx",
            "playwright",
            "test",
            f"{target}",
        ]

        proc = subprocess.run(cmd, text=True, capture_output=True)
        print(proc.stdout, end="")
        if proc.stderr:
            print(proc.stderr, end="")
        return proc

    return f


@pytest.fixture(scope="session")
def scene_on_server(docker_compose):
    """Create a scene (POST /scene with a tiny zip)."""
    base = f"http://{docker_compose['server']}:8080"

    # prepare in-memory zip
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("hello.txt", "world")
    buf.seek(0)

    files = {"file": ("scene.zip", buf, "application/zip")}
    r = requests.post(f"{base}/scene", files=files, timeout=5.0)
    assert r.status_code == 201, r.text
    scene = r.json()["uuid"]
    assert scene

    yield base, scene

    # teardown once all tests are finished
    try:
        requests.delete(f"{base}/scene/{scene}", timeout=5.0)
    except Exception:
        pass


@pytest.fixture(scope="session")
def session_on_server(scene_on_server):
    """Create a session bound to the scene created above."""
    base, scene = scene_on_server
    r = requests.post(f"{base}/session", json={"scene": scene}, timeout=5.0)
    assert r.status_code == 201, r.text
    data = r.json()
    session = data["uuid"]
    assert data == {"uuid": session, "scene": scene}

    yield base, session, scene

    # teardown
    try:
        requests.delete(f"{base}/session/{session}", timeout=5.0)
    except Exception:
        pass
