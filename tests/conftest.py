import concurrent
import io
import json
import os
import subprocess
import time
import uuid
import zipfile

import httpx
import pytest


def pytest_addoption(parser):
    g = parser.getgroup("pytest")
    g.addoption("--keep", action="store_true", default=False)


@pytest.fixture(scope="session")
def scope(request):
    return f"test-{uuid.uuid4().hex[:8]}"


@pytest.fixture(scope="session")
def docker_compose(scope, request):
    projects = {
        "motion": "docker/docker-compose.yml",
        "tests": "tests/docker-compose.yml",
    }

    def ready(compose_project, timeout=600, poll_every=1.0):
        deadline = time.time() + timeout
        while True:
            output = subprocess.run(
                [
                    "docker",
                    "compose",
                    "-p",
                    compose_project,
                    "ps",
                    "--format",
                    "{{json .}}",
                ],
                check=True,
                capture_output=True,
                text=True,
            ).stdout

            lines = [ln for ln in output.splitlines() if ln.strip()]
            if not lines:
                if time.time() > deadline:
                    raise TimeoutError(
                        f"No containers found for project '{compose_project}'."
                    )
                time.sleep(poll_every)
                continue

            services = [json.loads(ln) for ln in lines]

            def f_service_ready(s: dict) -> bool:
                state_ok = s.get("State", "").lower() == "running"
                if s.get("Service", "").startswith("node-"):
                    return state_ok
                return state_ok and (s.get("Health", "").lower() == "healthy")

            if all(f_service_ready(s) for s in services):
                return

            if time.time() > deadline:
                raise TimeoutError(
                    f"Project '{compose_project}' not healthy before timeout."
                )
            time.sleep(poll_every)

    env = {**os.environ, "SCOPE": scope}

    def _up(short: str, file: str) -> str:
        compose_project = f"{scope}-{short}"
        subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                file,
                "-p",
                compose_project,
                "up",
                "-d",
                "--remove-orphans",
            ],
            check=True,
            env=env,
        )
        return compose_project

    # 1) Start ALL stacks in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(projects)) as ex:
        up_futs = {
            ex.submit(_up, short, file): short for short, file in projects.items()
        }
        compose_projects: dict[str, str] = {}
        for fut in concurrent.futures.as_completed(up_futs):
            short = up_futs[fut]
            compose_projects[short] = fut.result()  # propagate exceptions

    # 2) Wait for ALL stacks to be ready in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(projects)) as ex:
        ready_futs = {
            ex.submit(ready, compose_projects[short]): short
            for short in projects.keys()
        }
        for fut in concurrent.futures.as_completed(ready_futs):
            fut.result()  # propagate exceptions

    try:
        entries: dict[str, str] = {}

        for short, file in projects.items():
            compose_project = f"{scope}-{short}"
            output = subprocess.run(
                [
                    "docker",
                    "compose",
                    "-p",
                    compose_project,
                    "ps",
                    "--format",
                    "{{json .}}",
                ],
                check=True,
                capture_output=True,
                text=True,
            ).stdout

            uniq = {
                subprocess.run(
                    [
                        "docker",
                        "inspect",
                        "-f",
                        "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                        json.loads(ln)["ID"],
                    ],
                    check=True,
                    capture_output=True,
                    text=True,
                ).stdout.strip()
                for ln in output.splitlines()
                if ln.strip()
            }
            uniq.discard("")

            if len(uniq) != 1:
                raise AssertionError(
                    f"Expected exactly one bridged IP in project '{compose_project}', got: {uniq}"
                )

            entries[short] = uniq.pop()

        yield entries

    finally:
        if not request.config.getoption("--keep"):
            for short, file in projects.items():
                compose_project = f"{scope}-{short}"
                subprocess.run(
                    [
                        "docker",
                        "compose",
                        "-f",
                        file,
                        "-p",
                        compose_project,
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
        base = f"http://{docker_compose['motion']}:5173"

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
    """Create a scene (POST /scene) with a zip that includes scene.usd + meta.json."""
    base = f"http://{docker_compose['motion']}:8080"

    # build in-memory zip with USD + meta.json
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        usd_contents = "#usda 1.0\ndef X {\n}\n"
        z.writestr("scene.usd", usd_contents)
        z.writestr("meta.json", json.dumps({"runner": "echo"}))
    buf.seek(0)

    files = {"file": ("scene.zip", buf, "application/zip")}
    r = httpx.post(f"{base}/scene", files=files, timeout=5.0)
    assert r.status_code == 201, r.text
    scene = r.json()["uuid"]
    assert scene

    yield base, scene

    # teardown once all tests are finished
    try:
        httpx.delete(f"{base}/scene/{scene}", timeout=5.0)
    except Exception:
        pass


@pytest.fixture(scope="session")
def session_on_server(scene_on_server):
    """Create a session bound to the scene created above."""
    base, scene = scene_on_server
    r = httpx.post(f"{base}/session", json={"scene": scene}, timeout=5.0)
    assert r.status_code == 201, r.text
    data = r.json()
    session = data["uuid"]
    assert data == {
        "uuid": session,
        "scene": scene,
        "joint": ["*"],
        "camera": {},
        "link": ["*"],
    }

    yield base, session, scene

    # teardown
    try:
        httpx.delete(f"{base}/session/{session}", timeout=5.0)
    except Exception:
        pass
