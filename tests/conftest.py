import concurrent
import importlib.resources
import json
import os
import pathlib
import subprocess
import tempfile
import time
import uuid

import pytest

import motion


def pytest_addoption(parser):
    g = parser.getgroup("pytest")
    g.addoption("--keep", action="store_true", default=False)


@pytest.fixture(scope="session")
def scope(request):
    return f"test-{uuid.uuid4().hex[:8]}"


@pytest.fixture(scope="session")
def docker_compose(scope, request):
    projects = {
        "motion": str(
            importlib.resources.files("motion").joinpath("docker-compose.yml")
        ),
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

    subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            ".docker/docker-compose.build.yml",
            "build",
        ],
        check=True,
        env=env,
    )

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
    """
    Create a Scene on the server using the typed client (POST /scene via SceneClient).
    Returns: (base_url, Scene)
    """
    base = f"http://{docker_compose['motion']}:8080"
    api = motion.client(base, timeout=10.0)

    # Create a temporary USD file; the client will zip + attach meta.json internally.
    with tempfile.TemporaryDirectory() as td:
        usd_path = pathlib.Path(td) / "scene.usd"
        usd_path.write_text("#usda 1.0\ndef X {\n}\n", encoding="utf-8")

        scene: motion.Scene = api.scene.create(
            file=usd_path,
            image="count",  # validated by SceneRunnerImageSpec
            device="cpu",  # validated by SceneRunnerDeviceSpec
        )

    # sanity
    assert isinstance(scene, motion.Scene)
    assert scene.uuid

    yield base, scene

    # teardown once all tests are finished
    try:
        api.scene.delete(scene)
    except Exception:
        pass


@pytest.fixture(scope="session")
def session_on_server(scene_on_server):
    """
    Create a Session bound to the created Scene using the typed client (POST /session).
    Returns: (base_url, Session, Scene)
    """
    base, scene = scene_on_server
    api = motion.client(base, timeout=10.0)

    session: motion.Session = api.session.create(scene=scene)
    # sanity (matches your default server response shape)
    assert session.uuid
    assert session.scene.uuid == scene.uuid
    assert session.joint == ["*"]
    assert isinstance(session.camera, dict)
    assert session.link == ["*"]

    yield base, session, scene

    # teardown
    try:
        api.session.delete(session)
    except Exception:
        pass
