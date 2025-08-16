import pathlib
import socket
import subprocess
import time
import uuid

import pytest


def pytest_addoption(parser):
    g = parser.getgroup("pytest")
    g.addoption("--keep", action="store_true", default=False)
    g.addoption("--react-image", action="store", default="node:20-alpine")
    g.addoption("--server-image", action="store", default="python:3.12-slim")
    g.addoption(
        "--browser-image",
        action="store",
        default="mcr.microsoft.com/playwright:v1.47.0-jammy",
    )


@pytest.fixture(scope="session")
def docker_container(request):
    def container_run(image, name_prefix, mount_dir, port, run):
        keep = request.config.getoption("--keep")
        name = f"{name_prefix}-{uuid.uuid4().hex[:8]}"

        subprocess.run(
            ["docker", "rm", "-f", name],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "--rm",
                "--name",
                name,
                "-v",
                f"{mount_dir}:/app",
                "-w",
                "/app",
                image,
                "sh",
                "-lc",
                run,
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )

        addr = subprocess.check_output(
            [
                "docker",
                "inspect",
                "-f",
                "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                name,
            ],
            text=True,
        ).strip()

        deadline = time.time() + 60
        while True:
            try:
                with socket.create_connection((addr, int(port)), timeout=1.0):
                    break
            except OSError:
                if time.time() >= deadline:
                    raise RuntimeError(f"{name} not reachable at {addr}:{port}")
                time.sleep(0.5)

        print(f"[container] started name={name} image={image} addr={addr} port={port}")

        try:
            yield {"name": name, "addr": addr, "port": port}
        finally:
            if not keep:
                subprocess.run(
                    ["docker", "rm", "-f", name],
                    check=False,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )

    return container_run


@pytest.fixture(scope="session")
def react_container(docker_container, request):
    image = request.config.getoption("--react-image")
    mount = pathlib.Path.cwd() / "react"
    run = "npm install && npm run dev -- --host"
    yield from docker_container(
        image=image, name_prefix="test-react", mount_dir=mount, port=5173, run=run
    )


@pytest.fixture(scope="session")
def server_container(docker_container, request):
    image = request.config.getoption("--server-image")
    mount = pathlib.Path.cwd()
    run = "pip install -r server/requirements.txt && pip install -e . && uvicorn server.server:app --host 0.0.0.0 --port 8080"
    yield from docker_container(
        image=image, name_prefix="test-server", mount_dir=mount, port=8080, run=run
    )


@pytest.fixture(scope="session")
def browser_container(docker_container, request):
    image = request.config.getoption("--browser-image")
    mount = pathlib.Path.cwd() / "tests"
    run = "npm install --no-audit --no-fund && npx playwright install --with-deps && python3 -m http.server 9999 --bind 0.0.0.0"
    yield from docker_container(
        image=image, name_prefix="test-browser", mount_dir=mount, port=9999, run=run
    )


@pytest.fixture(scope="session")
def browser_run(browser_container, react_container, server_container):
    def f(target: str):
        base = f"http://{react_container['addr']}:{react_container['port']}"

        cmd = [
            "docker",
            "exec",
            "-w",
            "/app",
            "-e",
            f"BASE_URL={base}",
            browser_container["name"],
            "npx",
            "playwright",
            "test",
            "-c",
            "/app/playwright.config.ts",
            f"/app/{target}",
        ]

        proc = subprocess.run(cmd, text=True, capture_output=True)
        print(proc.stdout, end="")
        if proc.stderr:
            print(proc.stderr, end="")
        return proc

    return f
