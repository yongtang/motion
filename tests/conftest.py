import pathlib
import socket
import subprocess
import time
import uuid

import pytest


def pytest_addoption(parser):
    g = parser.getgroup("pytest")
    g.addoption(
        "--server-image",
        action="store",
        default="python:3.12-slim",
        help="Docker image to run in tests",
    )
    g.addoption(
        "--keep", action="store_true", default=False, help="Keep container after tests"
    )


@pytest.fixture(scope="session")
def server_container(request):
    keep = request.config.getoption("--keep")
    image = request.config.getoption("--server-image")
    name = f"test-server-{uuid.uuid4().hex[:8]}"
    mount_dir = pathlib.Path.cwd() / "server"

    port = 8080

    print(
        f"[server_container] Starting container '{name}' port {port} with {mount_dir}"
    )

    # remove any leftover container (once)
    subprocess.run(
        ["docker", "rm", "-f", name],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    # run container before inspect/wait
    run_cmd = [
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
        "bash",
        "-lc",
        f"pip install -r requirements.txt && uvicorn server:app --host 0.0.0.0 --port 8080",
    ]
    subprocess.run(
        run_cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT
    )

    # get container's internal IP
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

    # wait for server to be ready
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            with socket.create_connection((addr, port), timeout=1.0):
                break
        except OSError:
            time.sleep(0.5)
    else:
        raise RuntimeError(f"port not reachable at {addr}:{port}")

    try:
        yield {"addr": addr, "port": port, "name": name}
    finally:
        if not keep:
            subprocess.run(
                ["docker", "rm", "-f", name],
                check=True,  # ← raises CalledProcessError on failure
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
            )
