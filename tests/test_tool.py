import subprocess
import sys


def test_tool(docker_compose, tmp_path):
    base = f"http://{docker_compose['motion']}:8080"

    file = tmp_path.joinpath("scene.usd")
    file.write_text("# usd")

    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "src.motion.tool",
            str(file),
            "--runtime",
            "echo",
            "--base",
            base,
            "--duration",
            "150",
        ],
        stderr=subprocess.STDOUT,
        stdout=subprocess.PIPE,
        text=True,
        timeout=300,
    )

    assert proc.returncode == 0, proc.stdout
    assert "[Tool] Done" in proc.stdout
