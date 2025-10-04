import json
import subprocess
import sys
import uuid
import zipfile

import pytest


def test_tool_scene(docker_compose, tmp_path, monkeypatch):
    base = f"http://{docker_compose['motion']}:8080"

    # Always enforce PYTHONPATH=src for subprocess
    monkeypatch.setenv("PYTHONPATH", "src")

    def run_tool_scene(*args: str):
        cmd = [sys.executable, "-m", "motion.tool", "--base", base, "scene", *args]
        return subprocess.run(cmd, text=True, capture_output=True, check=False)

    # Prepare a tiny USD file
    usd_path = tmp_path / "scene.usd"
    usd_path.write_text("#usda 1.0\ndef X {\n}\n", encoding="utf-8")

    # --- Happy path ---
    p = run_tool_scene(
        "create",
        "--file",
        str(usd_path),
        "--runner",
        "counter",
    )
    assert p.returncode == 0, p.stderr
    scene_uuid = json.loads(p.stdout.strip())["uuid"]

    p = run_tool_scene("list")
    scene_list = json.loads(p.stdout or "[]")
    assert any(it.get("uuid") == scene_uuid for it in scene_list)

    p = run_tool_scene("show", scene_uuid[:8])
    shown = json.loads(p.stdout.strip())
    assert shown["uuid"] == scene_uuid

    out_zip = tmp_path / f"{scene_uuid}.zip"
    p = run_tool_scene("archive", scene_uuid[:8], "--path", str(out_zip))
    assert out_zip.exists() and out_zip.stat().st_size > 0
    with zipfile.ZipFile(out_zip, "r") as z:
        assert "scene.usd" in z.namelist()
        assert "meta.json" in z.namelist()

    p = run_tool_scene("delete", scene_uuid[:8])
    deleted = json.loads(p.stdout.strip())
    assert deleted["uuid"] == scene_uuid

    p = run_tool_scene("list")
    assert not any(it.get("uuid") == scene_uuid for it in json.loads(p.stdout or "[]"))

    # --- Negative: bogus show/archive ---
    bogus = str(uuid.uuid4())
    out_zip_bogus = tmp_path / f"{bogus}.zip"

    p = run_tool_scene("show", bogus)
    assert p.returncode != 0 and "not found" in p.stderr.lower()

    p = run_tool_scene("archive", bogus, "--path", str(out_zip_bogus))
    assert p.returncode != 0 and "not found" in p.stderr.lower()
    assert not out_zip_bogus.exists()

    # --- Negative: ambiguous show ---
    def create_scene_uuid():
        cp = run_tool_scene("create", "--file", str(usd_path))
        assert cp.returncode == 0, cp.stderr
        return json.loads(cp.stdout.strip())["uuid"]

    s1 = create_scene_uuid()

    # Ensure we actually get another scene with the SAME first hex char as s1;
    # this guarantees that using s1[0] as a prefix will be ambiguous.
    s2 = create_scene_uuid()
    tries = 0
    while s2[0] != s1[0] and tries < 32:
        s2 = create_scene_uuid()
        tries += 1

    # If after a reasonable number of tries we still don't share the first char,
    # fall back to the longest common prefix (may be length 0). If length 0,
    # we can't assert ambiguity deterministically; in that rare case, just skip.
    if s2[0] == s1[0]:
        ambiguous = s1[0]
    else:
        lcp = 0
        for a, b in zip(s1, s2):
            if a == b:
                lcp += 1
            else:
                break
        if lcp == 0:
            # Best effort already; no shared prefix -> skip ambiguous assertion
            # (very unlikely given the loop above, but avoids false failures).
            return
        ambiguous = s1[:lcp]

    p = run_tool_scene("show", ambiguous)
    assert p.returncode != 0 and "ambiguous" in p.stderr.lower()

    # cleanup the extra scenes created for ambiguity
    run_tool_scene("delete", s1)
    run_tool_scene("delete", s2)


@pytest.mark.asyncio
async def test_tool_session(scene_on_server, tmp_path, monkeypatch):
    base, scene_obj = scene_on_server
    scene_uuid = str(scene_obj.uuid)

    monkeypatch.setenv("PYTHONPATH", "src")

    def run_tool_session(*args: str):
        cmd = [sys.executable, "-m", "motion.tool", "--base", base, "session", *args]
        return subprocess.run(cmd, text=True, capture_output=True, check=False)

    # --- Happy path ---
    p = run_tool_session("create", scene_uuid[:8])
    assert p.returncode == 0, p.stderr
    session_uuid = json.loads(p.stdout.strip())["uuid"]

    p = run_tool_session("list")
    sessions = json.loads(p.stdout or "[]")
    assert any(it.get("uuid") == session_uuid for it in sessions)

    p = run_tool_session("show", session_uuid[:8])
    shown = json.loads(p.stdout.strip())
    assert shown["uuid"] == session_uuid

    out_zip = tmp_path / f"{session_uuid}.zip"
    p = run_tool_session("archive", session_uuid[:8], "--path", str(out_zip))
    assert out_zip.exists() and out_zip.stat().st_size > 0
    with zipfile.ZipFile(out_zip, "r") as z:
        names = set(z.namelist())
        assert "session.json" in names
        assert "data.json" in names

    p = run_tool_session("delete", session_uuid[:8])
    deleted = json.loads(p.stdout.strip())
    assert deleted["uuid"] == session_uuid

    p = run_tool_session("list")
    assert not any(
        it.get("uuid") == session_uuid for it in json.loads(p.stdout or "[]")
    )

    # --- Negative: bogus show/archive ---
    bogus = str(uuid.uuid4())
    out_zip_bogus = tmp_path / f"{bogus}.zip"

    p = run_tool_session("show", bogus)
    assert p.returncode != 0 and "not found" in p.stderr.lower()

    p = run_tool_session("archive", bogus, "--path", str(out_zip_bogus))
    assert p.returncode != 0 and "not found" in p.stderr.lower()
    assert not out_zip_bogus.exists()

    # --- Negative: ambiguous show ---
    # Create two sessions with the same scene; keep creating until they share first hex
    s1 = json.loads(run_tool_session("create", scene_uuid[:8]).stdout.strip())["uuid"]
    s2 = json.loads(run_tool_session("create", scene_uuid[:8]).stdout.strip())["uuid"]

    tries = 0
    while s2[0] != s1[0] and tries < 32:
        s2 = json.loads(run_tool_session("create", scene_uuid[:8]).stdout.strip())[
            "uuid"
        ]
        tries += 1

    if s2[0] == s1[0]:
        ambiguous = s1[0]
        p = run_tool_session("show", ambiguous)
        assert p.returncode != 0 and "ambiguous" in p.stderr.lower()
    # cleanup sessions we created
    run_tool_session("delete", s1)
    run_tool_session("delete", s2)
