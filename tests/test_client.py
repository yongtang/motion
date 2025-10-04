import json
import pathlib
import tempfile
import uuid
import zipfile

import pytest

import motion


def test_client_scene(docker_compose):
    base = f"http://{docker_compose['motion']}:8080"
    client = motion.client(base=base, timeout=5.0)

    # create from USD + runner (client zips internally and uploads)
    with tempfile.TemporaryDirectory() as tdir:
        tdir = pathlib.Path(tdir)
        usd_path = tdir / "scene.usd"
        usd_path.write_text("#usda 1.0\ndef X {\n}\n", encoding="utf-8")

        runner = "counter"
        scene = client.scene.create(usd_path, runner)

    # got a typed Scene back
    assert scene
    assert scene.runner == runner

    # search returns the same typed Scene (equality by UUID)
    assert client.scene.search(str(scene.uuid)) == [scene]

    # archive via client API and confirm file exists and is non-empty
    with tempfile.TemporaryDirectory() as tdir:
        out = pathlib.Path(tdir) / f"{scene.uuid}.zip"
        client.scene.archive(scene, out)
        assert out.exists() and out.stat().st_size > 0

    # search for a random uuid should be empty
    bogus = str(uuid.uuid4())
    assert client.scene.search(bogus) == []

    # delete via client API
    assert client.scene.delete(scene) is None

    # after delete, search should be empty
    assert client.scene.search(str(scene.uuid)) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("model", ["bounce", "remote"])
async def test_client_session(scene_on_server, model):
    base, scene = scene_on_server
    client = motion.client(base=base, timeout=5.0)

    async with client.session.create(scene=scene) as session:
        # sanity on the typed object we got back
        assert isinstance(session, motion.Session)
        assert session.scene.uuid == scene.uuid
        assert session.joint == ["*"]
        assert isinstance(session.camera, dict)
        assert session.link == ["*"]

        # drive lifecycle via the async API
        await session.play(model=model)  # requires play(model: str | None = None)
        await session.wait("play", timeout=300.0)

        await session.stop()
        await session.wait("stop", timeout=60.0)

        # archive and validate JSONL
        with tempfile.TemporaryDirectory() as tdir:
            out = pathlib.Path(tdir) / f"{session.uuid}.zip"
            client.session.archive(session, out)  # sync call is fine inside async block
            assert out.exists() and out.stat().st_size > 0

            with zipfile.ZipFile(out, "r") as z:
                names = set(z.namelist())
                assert "session.json" in names, "archive missing session.json"
                assert "data.json" in names, "archive missing data.json"
                content = z.read("data.json").decode("utf-8", errors="ignore")
                lines = [ln for ln in content.splitlines() if ln.strip()]
                assert lines, "data.json empty"
                for ln in lines:
                    json.loads(ln)  # each line parses

        # delete and verify search is empty after
        assert client.session.delete(session) is None
        assert client.session.search(str(session.uuid)) == []


def test_client_scene_search(docker_compose):
    base = f"http://{docker_compose['motion']}:8080"
    client = motion.client(base=base, timeout=5.0)

    # Create TWO additional scenes so we can test prefix + all
    with tempfile.TemporaryDirectory() as tdir:
        tdir = pathlib.Path(tdir)
        usd_path = tdir / "scene.usd"
        usd_path.write_text("#usda 1.0\ndef X {\n}\n", encoding="utf-8")

        s1 = client.scene.create(usd_path, runner="counter")
        s2 = client.scene.create(usd_path, runner="counter")

    try:
        # Exact search returns the same typed Scene
        assert client.scene.search(str(s1.uuid)) == [s1]

        # Prefix search: use a short canonical prefix (with hyphens preserved)
        prefix = str(s1.uuid)[:8]
        prefix_results = client.scene.search(prefix)
        # We don't assert exclusivity (prefix can collide), just membership
        assert s1 in prefix_results

        # Empty search -> all: should include both newly-created scenes
        all_scenes = client.scene.search()  # q=None
        assert s1 in all_scenes
        assert s2 in all_scenes

        # Bogus (random) UUID should return empty
        bogus = str(uuid.uuid4())
        assert client.scene.search(bogus) == []
    finally:
        # Cleanup both scenes
        client.scene.delete(s1)
        client.scene.delete(s2)


def test_client_session_search(scene_on_server):
    base, scene = scene_on_server
    client = motion.client(base=base, timeout=5.0)

    # Create TWO sessions bound to the existing scene
    sess1 = client.session.create(scene=scene)
    sess2 = client.session.create(scene=scene)

    try:
        # Exact search returns the same typed Session
        assert client.session.search(str(sess1.uuid)) == [sess1]

        # Prefix search: short canonical prefix
        prefix = str(sess1.uuid)[:8]
        prefix_results = client.session.search(prefix)
        assert sess1 in prefix_results

        # Empty search -> all sessions: should contain both
        all_sessions = client.session.search()
        assert sess1 in all_sessions
        assert sess2 in all_sessions

        # Bogus UUID -> empty
        bogus = str(uuid.uuid4())
        assert client.session.search(bogus) == []
    finally:
        # Cleanup
        client.session.delete(sess1)
        client.session.delete(sess2)
