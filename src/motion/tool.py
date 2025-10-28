import asyncio
import contextlib
import importlib.resources
import itertools
import json
import logging
import time
import typing
import urllib

import typer

import motion

log = logging.getLogger(__name__)


def f_print(obj, *, output: str) -> None:
    def to_safe_one(o):
        if isinstance(o, motion.Scene):
            # downcast to bare pydantic model, then serialize
            return json.loads(
                motion.scene.SceneBase(uuid=o.uuid, runner=o.runner).json(
                    exclude_none=True
                )
            )

        if isinstance(o, motion.Session):
            # downcast to bare pydantic model, then serialize
            return json.loads(
                motion.session.SessionBase(
                    uuid=o.uuid,
                    scene=o.scene.uuid,
                    joint=o.joint,
                    camera=o.camera,
                    link=o.link,
                ).json(exclude_none=True)
            )

        if isinstance(o, dict):
            # enforce flat dict; show full offending dict on failure
            assert all(
                not isinstance(v, dict) for v in o.values()
            ), f"Nested dict not allowed: {o}"
            return o

        raise TypeError(f"Unsupported object type for f_print: {type(o)}")

    # compute payload; JSON will preserve shape, table will normalize later
    payload = (
        [to_safe_one(o) for o in obj]
        if isinstance(obj, (list, tuple))
        else to_safe_one(obj)
    )

    def render_table(data):
        # always render header; normalize to list internally
        rows = data if isinstance(data, list) else [data]
        if not rows:
            return ""
        keys = list(dict.fromkeys(k for r in rows for k in r.keys()))
        widths = {k: max(len(k), *(len(str(r.get(k, ""))) for r in rows)) for k in keys}
        header = "  ".join(k.ljust(widths[k]) for k in keys)
        sep = "  ".join("-" * widths[k] for k in keys)
        body = [
            "  ".join(str(r.get(k, "")).ljust(widths[k]) for k in keys) for r in rows
        ]
        return "\n".join([header, sep, *body])

    def render_json(data):
        # preserve shape: object stays object; list stays list
        return json.dumps(data, indent=2)

    if output == "json":
        print(render_json(payload))
    else:
        print(render_table(payload))


def f_prefix(items, q: str, *, kind: str):
    """
    Resolve prefix query against a list of typed client objects (Scenes/Sessions).

    Requires exactly one match; otherwise exits with a helpful error:
      - code 3: not found
      - code 4: ambiguous
    """
    matches = [i for i in items if str(i.uuid).startswith(q)]
    assert matches, f"{kind} not found for {q!r}"
    assert len(matches) == 1, f"ambiguous {kind} prefix {q!r}, {len(matches)} matches"
    return next(iter(matches))


async def f_xbox(data_callback, step_callback):

    period = 0.1  # 10 Hz
    joystick_index = 0

    e_axis = {
        0: "AXIS_LEFTX",
        1: "AXIS_LEFTY",
        2: "AXIS_RIGHTX",
        3: "AXIS_RIGHTY",
        4: "AXIS_TRIGGERLEFT",
        5: "AXIS_TRIGGERRIGHT",
    }
    e_button = {
        0: "BUTTON_A",
        1: "BUTTON_B",
        2: "BUTTON_X",
        3: "BUTTON_Y",
        4: "BUTTON_LEFTSHOULDER",
        5: "BUTTON_RIGHTSHOULDER",
        6: "BUTTON_BACK",
        7: "BUTTON_START",
        8: "BUTTON_GUIDE",
        9: "BUTTON_LEFTSTICK",
        10: "BUTTON_RIGHTSTICK",
    }
    import pygame

    pygame.init()
    pygame.joystick.init()

    saved = {}

    joystick = pygame.joystick.Joystick(joystick_index)
    joystick.init()

    def f_xbox_call():

        threshold = 0.01

        query = {
            "axis": [joystick.get_axis(i) for i in range(joystick.get_numaxes())],
            "button": [
                joystick.get_button(i) for i in range(joystick.get_numbuttons())
            ],
            "hat": [joystick.get_hat(i) for i in range(joystick.get_numhats())],
        }

        l_saved, l_query = (
            len(saved.get("axis", [])),
            len(query.get("axis", [])),
        )
        for i in range(max(l_saved, l_query)):
            if (i < min(l_saved, l_query)) and (
                abs(saved["axis"][i], query["axis"][i]) < threshold
            ):
                continue
            axis = query["axis"][i]
            log.info(f"Pygame: axes[{i}]: {axis}")
            entries.append((e_axis[i], max(-32767, min(32768, int(axis * 32768)))))

        l_saved, l_query = (
            len(saved.get("button", [])),
            len(query.get("button", [])),
        )
        for i in range(max(l_saved, l_query)):
            if (i < min(l_saved, l_query)) and (
                saved["button"][i] == query["button"][i]
            ):
                continue
            button = query["button"][i]
            log.info(f"Pygame: button[{i}]: {button}")
            entries.append((e_button[i], button))

        l_saved, l_query = (
            len(saved.get("hat", [])),
            len(query.get("hat", [])),
        )
        for i in range(max(l_saved, l_query)):
            if (i < min(l_saved, l_query)) and (saved["hat"][i] == query["hat"][i]):
                continue
            assert i == 0
            hx, hy = query["hat"][i]
            log.info(f"Pygame: hat[{i}]: {hx}, {hy}")
            entries.append(("BUTTON_DPAD_UP", int(hy == 1)))
            entries.append(("BUTTON_DPAD_DOWN", int(hy == -1)))
            entries.append(("BUTTON_DPAD_LEFT", int(hx == -1)))
            entries.append(("BUTTON_DPAD_RIGHT", int(hx == 1)))

        saved["axis"], saved["button"], saved["hat"] = (
            query["axis"],
            query["button"],
            query["hat"],
        )

    await data_callback(period)
    while not any(
        e.type == pygame.QUIT for e in pygame.event.get()
    ):  # pygame.event.pump() implicitly called with get()
        await step_callback(f_xbox_call())

        await data_callback(period)

    return


async def f_keyboard(data_callback, step_callback):
    import keyboard

    e_key = (
        "K",
        "W",
        "S",
        "A",
        "D",
        "Q",
        "E",
        "Z",
        "X",
        "T",
        "G",
        "C",
        "V",
    )

    period = 0.1  # 10 Hz

    state = {"run": True}

    def f_hook(e):
        state["key"] = e.name.upper()

    def f_stop():
        state["run"] = False

    keyboard.hook(f_hook)
    keyboard.add_hotkey("esc", f_stop)

    while state["run"]:
        await data_callback(period)
        while True:
            entry, state["key"] = state["key"], None
            if entry not in e_key:
                log.info(f"Event: {entry} skip")
            else:
                break

            await asyncio.sleep(period)

        log.info(f"Event: {entry} received")
        await step_callback([entry])


async def f_step(session, control, effector, gripper, data_callback):
    async with session.stream(start=None) as stream:

        async def step_callback(entries):
            step = {"gamepad": {effector: entries}}
            if gripper:
                step["metadata"] = json.dumps(
                    {"gripper": list(gripper)}, sort_keys=True
                )

            await stream.step(step)
            log.info(f"Step: {step}")

        if control == "xbox":
            await f_xbox(
                data_callback=data_callback,
                step_callback=step_callback,
            )
        elif control == "keyboard":
            await f_keyboard(
                data_callback=data_callback,
                step_callback=step_callback,
            )
        else:
            assert False, f"{control}"


async def f_proc(*argv: str) -> tuple[str, str]:
    """Run a process, log stdout/stderr line-by-line, return both as strings."""
    proc = await asyncio.create_subprocess_exec(
        *argv,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    assert proc.stdout is not None and proc.stderr is not None

    stdout_lines: list[str] = []
    stderr_lines: list[str] = []

    async def read_stream(stream, collector, label):
        async for line in stream:
            entry = line.decode(errors="replace").strip()
            log.info(f"[{label}] {entry}")
            collector.append(entry)

    # spawn reader tasks
    task_stdout = asyncio.create_task(read_stream(proc.stdout, stdout_lines, "stdout"))
    task_stderr = asyncio.create_task(read_stream(proc.stderr, stderr_lines, "stderr"))

    try:
        await proc.wait()
    except asyncio.CancelledError:
        # Cancelled by caller, kill the child so no transport lingers.
        with contextlib.suppress(ProcessLookupError):
            proc.terminate()
        with contextlib.suppress(asyncio.TimeoutError):
            # Give it a brief moment to exit; if it already exited this returns immediately.
            await asyncio.wait_for(proc.wait(), timeout=1.5)
        raise
    finally:
        task_stdout.cancel()
        task_stderr.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task_stdout
            await task_stderr

    return "\n".join(stdout_lines), "\n".join(stderr_lines)


async def f_compose() -> dict[str, str]:
    """
    Return {project: ip} for compose projects that have exactly one unique, non-empty IP.
    """
    stdout, _ = await f_proc("docker", "compose", "ls", "--format", "json")
    data = json.loads(stdout) if stdout.strip() else []
    if isinstance(data, dict):  # some engines return {"data":[...]}
        data = data.get("data", [])

    projects: list[str] = []
    for e in data or []:
        name = e.get("Name") or e.get("name") or e.get("Project") or e.get("project")
        if name:
            projects.append(name)

    collected: dict[str, str] = {}
    for project in projects:
        stdout, _ = await f_proc("docker", "compose", "-p", project, "ps", "-q")
        containers = [x.strip() for x in stdout.split("\n") if x.strip()]
        if not containers:
            continue

        ips: set[str] = set()
        for container in containers:
            stdout, _ = await f_proc(
                "docker",
                "inspect",
                "-f",
                "{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}",
                container,
            )
            ips |= {ip for ip in stdout.split() if ip}

        if len(ips) == 1:
            collected[project] = next(iter(ips))

    return collected


async def f_base_start(base, timeout) -> str:
    """Start docker compose project, resolve its single IP via f_compose(), then wait until healthy."""
    if base.startswith(("http://", "https://")):
        return base

    compose = str(importlib.resources.files("motion").joinpath("docker-compose.yml"))

    log.info("docker compose up -d...")
    await f_proc("docker", "compose", "-p", base, "-f", compose, "up", "-d")

    # stream logs in background (non-blocking)
    task = asyncio.create_task(
        f_proc(
            "docker",
            "compose",
            "-p",
            base,
            "-f",
            compose,
            "logs",
            "-f",
            "--no-color",
        )
    )

    deadline = time.time() + max(30.0, timeout * 6)

    while True:
        ip = (await f_compose()).get(base)

        if ip:
            stdout, _ = await f_proc(
                "docker", "compose", "-p", base, "-f", compose, "ps", "-q"
            )
            containers = [c.strip() for c in stdout.split("\n") if c.strip()]
            if containers:
                statuses: dict[str, str] = {}
                for container in containers:
                    stdout, _ = await f_proc(
                        "docker",
                        "inspect",
                        "-f",
                        "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}",
                        container,
                    )
                    statuses[container] = stdout.strip()

                log.info(f"health: {statuses}")

                if all(s in ("healthy", "running") for s in statuses.values()):
                    log.info(f"Compose project {base} ready at http://{ip}:8080")
                    return f"http://{ip}:8080"

        if time.time() > deadline:
            raise TimeoutError(f"containers not healthy (project={base}, ip={ip})")

        await asyncio.sleep(2)


async def f_base_close(base, timeout) -> None:
    """Stop docker compose project and log output."""
    if base.startswith(("http://", "https://")):
        return

    log.info("docker compose down...")
    await f_proc(
        "docker",
        "compose",
        "-p",
        base,
        "down",
        "-v",
        "--remove-orphans",
    )
    log.info(f"Compose project {base} stopped and removed.")


# -------------------
# Quick command (async)
# -------------------


async def f_quick(
    base,
    timeout,
    file,
    runner,
    joint,
    camera,
    link,
    device,
    model,
    tick,
    control,
    effector,
    gripper,
    archive,
):
    """
    One-shot flow:
      - docker compose up (if base is project name)
      - scene create
      - session create (context)
      - session play
      - session step (xbox)
      - session stop
      - session archive
      - session delete
      - scene delete
      - docker compose down (if started)
    """
    base = await f_base_start(base=base, timeout=timeout)
    client = motion.client(base=base, timeout=timeout)

    try:
        # 1) scene create
        scene = client.scene.create(file=file, runner=runner)

        # 2) session create (keep context while we operate)
        async with client.session.create(
            scene, joint=joint, camera=camera, link=link
        ) as session:
            try:
                # 3) play
                await session.play(device=device, model=model, tick=tick)

                # 4) drive (xbox loop)
                async with session.stream(start=1) as stream:
                    if tick:

                        async def f_data(period):
                            for i in itertools.count():
                                log.info(f"Data: wait {i}")
                                with contextlib.suppress(asyncio.TimeoutError):
                                    msg = await stream.data()
                                    log.info(f"Data: {msg}")
                                    return
                                await asyncio.sleep(0)

                        await f_step(
                            session=session,
                            control=control,
                            effector=effector,
                            gripper=gripper,
                            data_callback=f_data,
                        )

                    else:

                        async def f_data():
                            for i in itertools.count():
                                log.info(f"Data: wait {i}")
                                with contextlib.suppress(asyncio.TimeoutError):
                                    msg = await stream.data()
                                    log.info(f"Data: {msg}")
                                await asyncio.sleep(0)

                        await asyncio.gather(
                            f_data(),
                            f_step(
                                session=session,
                                control=control,
                                effector=effector,
                                gripper=gripper,
                                data_callback=asyncio.sleep,
                            ),
                        )

                # 5) stop
                await session.stop()

                # 6) archive
                client.archive(session=session, file=archive)

            finally:
                with contextlib.suppress(Exception):
                    client.session.delete(session)

        with contextlib.suppress(Exception):
            client.scene.delete(scene)

    finally:
        await f_base_close(base, timeout)


# =========================
# Typer CLI wiring (Python 3.9+)
# =========================

app = typer.Typer(help="Motion tooling")
scene_app = typer.Typer(help="Scene commands")
session_app = typer.Typer(help="Session commands")
server_app = typer.Typer(help="Server commands")
quick_app = typer.Typer(help="Quick", invoke_without_command=True)

app.add_typer(scene_app, name="scene")
app.add_typer(session_app, name="session")
app.add_typer(server_app, name="server")
app.add_typer(quick_app, name="quick")


@app.callback()
def app_options(
    context: typer.Context,
    base: str = typer.Option(
        "http://127.0.0.1:8080", "--base", help="Base URL or compose project name"
    ),
    output: str = typer.Option("json", "--output", help="Output format (json|table)"),
    timeout: float = typer.Option(10.0, "--timeout", help="Client timeout in seconds"),
    log_level: str = typer.Option(
        "error", "--log-level", help="Logging level (debug|info|warning|error|critical)"
    ),
):
    if context.invoked_subcommand != "server" and not base.startswith(
        ("http://", "https://")
    ):
        project = base or "motion"
        mapping = asyncio.run(f_compose())
        ip = mapping.get(project)
        assert ip, f"no compose project found or no single IP for {project!r}"
        base = f"http://{ip}:8080"

    context.obj = {
        "base": base,
        "output": output,
        "timeout": timeout,
        "log_level": log_level,
    }
    logging.basicConfig(level=getattr(logging, log_level.upper()), force=True)


@server_app.callback()
def server_app_options(context: typer.Context):
    base = context.obj["base"]
    if context.invoked_subcommand == "create":
        assert not base.startswith(
            ("http://", "https://")
        ), f"server create requires a compose project name for --base, not a URL: {base}"
    else:
        if base.startswith(("http://", "https://")):
            parsed = urllib.parse.urlparse(base)
            assert parsed.hostname, f"invalid base URL {base}"
            host = parsed.hostname

            mapping = asyncio.run(f_compose())
            matches = [p for p, ip in mapping.items() if ip == host]
            assert matches, f"no compose project found for host {host}"
            assert len(matches) == 1, f"ambiguous host {host}, matches: {matches}"
            context.obj["base"] = next(iter(matches))


# =========================
# Scene wrappers
# =========================
@scene_app.command("create", help="Create a Scene by uploading a USD file.")
def scene_create(
    context: typer.Context,
    file: str = typer.Option(..., "--file", help="Path to USD file"),
    runner: str = typer.Option("counter", "--runner", help="Runner type"),
):
    client = motion.client(base=context.obj["base"], timeout=context.obj["timeout"])
    scene = client.scene.create(file, runner)
    f_print(scene, output=context.obj["output"])


@scene_app.command("delete", help="Delete a Scene resolved from UUID prefix.")
def scene_delete(context: typer.Context, scene: str):
    client = motion.client(base=context.obj["base"], timeout=context.obj["timeout"])
    scene = f_prefix(client.scene.search(scene), scene, kind="scene")
    client.scene.delete(scene)
    f_print(scene, output=context.obj["output"])


@scene_app.command("list", help="List Scenes (optionally by prefix query).")
def scene_list(context: typer.Context, q: typing.Optional[str] = typer.Argument(None)):
    client = motion.client(base=context.obj["base"], timeout=context.obj["timeout"])
    scenes = client.scene.search(q or "")
    f_print(scenes, output=context.obj["output"])


@scene_app.command("show", help="Show one Scene by UUID prefix.")
def scene_show(context: typer.Context, scene: str):
    client = motion.client(base=context.obj["base"], timeout=context.obj["timeout"])
    scene = f_prefix(client.scene.search(scene), scene, kind="scene")
    f_print(scene, output=context.obj["output"])


@scene_app.command("archive", help="Download a Scene archive (zip) to PATH.")
def scene_archive(
    context: typer.Context,
    scene: str,
    path: str = typer.Option(..., "--path", help="Path to save scene archive zip"),
):
    client = motion.client(base=context.obj["base"], timeout=context.obj["timeout"])
    scene = f_prefix(client.scene.search(scene), scene, kind="scene")
    client.scene.archive(scene, path)
    f_print({"path": path}, output=context.obj["output"])


# =========================
# Session wrappers
# =========================


@session_app.command("create", help="Create a Session bound to a Scene (prefix).")
def session_create(
    context: typer.Context,
    scene: str,
    joint: typing.Optional[typing.List[str]] = typer.Option(
        None,
        "--joint",
        help="Repeatable: --joint j1 --joint j2",
    ),
    camera: typing.Optional[typing.List[str]] = typer.Option(
        None,
        "--camera",
        help="Repeatable: --camera name=widthxheight (e.g. front=640x480)",
    ),
    link: typing.Optional[typing.List[str]] = typer.Option(
        None,
        "--link",
        help="Repeatable: --link ee --link gripper",
    ),
):
    client = motion.client(base=context.obj["base"], timeout=context.obj["timeout"])
    scene = f_prefix(client.scene.search(scene), scene, kind="scene")

    # Parse cameras into {name: {"width": int, "height": int}}
    def f(entry):
        try:
            name, size = entry.split("=", 1)
            width, height = size.lower().split("x", 1)
            width, height = int(width), int(height)
        except Exception:
            raise typer.BadParameter(
                f"--camera must be in name=widthxheight format, got {entry!r}"
            )
        if width <= 0 or height <= 0:
            raise typer.BadParameter("--camera width/height must be > 0")
        return (name, {"width": width, "height": height})

    camera = dict(f(entry) for entry in camera) if camera else None

    session = client.session.create(scene, joint=joint, camera=camera, link=link)
    f_print(session, output=context.obj["output"])


@session_app.command("delete", help="Delete a Session resolved from UUID prefix.")
def session_delete(context: typer.Context, session: str):
    client = motion.client(base=context.obj["base"], timeout=context.obj["timeout"])
    session = f_prefix(client.session.search(session), session, kind="session")
    client.session.delete(session)
    f_print(session, output=context.obj["output"])


@session_app.command("list", help="List Sessions (optionally by prefix query).")
def session_list(
    context: typer.Context, q: typing.Optional[str] = typer.Argument(None)
):
    client = motion.client(base=context.obj["base"], timeout=context.obj["timeout"])
    sessions = client.session.search(q or "")
    f_print(sessions, output=context.obj["output"])


@session_app.command("show", help="Show one Session by UUID prefix.")
def session_show(context: typer.Context, session: str):
    client = motion.client(base=context.obj["base"], timeout=context.obj["timeout"])
    session = f_prefix(client.session.search(session), session, kind="session")
    f_print(session, output=context.obj["output"])


@session_app.command("archive", help="Download a Session archive (zip) to PATH.")
def session_archive(
    context: typer.Context,
    session: str,
    path: str = typer.Option(..., "--path", help="Path to save session archive zip"),
):
    client = motion.client(base=context.obj["base"], timeout=context.obj["timeout"])
    session = f_prefix(client.session.search(session), session, kind="session")
    client.session.archive(session, path)
    f_print({"path": path}, output=context.obj["output"])


@session_app.command("play", help="POST /session/{id}/play and print metadata.")
def session_play(
    context: typer.Context,
    session: str,
    device: typing.Optional[str] = typer.Option(None, "--device", help="cpu|cuda"),
    model: typing.Optional[str] = typer.Option(
        None, "--model", help="model|bounce|remote"
    ),
    tick: typing.Optional[bool] = typer.Option(None, "--tick/--no-tick"),
):
    client = motion.client(base=context.obj["base"], timeout=context.obj["timeout"])
    session = f_prefix(client.session.search(session), session, kind="session")

    async def f():
        async with session:
            await session.play(device=device, model=model, tick=tick)

    asyncio.run(f())
    f_print(session, output=context.obj["output"])


@session_app.command("stop", help="POST /session/{id}/stop.")
def session_stop(context: typer.Context, session: str):
    client = motion.client(base=context.obj["base"], timeout=context.obj["timeout"])
    session = f_prefix(client.session.search(session), session, kind="session")

    async def f():
        async with session:
            await session.stop()

    asyncio.run(f())
    f_print(session, output=context.obj["output"])


@session_app.command("step", help="Drive a session with step messages.")
def session_step(
    context: typer.Context,
    session: str,
    control: str = typer.Option("xbox", "--control", help="Control scheme"),
    effector: str = typer.Option(..., "--link", help="Link name of end effector"),
):
    client = motion.client(base=context.obj["base"], timeout=context.obj["timeout"])
    session = f_prefix(client.session.search(session), session, kind="session")

    async def f():
        async with session:
            await f_step(
                session=session,
                control=control,
                effector=effector,
                gripper=gripper,
                data_callback=asyncio.sleep,
            )

    asyncio.run(f())
    f_print(session, output=context.obj["output"])


# =========================
# Server wrappers
# =========================
@server_app.command(
    "create", help="Start docker compose project and print reachable URL."
)
def server_create(context: typer.Context):
    endpoint = asyncio.run(f_base_start(context.obj["base"], context.obj["timeout"]))
    f_print(
        {"endpoint": endpoint, "compose": context.obj["base"]},
        output=context.obj["output"],
    )


@server_app.command("delete", help="Stop docker compose project.")
def server_delete(context: typer.Context):
    asyncio.run(f_base_close(context.obj["base"], context.obj["timeout"]))
    f_print(
        {"status": "ok", "compose": context.obj["base"]}, output=context.obj["output"]
    )


# =========================
# Quick wrapper
# =========================
@quick_app.callback()
def quick_callback(
    context: typer.Context,
    file: str = typer.Option(..., "--file"),
    runner: str = typer.Option("counter", "--runner"),
    joint: typing.Optional[typing.List[str]] = typer.Option(
        None, "--joint", help="Repeatable: --joint j1 --joint j2"
    ),
    camera: typing.Optional[typing.List[str]] = typer.Option(
        None,
        "--camera",
        help="Repeatable: --camera name=widthxheight (e.g. front=640x480)",
    ),
    link: typing.Optional[typing.List[str]] = typer.Option(
        None, "--link", help="Repeatable: --link ee --link gripper"
    ),
    device: typing.Optional[str] = typer.Option(None, "--device", help="cpu|cuda"),
    model: typing.Optional[str] = typer.Option(
        None, "--model", help="model|bounce|remote"
    ),
    tick: typing.Optional[bool] = typer.Option(None, "--tick/--no-tick"),
    control: str = typer.Option("xbox", "--control"),
    effector: str = typer.Option(..., "--effector", help="Link name of end effector"),
    gripper: typing.Optional[typing.List[str]] = typer.Option(
        None, "--gripper", help="Repeatable: --gripper left --gripper right"
    ),
    archive: typing.Optional[str] = typer.Option(
        None, "--archive", help="Directory to store archives"
    ),
):
    """
    One-shot flow:
      - docker compose up (if base is project name)
      - scene create
      - session create (context)
      - session play
      - session step (xbox)
      - session stop
      - session archive
      - session delete
      - scene delete
      - docker compose down (if started)
    """
    base = context.obj["base"]
    if context.invoked_subcommand == "create":
        assert not base.startswith(
            ("http://", "https://")
        ), f"server create requires a compose project name for --base, not a URL: {base}"
    else:
        if base.startswith(("http://", "https://")):
            parsed = urllib.parse.urlparse(base)
            assert parsed.hostname, f"invalid base URL {base}"
            host = parsed.hostname

            mapping = asyncio.run(f_compose())
            matches = [p for p, ip in mapping.items() if ip == host]
            if matches:
                assert len(matches) == 1, f"ambiguous host {host}, matches: {matches}"
                context.obj["base"] = next(iter(matches))

    # Parse cameras into {name: {"width": int, "height": int}}
    def f(entry):
        try:
            name, size = entry.split("=", 1)
            width, height = size.lower().split("x", 1)
            width, height = int(width), int(height)
        except Exception:
            raise typer.BadParameter(
                f"--camera must be in name=widthxheight format, got {entry!r}"
            )
        if width <= 0 or height <= 0:
            raise typer.BadParameter("--camera width/height must be > 0")
        return (name, {"width": width, "height": height})

    camera = dict(f(entry) for entry in camera) if camera else None

    asyncio.run(
        f_quick(
            base=context.obj["base"],
            timeout=context.obj["timeout"],
            file=file,
            runner=runner,
            joint=joint,
            camera=camera,
            link=link,
            device=device,
            model=model,
            tick=tick,
            control=control,
            effector=effector,
            gripper=gripper,
            archive=archive,
        )
    )
    f_print({"status": "ok", "archive": archive}, output=context.obj["output"])


# =========================
# Entry
# =========================
if __name__ == "__main__":
    app()
