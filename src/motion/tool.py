import asyncio
import contextlib
import importlib.resources
import json
import logging
import math
import time
import types
import typing
import urllib.parse

import typer

import motion

log = logging.getLogger(__name__)


def f_print(obj, *, output: str) -> None:
    def to_safe_one(o):
        if isinstance(o, motion.Scene):
            # downcast to bare pydantic model, then serialize
            return json.loads(
                motion.scene.SceneBase(uuid=o.uuid, runner=o.runner).json()
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
                ).json()
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


async def f_step(session, control, data, link):
    """
    Drive a session with step messages using a control scheme.

    Currently supported:
      --control xbox

    Xbox joystick teleop (pygame), matching ROS2 teleop_twist_joy (Humble) for base mappings,
    with two extras:

      1) Momentary rotation mode (hold rotation_button) repurposes the left stick into roll/pitch.
      2) Exit button to cleanly stop the loop.

    --------------------------------------------------------------------------
    ROS2 OUTPUTS (naming and structure follow ROS2 messages):

      We publish two structures per step:

        - Twist (geometry_msgs/Twist semantics) under key "twist":
            twist["linear"]  = (x, y, z)   # m/s
            twist["angular"] = (x, y, z)   # rad/s

        - Pose (geometry_msgs/Pose semantics) under key "pose":
            pose["position"]    = (x, y, z)
            pose["orientation"] = (x, y, z, w)  # quaternion (ROS2 order = xyzw)

      NOTE ON INTERNAL STATE NAMING (this tool):
        To keep our internal integrator clear and consistent across the codebase, we use:

          translation_x, translation_y, translation_z    # meters
          rotation_r, rotation_p, rotation_y             # radians (roll, pitch, yaw)

        These map to ROS2 pose fields as:
          position = (translation_x, translation_y, translation_z)
          orientation (xyzw) = quaternion(rotation_r, rotation_p, rotation_y)

    --------------------------------------------------------------------------
    HUMBLE teleop_twist_joy CONFIG (as provided):
      axis_linear:        # Left thumb stick vertical
        x: 1
      scale_linear:
        x: 0.7
      scale_linear_turbo:
        x: 1.5

      axis_angular:       # Left thumb stick horizontal
        yaw: 0
      scale_angular:
        yaw: 0.4

      enable_button: 2            # Left trigger button (per your driver setup)
      enable_turbo_button: 5      # Right trigger button (per your driver setup)

    --------------------------------------------------------------------------
    BEHAVIOR:

      - Normal mode (no rotation button held):
          twist.linear.x  <- axis 1 (left stick vertical; LY)
          twist.angular.z <- axis 0 (left stick horizontal; LX)
          twist.linear.y = twist.linear.z = twist.angular.x = twist.angular.y = 0.0

          *Deadman (enable_button, default=2) must be held to output any motion.
          *Turbo (turbo_button, default=5) multiplies twist.linear.x scale by max_linear_turbo.

      - Extra: Momentary rotation mode (hold rotation_button, default=9 = Left Stick click):
          twist.angular.x <- axis 1 (LY)  # roll rate
          twist.angular.y <- axis 0 (LX)  # pitch rate
          Suppresses translation and yaw while held:
            twist.linear.{x,y,z} = 0.0; twist.angular.z = 0.0

      - Extra: Exit button (default=1 = B button):
          Pressing it quits the async loop and calls pygame.quit().

    --------------------------------------------------------------------------
    DEFAULT BUTTON/AXIS MAP (typical Xbox controller via SDL2/Pygame; may vary):
      Axes:
        0: Left stick horizontal (LX)
        1: Left stick vertical   (LY)
        2: Right stick horizontal (unused here)
        3: Right stick vertical   (unused here)
      Buttons:
        0: A
        1: B                       <-- Exit button (default extra)
        2: X                       <-- Enable (deadman, per Humble config)
        3: Y
        4: LB
        5: RB                      <-- Turbo (per Humble config)
        6: Back
        7: Start
        8: Guide/Xbox
        9: Left Stick click (LS)   <-- Rotation button (default extra)
       10: Right Stick click (RS)

    --------------------------------------------------------------------------
    NOTES:
      - No sign inversion is applied to match the config literally.
        If your stick reports "up" as -1 and you want "up = +forward (+x)",
        invert LY when computing linear.x.

      - This tool integrates a synthetic pose for convenience; teleop_twist_joy
        normally publishes Twist only (no pose integration).

      - ISAAC SIM difference: Isaac returns quaternion as (w, x, y, z) [scalar-first].
        Here we output ROS2 order (x, y, z, w).

    CALLBACKS / TRANSPORT:
      - Steps are sent as: {"pose": {...}, "twist": {...}} via session.stream().step()
      - We purposely keep the control/physics integrator stateless except for the
        minimal "state" dict, so this remains easy to test and to port.
    """
    assert control == "xbox", f"unsupported control {control}; only 'xbox' is supported"

    # If pygame is not installed, let the ImportError speak for itself (per your request).
    import pygame

    # Hard-coded control parameters for now (we can expose them later if actually needed)
    rate_hz = 20.0
    joystick_index = 0

    # Resolve session by prefix and open the step/data stream
    async with session:
        async with session.stream(start=None) as stream:

            # -------------------
            # Math helpers (stateless)
            # -------------------
            def euler_to_quaternion_xyzw(roll: float, pitch: float, yaw: float):
                """Convert Euler RPY (rad) to ROS2 quaternion (x, y, z, w)."""
                cy, sy = math.cos(yaw * 0.5), math.sin(yaw * 0.5)
                cp, sp = math.cos(pitch * 0.5), math.sin(pitch * 0.5)
                cr, sr = math.cos(roll * 0.5), math.sin(roll * 0.5)
                w = cr * cp * cy + sr * sp * sy
                x = sr * cp * cy - cr * sp * sy
                y = cr * sp * cy + sr * cp * sy
                z = cr * cp * sy - sr * sp * cy
                return {"x": x, "y": y, "z": z, "w": w}

            def joystick_axis_safe(joystick, i: int) -> float:
                """Return axis i if present; otherwise 0.0 (defensive)."""
                return joystick.get_axis(i) if i < joystick.get_numaxes() else 0.0

            def joystick_button(joystick, i: int) -> bool:
                """Return True if button i is present and pressed."""
                return (i < joystick.get_numbuttons()) and (joystick.get_button(i) == 1)

            def read_axes(joystick, deadzone: float):
                """
                Read first four axes: a0=LX, a1=LY, a2=RX, a3=RY.
                We apply a simple deadzone clamp to avoid noise around 0.
                """
                pygame.event.pump()

                def dz(v):  # inline small helper (no extra nesting)
                    return v if abs(v) >= deadzone else 0.0

                return (
                    dz(joystick_axis_safe(joystick, 0)),
                    dz(joystick_axis_safe(joystick, 1)),
                    dz(joystick_axis_safe(joystick, 2)),
                    dz(joystick_axis_safe(joystick, 3)),
                )

            def compute_twist(
                a0,
                a1,
                a2,
                a3,
                *,
                deadman,
                turbo,
                rotation_hold,
                max_linear,
                max_linear_turbo,
                max_angular,
            ):
                """
                Compute geometry_msgs/Twist from joystick input.

                Returns a dict shaped exactly like geometry_msgs/Twist:
                  {
                    "linear":  (x, y, z),  # m/s
                    "angular": (x, y, z),  # rad/s
                  }

                Mapping matches teleop_twist_joy config:
                  - Normal:
                      linear.x <- LY (a1) * max_linear[ * turbo]
                      angular.z <- LX (a0) * max_angular
                  - Rotation hold:
                      angular.x <- LY (a1) * max_angular   (roll)
                      angular.y <- LX (a0) * max_angular   (pitch)
                      (Suppress linear.{x,y,z} and angular.z)
                """
                linear_scale = max_linear * (max_linear_turbo if turbo else 1.0)
                angular_scale = max_angular

                lin_x = lin_y = lin_z = ang_x = ang_y = ang_z = 0.0
                if deadman:
                    if rotation_hold:
                        ang_x = a1 * angular_scale  # roll
                        ang_y = a0 * angular_scale  # pitch
                    else:
                        lin_x = a1 * linear_scale
                        ang_z = a0 * angular_scale
                return {
                    "linear": (lin_x, lin_y, lin_z),
                    "angular": (ang_x, ang_y, ang_z),
                }

            def integrate_pose(state, twist, dt: float):
                """
                Integrate a synthetic pose from Twist (ROS2-style).

                This tool keeps a minimal "state" dict using *explicit* names:

                    translation_x, translation_y, translation_z  (meters)
                    rotation_r, rotation_p, rotation_y           (radians; roll/pitch/yaw)

                The integrator is trivially simple (Euler); it is *not* intended for
                physically correct simulation, only for producing plausible numbers
                while teleoperating demos or remote nodes.
                """
                lin_x, lin_y, lin_z = twist["linear"]
                ang_x, ang_y, ang_z = twist["angular"]

                state["translation_x"] += lin_x * dt
                state["translation_y"] += lin_y * dt
                state["translation_z"] += lin_z * dt
                state["rotation_r"] += ang_x * dt
                state["rotation_p"] += ang_y * dt
                state["rotation_y"] += ang_z * dt

                # Wrap angles to [-pi, pi] to keep quaternions well-behaved.
                for k in ("rotation_r", "rotation_p", "rotation_y"):
                    v = state[k]
                    state[k] = (v + math.pi) % (2 * math.pi) - math.pi

            def pose_from_state(state):
                """
                Return ROS2-style Pose dict using the internal state:

                  position = (translation_x, translation_y, translation_z)
                  orientation = quaternion(rotation_r, rotation_p, rotation_y)  # xyzw
                """
                position = {
                    "x": state["translation_x"],
                    "y": state["translation_y"],
                    "z": state["translation_z"],
                }
                orientation = euler_to_quaternion_xyzw(
                    state["rotation_r"], state["rotation_p"], state["rotation_y"]
                )
                return {"position": position, "orientation": orientation}

            # ------------------------------------------------------------------
            # Initialize pygame + joystick (let ImportError/RuntimeError surface)
            # ------------------------------------------------------------------
            pygame.init()
            pygame.joystick.init()
            if pygame.joystick.get_count() == 0:
                pygame.quit()
                assert (
                    False
                ), "No joystick found (xbox control requires a connected controller)"

            joystick = pygame.joystick.Joystick(joystick_index)
            joystick.init()
            log.info(
                f"Joystick initialized: {joystick.get_name()} "
                f"axes={joystick.get_numaxes()} buttons={joystick.get_numbuttons()}"
            )

            # Control parameters (align with teleop_twist_joy expectations)
            max_linear = 0.7
            max_linear_turbo = 1.5
            max_angular = 0.4
            deadzone = 0.08

            # Button mapping (typical Xbox via SDL2/Pygame)
            enable_button = 2  # X (deadman)
            turbo_button = 5  # RB
            rotation_button = 9  # Left stick click (momentary rotation mode)
            exit_button = 1  # B

            # Minimal state for pose integration + timing (explicit names)
            state = dict(
                translation_x=0.0,
                translation_y=0.0,
                translation_z=0.0,
                rotation_r=0.0,
                rotation_p=0.0,
                rotation_y=0.0,
                last=time.time(),
            )

            period = 1.0 / rate_hz

            log.info(
                "Running (async): "
                "hold ENABLE (X), hold ROTATION (LS click) for roll/pitch, "
                "TURBO (RB) scales linear.x, EXIT (B) quits."
            )

            while True:
                now = time.time()
                dt = max(0.001, min(now - state["last"], 0.5))
                state["last"] = now

                a0, a1, a2, a3 = read_axes(joystick, deadzone)
                deadman = joystick_button(joystick, enable_button)
                turbo = joystick_button(joystick, turbo_button)
                rotation_hold = joystick_button(joystick, rotation_button)

                twist = compute_twist(
                    a0,
                    a1,
                    a2,
                    a3,
                    deadman=deadman,
                    turbo=turbo,
                    rotation_hold=rotation_hold,
                    max_linear=max_linear,
                    max_linear_turbo=max_linear_turbo,
                    max_angular=max_angular,
                )
                integrate_pose(state, twist, dt)
                pose = pose_from_state(state)

                # Send one step message over the session stream
                if data == "pose":
                    step = {"pose": {link: pose}}
                elif data == "twist":
                    step = {"twist": {link: twist}}
                else:
                    assert False, f"unsupported data type {data}"

                await stream.step(step)

                if joystick_button(joystick, exit_button):
                    log.info("Exit button pressed. Quitting...")
                    break

                await asyncio.sleep(period)

            # Clean up only after normal exit
            joystick.quit()
            pygame.quit()


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


async def f_quick(base, timeout, file, runner, model):
    """
    One-shot flow:
      - docker compose up (if base is project name)
      - scene create
      - session create (context)
      - session play
      - session step (xbox)
      - session stop
      - (optional) archive both scene and session if --archive is provided
      - session delete
      - scene delete
      - docker compose down (if started)
    """
    base = await f_base_start(base, timeout)
    client = motion.client(base=base, timeout=timeout)

    try:
        # 1) scene create
        scene = client.scene.create(file, runner)

        # 2) session create (keep context while we operate)
        async with client.session.create(scene) as session:
            try:
                # 3) play
                await session.play(model=model)

                # 4) drive (xbox loop)
                await f_step(session, control, link, step)

                # 5) stop
                await session.stop()

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

app.add_typer(scene_app, name="scene")
app.add_typer(session_app, name="session")
app.add_typer(server_app, name="server")


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
    runner: str = typer.Option("count", "--runner", help="Runner type"),
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
def session_create(context: typer.Context, scene: str):
    client = motion.client(base=context.obj["base"], timeout=context.obj["timeout"])
    scene = f_prefix(client.scene.search(scene), scene, kind="scene")
    session = client.session.create(scene)
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
    model: typing.Optional[str] = typer.Option(None, "--model"),
):
    client = motion.client(base=context.obj["base"], timeout=context.obj["timeout"])
    session = f_prefix(client.session.search(session), session, kind="session")

    async def f():
        async with session:
            await session.play(model=model)

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
    link: str = typer.Option(..., "--link", help="Link name of end effector"),
    data: str = typer.Option(..., "--data", help="Data format: pose|twist"),
):
    client = motion.client(base=context.obj["base"], timeout=context.obj["timeout"])
    session = f_prefix(client.session.search(session), session, kind="session")
    asyncio.run(f_step(session, control, data, link))
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
@app.command(
    "quick",
    help="One-shot flow: up -> create -> play -> step -> stop -> archive -> down.",
)
def quick(
    context: typer.Context,
    file: str = typer.Option(..., "--file"),
    runner: str = typer.Option("count", "--runner"),
    model: typing.Optional[str] = typer.Option(None, "--model"),
    control: str = typer.Option("xbox", "--control"),
    archive: typing.Optional[str] = typer.Option(
        None, "--archive", help="Directory to store archives"
    ),
):
    args = types.SimpleNamespace(
        file=file,
        runner=runner,
        model=model,
        control=control,
        archive=archive,
        base=context.obj["base"],
        timeout=context.obj["timeout"],
        output=context.obj["output"],
        log_level=context.obj["log_level"],
    )
    asyncio.run(quick_run(None, args))


# =========================
# Entry
# =========================
if __name__ == "__main__":
    app()
