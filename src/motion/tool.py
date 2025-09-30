import argparse
import asyncio
import contextlib
import importlib.resources
import json
import logging
import math
import pathlib
import sys
import time

import motion

log = logging.getLogger(__name__)


def f_fail(code: int, msg: str) -> None:
    """Print an error and exit with given code."""
    print(msg, file=sys.stderr)
    sys.exit(code)


def f_print(obj, *, json_mode: bool, pretty_mode: bool, quiet_mode: bool) -> None:
    """
    Print object(s) depending on flags.

    IMPORTANT: Avoid calling .json() on live motion.Session objects because they may
    hold a non-serializable httpx.AsyncClient on a private attr. Convert to plain dicts.

    - motion.Session -> minimal safe dict {uuid, scene, joint, camera, link}
    - motion.Scene   -> {uuid, runner:{image,device}}
    - lists/tuples   -> map element-wise through the same conversion
    - other pydantic models -> attempt o.json() then json.loads as a fallback
    """
    if quiet_mode:
        return

    def to_safe(o):
        if isinstance(o, motion.Session):
            return {
                "uuid": str(o.uuid),
                "scene": str(o.scene.uuid),
                "joint": o.joint,
                "camera": o.camera,
                "link": o.link,
            }
        if isinstance(o, motion.Scene):
            return {
                "uuid": str(o.uuid),
                "runner": {
                    "image": o.runner.image.value,
                    "device": o.runner.device.value,
                },
            }
        if hasattr(o, "json") and callable(o.json):
            try:
                return json.loads(o.json())
            except Exception:
                pass
        return o

    if not json_mode:
        print(obj)
        return

    indent = 2 if pretty_mode else None

    if isinstance(obj, (list, tuple)):
        payload = [to_safe(o) for o in obj]
        print(json.dumps(payload, indent=indent))
        return

    print(json.dumps(to_safe(obj), indent=indent))


def f_prefix(items, q: str, *, kind: str):
    """
    Resolve prefix query against a list of typed client objects (Scenes/Sessions).

    Requires exactly one match; otherwise exits with a helpful error:
      - code 3: not found
      - code 4: ambiguous
    """
    matches = [i for i in items if str(i.uuid).startswith(q)]
    if not matches:
        f_fail(3, f"{kind} not found for {q!r}")
    if len(matches) > 1:
        f_fail(4, f"ambiguous {kind} prefix {q!r}, {len(matches)} matches")
    return matches[0]


# -------------------
# Scene commands (sync)
# -------------------


def scene_create(client, args):
    """Create a Scene by uploading a USD file. The client zips USD+empty meta.json internally."""
    file = pathlib.Path(args.file)
    scene = client.scene.create(file, args.image, args.device)
    f_print(scene, json_mode=args.json, pretty_mode=args.pretty, quiet_mode=args.quiet)


def scene_delete(client, args):
    """Delete a Scene resolved from a UUID prefix."""
    scenes = client.scene.search(args.scene)
    scene = f_prefix(scenes, args.scene, kind="scene")
    client.scene.delete(scene)
    f_print(scene, json_mode=args.json, pretty_mode=args.pretty, quiet_mode=args.quiet)


def scene_list(client, args):
    """
    List Scenes. If a prefix query (q) is provided, server-side returns all matches
    (the server accepts empty q to list-all; client simply forwards).
    """
    scenes = client.scene.search(args.q or "")
    f_print(scenes, json_mode=args.json, pretty_mode=args.pretty, quiet_mode=args.quiet)


def scene_show(client, args):
    """Show one Scene by UUID prefix (must be unique)."""
    scenes = client.scene.search(args.scene)
    scene = f_prefix(scenes, args.scene, kind="scene")
    f_print(scene, json_mode=args.json, pretty_mode=args.pretty, quiet_mode=args.quiet)


def scene_archive(client, args):
    """Download a Scene archive (zip containing scene.usd + meta.json) to the given output path."""
    scenes = client.scene.search(args.scene)
    scene = f_prefix(scenes, args.scene, kind="scene")
    out = pathlib.Path(args.output)
    client.scene.archive(scene, out)
    f_print(
        {"output": str(out)},
        json_mode=args.json,
        pretty_mode=args.pretty,
        quiet_mode=args.quiet,
    )


# -------------------
# Session commands (async)
# -------------------


async def session_create(client, args):
    """
    Create a Session bound to a Scene (resolved from prefix).
    Prints the typed Session object (safe JSON) while inside the async context.
    """
    scenes = client.scene.search(args.scene)
    scene = f_prefix(scenes, args.scene, kind="scene")
    async with client.session.create(scene) as session:
        f_print(
            session, json_mode=args.json, pretty_mode=args.pretty, quiet_mode=args.quiet
        )


async def session_delete(client, args):
    """Delete a Session resolved from a UUID prefix."""
    sessions = client.session.search(args.session)
    session = f_prefix(sessions, args.session, kind="session")
    client.session.delete(session)
    f_print(
        session, json_mode=args.json, pretty_mode=args.pretty, quiet_mode=args.quiet
    )


async def session_list(client, args):
    """
    List Sessions. If a prefix query (q) is provided, server returns all matches.
    Empty q lists all sessions.
    """
    sessions = client.session.search(args.q or "")
    f_print(
        sessions, json_mode=args.json, pretty_mode=args.pretty, quiet_mode=args.quiet
    )


async def session_show(client, args):
    """Show one Session by UUID prefix (must be unique)."""
    sessions = client.session.search(args.session)
    session = f_prefix(sessions, args.session, kind="session")
    f_print(
        session, json_mode=args.json, pretty_mode=args.pretty, quiet_mode=args.quiet
    )


async def session_archive(client, args):
    """Download a Session archive (zip containing session.json + data.json) to the given output path."""
    sessions = client.session.search(args.session)
    session = f_prefix(sessions, args.session, kind="session")
    out = pathlib.Path(args.output)
    client.session.archive(session, out)
    f_print(
        {"output": str(out)},
        json_mode=args.json,
        pretty_mode=args.pretty,
        quiet_mode=args.quiet,
    )


async def session_play(client, args):
    """
    POST /session/{id}/play using the typed async API.
    Resolved by UUID prefix; prints the session metadata after invoking play().
    """
    sessions = client.session.search(args.session)
    async with f_prefix(sessions, args.session, kind="session") as session:
        await session.play(model=args.model)
        f_print(
            session, json_mode=args.json, pretty_mode=args.pretty, quiet_mode=args.quiet
        )


async def session_stop(client, args):
    """
    POST /session/{id}/stop using the typed async API.
    Resolved by UUID prefix; prints the session metadata after invoking stop().
    """
    sessions = client.session.search(args.session)
    async with f_prefix(sessions, args.session, kind="session") as session:
        await session.stop()
        f_print(
            session, json_mode=args.json, pretty_mode=args.pretty, quiet_mode=args.quiet
        )


async def session_step(client, args):
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
    if args.control != "xbox":
        f_fail(5, f"unsupported control {args.control!r}; only 'xbox' is supported")

    # If pygame is not installed, let the ImportError speak for itself (per your request).
    import pygame

    # Hard-coded control parameters for now (we can expose them later if actually needed)
    rate_hz = 20.0
    joystick_index = 0

    # Resolve session by prefix and open the step/data stream
    sessions = client.session.search(args.session)
    async with f_prefix(sessions, args.session, kind="session") as session:
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
                return (x, y, z, w)

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
                q = euler_to_quaternion_xyzw(
                    state["rotation_r"], state["rotation_p"], state["rotation_y"]
                )
                return {
                    "position": (
                        state["translation_x"],
                        state["translation_y"],
                        state["translation_z"],
                    ),
                    "orientation": q,
                }

            # ------------------------------------------------------------------
            # Initialize pygame + joystick (let ImportError/RuntimeError surface)
            # ------------------------------------------------------------------
            pygame.init()
            pygame.joystick.init()
            if pygame.joystick.get_count() == 0:
                pygame.quit()
                f_fail(
                    7,
                    "No joystick found (xbox control requires a connected controller)",
                )

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
                await stream.step({"pose": pose, "twist": twist})

                if joystick_button(joystick, exit_button):
                    log.info("Exit button pressed. Quitting...")
                    break

                await asyncio.sleep(period)

            # Clean up only after normal exit
            joystick.quit()
            pygame.quit()

        # We print something on success so CLI users get a confirmation even in quiet setups.
        f_print(
            {"status": "ok"},
            json_mode=args.json,
            pretty_mode=args.pretty,
            quiet_mode=args.quiet,
        )


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

    await asyncio.gather(
        read_stream(proc.stdout, stdout_lines, "stdout"),
        read_stream(proc.stderr, stderr_lines, "stderr"),
    )
    await proc.wait()

    return "\n".join(stdout_lines), "\n".join(stderr_lines)


async def f_base_start(args) -> str:
    """Start docker compose project, wait until healthy, and return base URL."""
    if str(args.base).startswith(("http://", "https://")):
        return args.base

    project = args.base or "motion"
    compose = str(importlib.resources.files("motion").joinpath("docker-compose.yml"))

    log.info("docker compose up -d …")
    await f_proc("docker", "compose", "-p", project, "-f", compose, "up", "-d")

    # stream logs in background
    asyncio.create_task(
        f_proc(
            "docker",
            "compose",
            "-p",
            project,
            "-f",
            compose,
            "logs",
            "-f",
            "--no-color",
        )
    )

    deadline = time.time() + max(30.0, args.timeout * 6)
    ip: str | None = None
    while True:
        # discover containers
        stdout, stderr = await f_proc(
            "docker", "compose", "-p", project, "-f", compose, "ps", "-q"
        )
        containers = [c.strip() for c in stdout.split("\n") if c.strip()]
        if not containers:
            if time.time() > deadline:
                raise TimeoutError("docker compose ps returned no containers")
            await asyncio.sleep(1)
            continue

        statuses: dict[str, str] = {}
        ips: set[str] = set()
        for container in containers:
            stdout, stderr = await f_proc(
                "docker",
                "inspect",
                "-f",
                "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}",
                container,
            )
            statuses[container] = stdout
            stdout, stderr = await f_proc(
                "docker",
                "inspect",
                "-f",
                "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                container,
            )
            ips.add(stdout.strip())
        ips = {ip for ip in ips if ip}

        log.info(f"health: {statuses}")

        if (
            all(s in ("healthy", "running") for s in statuses.values())
            and len(ips) == 1
        ):
            ip = next(iter(ips))
            break

        if time.time() > deadline:
            raise TimeoutError(f"containers not healthy: {statuses} ips={ips}")

        await asyncio.sleep(2)

    log.info(f"Compose project {project} ready at http://{ip}:8080")
    return f"http://{ip}:8080"


async def f_base_close(args) -> None:
    """Stop docker compose project and log output."""
    if str(args.base).startswith(("http://", "https://")):
        return

    project = args.base or "motion"

    log.info("docker compose down…")
    await f_proc(
        "docker",
        "compose",
        "-p",
        project,
        "-f",
        str(importlib.resources.files("motion").joinpath("docker-compose.yml")),
        "down",
        "-v",
        "--remove-orphans",
    )
    log.info(f"Compose project {project} stopped and removed.")


# -------------------
# Quick command (async)
# -------------------


async def quick_run(client, args):
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
    base = await f_base_start(args)
    client = motion.client(base=base, timeout=args.timeout)

    try:
        # 1) scene create
        file = pathlib.Path(args.file)
        scene = client.scene.create(file, args.image, args.device)
        f_print(
            scene, json_mode=args.json, pretty_mode=args.pretty, quiet_mode=args.quiet
        )
        args.scene = str(scene.uuid)

        # 2) session create (keep context while we operate)
        async with client.session.create(scene) as session:
            f_print(
                session,
                json_mode=args.json,
                pretty_mode=args.pretty,
                quiet_mode=args.quiet,
            )
            args.session = str(session.uuid)

            try:
                # 3) play
                await session_play(client, args)

                # 4) drive (xbox loop)
                await session_step(client, args)

                # 5) stop
                await session_stop(client, args)

                # 6) optional archives
                if args.archive:
                    outdir = pathlib.Path(args.archive)
                    outdir.mkdir(parents=True, exist_ok=True)
                    scene_zip = outdir / f"scene-{scene.uuid}.zip"
                    session_zip = outdir / f"session-{session.uuid}.zip"
                    client.scene.archive(scene, scene_zip)
                    client.session.archive(session, session_zip)
                    f_print(
                        {
                            "archive": {
                                "scene": str(scene_zip),
                                "session": str(session_zip),
                            }
                        },
                        json_mode=args.json,
                        pretty_mode=args.pretty,
                        quiet_mode=args.quiet,
                    )

            finally:
                with contextlib.suppress(Exception):
                    await session_delete(client, args)

        with contextlib.suppress(Exception):
            scene_delete(client, args)

    finally:
        await f_base_close(args)


# -------------------
# Parser
# -------------------


def f_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="http://127.0.0.1:8080")
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument(
        "--log-level",
        type=str.lower,
        choices=["debug", "info", "warning", "error", "critical"],
        default="error",
    )

    flag_parser = argparse.ArgumentParser(add_help=False)
    flag_parser.add_argument("--json", dest="json", action="store_true")
    flag_parser.add_argument("--pretty", dest="pretty", action="store_true")
    flag_parser.add_argument("--quiet", dest="quiet", action="store_true")

    mode_parser = parser.add_subparsers(dest="mode", required=True)

    # Scene
    scene_parser = mode_parser.add_parser("scene")
    scene_command = scene_parser.add_subparsers(dest="command", required=True)

    scene_create_parser = scene_command.add_parser("create", parents=[flag_parser])
    scene_create_parser.add_argument("--file", required=True)
    scene_create_parser.add_argument("--image", default="count")
    scene_create_parser.add_argument("--device", default="cpu")

    scene_delete_parser = scene_command.add_parser("delete", parents=[flag_parser])
    scene_delete_parser.add_argument("scene")

    scene_list_parser = scene_command.add_parser("list", parents=[flag_parser])
    scene_list_parser.add_argument("q", nargs="?")

    scene_show_parser = scene_command.add_parser("show", parents=[flag_parser])
    scene_show_parser.add_argument("scene")

    scene_archive_parser = scene_command.add_parser("archive", parents=[flag_parser])
    scene_archive_parser.add_argument("scene")
    scene_archive_parser.add_argument("output")

    # Session
    session_parser = mode_parser.add_parser("session")
    session_command = session_parser.add_subparsers(dest="command", required=True)

    session_create_parser = session_command.add_parser("create", parents=[flag_parser])
    session_create_parser.add_argument("scene")

    session_delete_parser = session_command.add_parser("delete", parents=[flag_parser])
    session_delete_parser.add_argument("session")

    session_list_parser = session_command.add_parser("list", parents=[flag_parser])
    session_list_parser.add_argument("q", nargs="?")

    session_show_parser = session_command.add_parser("show", parents=[flag_parser])
    session_show_parser.add_argument("session")

    session_archive_parser = session_command.add_parser(
        "archive", parents=[flag_parser]
    )
    session_archive_parser.add_argument("session")
    session_archive_parser.add_argument("output")

    session_play_parser = session_command.add_parser("play", parents=[flag_parser])
    session_play_parser.add_argument("session")
    session_play_parser.add_argument("--model", default=None)

    session_stop_parser = session_command.add_parser("stop", parents=[flag_parser])
    session_stop_parser.add_argument("session")

    # session step (xbox only; hard-coded rate and joystick index for now)
    session_step_parser = session_command.add_parser("step", parents=[flag_parser])
    session_step_parser.add_argument("session")
    session_step_parser.add_argument("--control", default="xbox", choices=["xbox"])

    quick_parser = mode_parser.add_parser("quick", parents=[flag_parser])
    quick_parser.add_argument("--file", required=True)
    quick_parser.add_argument("--image", default="count")
    quick_parser.add_argument("--device", default="cpu")
    quick_parser.add_argument("--model", default=None)
    quick_parser.add_argument("--control", default="xbox", choices=["xbox"])
    quick_parser.add_argument(
        "--archive",
        help="Directory to save both scene and session archives before cleanup",
    )

    return parser


# -------------------
# Main entry
# -------------------


async def main():
    parser = f_parser()
    args = parser.parse_args()

    level = getattr(logging, args.log_level.upper())
    logging.basicConfig(level=level, force=True)
    log.setLevel(level)

    client = motion.client(base=args.base, timeout=args.timeout)

    if args.mode == "scene":
        if args.command == "create":
            scene_create(client, args)
        elif args.command == "delete":
            scene_delete(client, args)
        elif args.command == "list":
            scene_list(client, args)
        elif args.command == "show":
            scene_show(client, args)
        elif args.command == "archive":
            scene_archive(client, args)

    elif args.mode == "session":
        if args.command == "create":
            await session_create(client, args)
        elif args.command == "delete":
            await session_delete(client, args)
        elif args.command == "list":
            await session_list(client, args)
        elif args.command == "show":
            await session_show(client, args)
        elif args.command == "archive":
            await session_archive(client, args)
        elif args.command == "play":
            await session_play(client, args)
        elif args.command == "stop":
            await session_stop(client, args)
        elif args.command == "step":
            await session_step(client, args)

    elif args.mode == "quick":
        await quick_run(None, args)

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
