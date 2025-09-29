import argparse
import asyncio
import json
import logging
import pathlib
import sys

import motion

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def f_fail(code: int, msg: str) -> None:
    """Print an error and exit with given code."""
    print(msg, file=sys.stderr)
    sys.exit(code)


def f_print(obj, *, json_mode: bool, pretty_mode: bool, quiet_mode: bool) -> None:
    """
    Print object(s) depending on flags.

    IMPORTANT: We avoid calling .json() on live Session objects because they may
    hold a non-serializable httpx.AsyncClient on a private attr. Instead we convert
    known models to safe plain dicts.
    """
    if quiet_mode:
        return

    def to_safe(o):
        # Special-case motion.Session (avoid AsyncClient/private attrs in JSON)
        if isinstance(o, motion.Session):
            return {
                "uuid": str(o.uuid),
                "scene": str(o.scene.uuid),
                "joint": o.joint,
                "camera": o.camera,
                "link": o.link,
            }
        # Special-case motion.Scene for symmetry / stability
        if isinstance(o, motion.Scene):
            return {
                "uuid": str(o.uuid),
                "runner": {
                    "image": o.runner.image.value,
                    "device": o.runner.device.value,
                },
            }
        # Fallbacks:
        if hasattr(o, "json") and callable(o.json):
            # Parse the model's own JSON (works for plain BaseModel objects)
            try:
                return json.loads(o.json())
            except Exception:
                pass
        return o  # plain dict/str/etc.

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
    """Resolve prefix query against items, require unique match."""
    matches = [i for i in items if str(i.uuid).startswith(q)]
    if not matches:
        f_fail(3, f"{kind} not found for {q!r}")
    if len(matches) > 1:
        f_fail(4, f"ambiguous {kind} prefix {q!r}, {len(matches)} matches")
    return matches[0]


# -------------------
# Scene commands
# -------------------


def scene_create(client, args, *, json_mode, pretty_mode, quiet_mode):
    file = pathlib.Path(args.file)
    scene = client.scene.create(file, args.image, args.device)
    f_print(scene, json_mode=json_mode, pretty_mode=pretty_mode, quiet_mode=quiet_mode)


def scene_delete(client, args, *, json_mode, pretty_mode, quiet_mode):
    scenes = client.scene.search(args.scene)
    scene = f_prefix(scenes, args.scene, kind="scene")
    client.scene.delete(scene)
    f_print(scene, json_mode=json_mode, pretty_mode=pretty_mode, quiet_mode=quiet_mode)


def scene_list(client, args, *, json_mode, pretty_mode, quiet_mode):
    scenes = client.scene.search(args.q or "")
    f_print(scenes, json_mode=json_mode, pretty_mode=pretty_mode, quiet_mode=quiet_mode)


def scene_show(client, args, *, json_mode, pretty_mode, quiet_mode):
    scenes = client.scene.search(args.scene)
    scene = f_prefix(scenes, args.scene, kind="scene")
    f_print(scene, json_mode=json_mode, pretty_mode=pretty_mode, quiet_mode=quiet_mode)


def scene_archive(client, args, *, json_mode, pretty_mode, quiet_mode):
    scenes = client.scene.search(args.scene)
    scene = f_prefix(scenes, args.scene, kind="scene")
    out = pathlib.Path(args.output)
    client.scene.archive(scene, out)
    f_print(
        {"output": str(out)},
        json_mode=json_mode,
        pretty_mode=pretty_mode,
        quiet_mode=quiet_mode,
    )


# -------------------
# Session commands
# -------------------


async def session_create(client, args, *, json_mode, pretty_mode, quiet_mode):
    scenes = client.scene.search(args.scene)
    scene = f_prefix(scenes, args.scene, kind="scene")
    async with client.session.create(scene) as session:
        f_print(
            session, json_mode=json_mode, pretty_mode=pretty_mode, quiet_mode=quiet_mode
        )


def session_delete(client, args, *, json_mode, pretty_mode, quiet_mode):
    sessions = client.session.search(args.session)
    session = f_prefix(sessions, args.session, kind="session")
    client.session.delete(session)
    f_print(
        session, json_mode=json_mode, pretty_mode=pretty_mode, quiet_mode=quiet_mode
    )


def session_list(client, args, *, json_mode, pretty_mode, quiet_mode):
    sessions = client.session.search(args.q or "")
    f_print(
        sessions, json_mode=json_mode, pretty_mode=pretty_mode, quiet_mode=quiet_mode
    )


def session_show(client, args, *, json_mode, pretty_mode, quiet_mode):
    sessions = client.session.search(args.session)
    session = f_prefix(sessions, args.session, kind="session")
    f_print(
        session, json_mode=json_mode, pretty_mode=pretty_mode, quiet_mode=quiet_mode
    )


def session_archive(client, args, *, json_mode, pretty_mode, quiet_mode):
    sessions = client.session.search(args.session)
    session = f_prefix(sessions, args.session, kind="session")
    out = pathlib.Path(args.output)
    client.session.archive(session, out)
    f_print(
        {"output": str(out)},
        json_mode=json_mode,
        pretty_mode=pretty_mode,
        quiet_mode=quiet_mode,
    )


async def session_play(client, args, *, json_mode, pretty_mode, quiet_mode):
    sessions = client.session.search(args.session)
    async with f_prefix(sessions, args.session, kind="session") as session:
        await session.play(model=args.model)
        f_print(
            session, json_mode=json_mode, pretty_mode=pretty_mode, quiet_mode=quiet_mode
        )


async def session_stop(client, args, *, json_mode, pretty_mode, quiet_mode):
    sessions = client.session.search(args.session)
    async with f_prefix(sessions, args.session, kind="session") as session:
        await session.stop()
        f_print(
            session, json_mode=json_mode, pretty_mode=pretty_mode, quiet_mode=quiet_mode
        )


# -------------------
# Parser
# -------------------


def f_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="http://127.0.0.1:8080")
    parser.add_argument("--timeout", type=float, default=10.0)

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

    return parser


# -------------------
# Main entry
# -------------------


async def main():
    parser = f_parser()
    args = parser.parse_args()
    client = motion.client(base=args.base, timeout=args.timeout)

    if args.mode == "scene":
        if args.command == "create":
            scene_create(
                client,
                args,
                json_mode=args.json,
                pretty_mode=args.pretty,
                quiet_mode=args.quiet,
            )
        elif args.command == "delete":
            scene_delete(
                client,
                args,
                json_mode=args.json,
                pretty_mode=args.pretty,
                quiet_mode=args.quiet,
            )
        elif args.command == "list":
            scene_list(
                client,
                args,
                json_mode=args.json,
                pretty_mode=args.pretty,
                quiet_mode=args.quiet,
            )
        elif args.command == "show":
            scene_show(
                client,
                args,
                json_mode=args.json,
                pretty_mode=args.pretty,
                quiet_mode=args.quiet,
            )
        elif args.command == "archive":
            scene_archive(
                client,
                args,
                json_mode=args.json,
                pretty_mode=args.pretty,
                quiet_mode=args.quiet,
            )

    elif args.mode == "session":
        if args.command == "create":
            await session_create(
                client,
                args,
                json_mode=args.json,
                pretty_mode=args.pretty,
                quiet_mode=args.quiet,
            )
        elif args.command == "delete":
            session_delete(
                client,
                args,
                json_mode=args.json,
                pretty_mode=args.pretty,
                quiet_mode=args.quiet,
            )
        elif args.command == "list":
            session_list(
                client,
                args,
                json_mode=args.json,
                pretty_mode=args.pretty,
                quiet_mode=args.quiet,
            )
        elif args.command == "show":
            session_show(
                client,
                args,
                json_mode=args.json,
                pretty_mode=args.pretty,
                quiet_mode=args.quiet,
            )
        elif args.command == "archive":
            session_archive(
                client,
                args,
                json_mode=args.json,
                pretty_mode=args.pretty,
                quiet_mode=args.quiet,
            )
        elif args.command == "play":
            await session_play(
                client,
                args,
                json_mode=args.json,
                pretty_mode=args.pretty,
                quiet_mode=args.quiet,
            )
        elif args.command == "stop":
            await session_stop(
                client,
                args,
                json_mode=args.json,
                pretty_mode=args.pretty,
                quiet_mode=args.quiet,
            )

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
