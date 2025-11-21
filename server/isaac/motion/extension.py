import asyncio
import contextlib
import functools
import itertools
import json
import time

import carb
import isaaclab.controllers
import isaaclab.devices
import isaacsim.core.experimental.prims
import isaacsim.replicator.agent.core.data_generation.writers.rtsp  # pylint: disable=W0611
import isaacsim.robot_motion
import numpy
import omni.ext
import omni.kit
import omni.replicator.core
import omni.timeline
import omni.usd
import pxr
import torch

from .channel import Channel
from .interface import Interface


class throttle:
    def __init__(self, rate):
        self.rate = rate
        self.last = 0.0

    def __call__(self, fn):
        def wrapped(*args, **kwargs):
            now = time.perf_counter()
            if now - self.last >= self.rate:
                self.last = now
                return fn(*args, **kwargs)

        return wrapped


def f_gamepad(name, entry):
    e_button = {
        "BUTTON_A": carb.input.GamepadInput.A,
        "BUTTON_B": carb.input.GamepadInput.B,
        "BUTTON_X": carb.input.GamepadInput.X,
        "BUTTON_Y": carb.input.GamepadInput.Y,
        "BUTTON_LEFTSHOULDER": carb.input.GamepadInput.LEFT_SHOULDER,
        "BUTTON_RIGHTSHOULDER": carb.input.GamepadInput.RIGHT_SHOULDER,
        "BUTTON_LEFTSTICK": carb.input.GamepadInput.LEFT_STICK,
        "BUTTON_RIGHTSTICK": carb.input.GamepadInput.RIGHT_STICK,
        "BUTTON_START": carb.input.GamepadInput.MENU1,
        "BUTTON_BACK": carb.input.GamepadInput.MENU2,
        "BUTTON_DPAD_UP": carb.input.GamepadInput.DPAD_UP,
        "BUTTON_DPAD_DOWN": carb.input.GamepadInput.DPAD_DOWN,
        "BUTTON_DPAD_LEFT": carb.input.GamepadInput.DPAD_LEFT,
        "BUTTON_DPAD_RIGHT": carb.input.GamepadInput.DPAD_RIGHT,
        # "BUTTON_GUIDE": None,
    }

    e_trigger = {
        "AXIS_TRIGGERLEFT": carb.input.GamepadInput.LEFT_TRIGGER,
        "AXIS_TRIGGERRIGHT": carb.input.GamepadInput.RIGHT_TRIGGER,
    }
    e_axis = {
        "AXIS_LEFTX": (
            carb.input.GamepadInput.LEFT_STICK_LEFT,
            carb.input.GamepadInput.LEFT_STICK_RIGHT,
        ),
        "AXIS_LEFTY": (
            carb.input.GamepadInput.LEFT_STICK_DOWN,
            carb.input.GamepadInput.LEFT_STICK_UP,
        ),
        "AXIS_RIGHTX": (
            carb.input.GamepadInput.RIGHT_STICK_LEFT,
            carb.input.GamepadInput.RIGHT_STICK_RIGHT,
        ),
        "AXIS_RIGHTY": (
            carb.input.GamepadInput.RIGHT_STICK_DOWN,
            carb.input.GamepadInput.RIGHT_STICK_UP,
        ),
    }

    if name.startswith("AXIS_"):
        assert -32768 <= entry <= 32767, f"{name} {entry}"
        if name.startswith("AXIS_TRIGGER"):
            return e_trigger[name], (
                (float(entry) / 32767.0) if entry >= 0 else (float(entry) / 32768.0)
            )
        else:
            return e_axis[name][1 if (entry >= 0) else 0], (
                (float(entry) / 32767.0) if entry >= 0 else (-float(entry) / 32768.0)
            )
    elif name.startswith("BUTTON_"):
        assert entry in (0, 1), f"{name} {entry}"
        return e_button[name], (1.0 if entry else 0.0)

    assert False, f"{name} {entry}"


# step list [{'keyboard': {'/World/Franka/panda_hand': ['A']}, 'metadata': '{"gripper": ["/World/Franka/panda_hand/panda_finger_joint1", "/World/Franka/panda_hand/panda_finger_joint2"]}'}]


def updategamepadkeyboard(provider, step, gamepad):
    carb.log_error(f"[motion.extension] [updategamepad]")
    # step already json dict
    # step = json.loads(step.decode())
    carb.log_error(f"[motion.extension] [updategamepad] keys {step.keys()}")
    if not "gamepad" in step.keys() and not "keyboard" in step.keys():
        carb.log_error(f"[motion.extension] [updategamepad2] unknow control type")
        return
    if "gamepad" in step.keys():
        if step["gamepad"] is None:
            carb.log_error(f"[motion.extension] [updategamepad2] empty gamepad event")
            assert False, f"{step}"
        assert len(step["gamepad"]) == 1
        effector, entries = next(iter(step["gamepad"].items()))
        carb.log_error(f"[motion.extension] [updategamepad {entries[0]}")
        for name, entry in entries:
            carb.log_info(f"[motion.extension] [updategamepad] entry: {name}={entry}")
            if name == "BUTTON_GUIDE":
                continue
            provider.buffer_gamepad_event(gamepad, *f_gamepad(name, entry))
    if "keyboard" in step.keys():
        if step["keyboard"] is None:
            carb.log_error(f"[motion.extension] [updategamepad2] empty keyboard")
            assert False, f"{step}"
        assert len(step["keyboard"]) == 1
        effector, entries = next(iter(step["keyboard"].items()))
        carb.log_error(f"[motion.extension] [updategamepad {entries[0]}")
        for entry in entries:
            carb.log_info(f"[motion.extension] [updategamepad] entry: ={entry}")
            provider.buffer_gamepad_event(gamepad, *f_gamepad("AXIS_LEFTX", 1000))
    provider.update_gamepad(gamepad)


def f_step(device, articulation, controller, provider, gamepad, se3, joint, link, step):
    carb.log_info(f"[motion.extension] [run_call] Step data={step}")

    items = {
        "gamepad": list(
            item.get("gamepad") for item in step if item.get("gamepad") is not None
        ),
        "keyboard": list(
            item.get("keyboard") for item in step if item.get("keyboard") is not None
        ),
    }
    carb.log_info(f"[motion.extension] [run_call] Items: {items}")

    assert not (len(items["gamepad"]) != 0 and len(items["keyboard"]) != 0)
    option = "gamepad" if len(items["gamepad"]) != 0 else "keyboard"
    carb.log_info(f"[motion.extension] [run_call] Option: {option}")

    entries = list(
        itertools.chain.from_iterable(
            itertools.chain.from_iterable(item.values()) for item in items[option]
        )
    )
    carb.log_info(f"[motion.extension] [run_call] Entries: {entries}")

    effector = set(itertools.chain.from_iterable(item.keys() for item in items[option]))
    assert len(effector) == 1, f"{effector}"
    effector = next(iter(effector))
    carb.log_info(f"[motion.extension] [run_call] Effector: {effector}")

    if option == "gamepad":
        for name, entry in entries:
            carb.log_info(f"[motion.extension] [run_call] Step: {name}, {entry}")
            if name == "BUTTON_GUIDE":
                continue
            provider.buffer_gamepad_event(gamepad, *f_gamepad(name, entry))
        provider.update_gamepad(gamepad)
    else:
        assert False, f"{option}"

    advance = se3.advance()
    command, gripper = advance[:6].unsqueeze(0), advance[6]
    carb.log_info(f"[motion.extension] [run_call] Command: {command}, {gripper}")

    jacobian = articulation.get_jacobian_matrices()
    carb.log_info(f"[motion.extension] [run_call] Jacobian: {jacobian.shape}")

    carb.log_info(
        f"[motion.extension] [run_call] Link: {list(enumerate(articulation.link_paths))}"
    )
    if "keyboard" in step[0].keys():
        step[0]["gamepad"] = step[0]["keyboard"]
    effector, entries = next(iter(step[0]["gamepad"].items()))
    index = next(
        (
            index
            for index, entries in enumerate(articulation.link_paths)
            if effector in entries
        )
    )
    entry = articulation.link_paths[index].index(effector) - 1  # first is base_link
    carb.log_info(
        f"[motion.extension] [run_call] Jacobian: index={index} entry={entry} effector={effector} link={articulation.link_paths}"
    )
    assert (
        articulation.jacobian_matrix_shape[0] == len(articulation.link_paths[index]) - 1
    ), f"{articulation.jacobian_matrix_shape} vs. {articulation.link_paths}({index})"
    jacobian = torch.tensor(
        jacobian[index, entry, :, :], dtype=torch.float32, device=device
    )
    carb.log_info(f"[motion.extension] [run_call] Jacobian entry: {jacobian.shape}")

    position, quaternion = link.get_world_poses()  # quaternion: w, x, y, z
    position, quaternion = (
        torch.tensor(
            position[link.paths.index(effector) : link.paths.index(effector) + 1],
            dtype=torch.float32,
            device=device,
        ),
        torch.tensor(
            quaternion[link.paths.index(effector) : link.paths.index(effector) + 1],
            dtype=torch.float32,
            device=device,
        ),
    )
    carb.log_info(
        f"[motion.extension] [run_call] Jacobian position/quaternion: {position.shape}/{quaternion.shape}"
    )

    controller.set_command(
        command=command,
        ee_pos=position,
        ee_quat=quaternion,
    )
    carb.log_info(f"[motion.extension] [run_call] Jacobian command: done")

    positions = torch.tensor(articulation.get_dof_positions(), device=device)
    joint_pos = torch.tensor(
        positions[index : index + 1], dtype=torch.float32, device=device
    )

    carb.log_info(
        f"[motion.extension] [run_call] Jacobian compute: jacobian={jacobian.shape} joint_pos={joint_pos.shape}"
    )
    joint_pos = controller.compute(
        ee_pos=position,
        ee_quat=quaternion,
        jacobian=jacobian,
        joint_pos=joint_pos,
    )
    carb.log_info(f"[motion.extension] [run_call] Jacobian compute: {joint_pos}")

    positions[index : index + 1] = joint_pos

    if "metadata" in step:
        metadata = step["metadata"]
        carb.log_info(f"[motion.extension] [run_call] Metadata: {metadata}")
        metadata = json.loads(metadata)
        carb.log_info(f"[motion.extension] [run_call] Metadata: {metadata}")
        if metadata.get("gripper"):
            carb.log_info(
                f"[motion.extension] [run_call] DOF: {articulation.dof_paths[index]}"
            )
            lower, upper = articulation.get_dof_limits()
            lower, upper = numpy.asarray(lower), numpy.asarray(upper)
            carb.log_info(f"[motion.extension] [run_call] DOF limit: {lower} {upper}")

            carb.log_info(
                f"[motion.extension] [run_call] Gripper: {metadata['gripper']}"
            )
            for i in [
                articulation.dof_paths[index].index(e) for e in metadata["gripper"]
            ]:
                limit_l, limit_u = lower[index][i], upper[index][i]
                carb.log_info(
                    f"[motion.extension] [run_call] Gripper(Limit[{i}]): {gripper} ({limit_l} {limit_u})"
                )
                positions[index][i] = limit_l + (limit_u - limit_l) * (
                    (gripper + 1.0) / 2
                )
                carb.log_info(
                    f"[motion.extension] [run_call] Gripper Position[{i}]: {positions[index][i]}"
                )

    articulation.set_dof_position_targets(positions.cpu().numpy())
    carb.log_info(f"[motion.extension] [run_call] Articulations positions: {positions}")

    return step, effector


def f_data(
    e,
    session,
    channel,
    articulation,
    controller,
    joint,
    link,
    annotator,
    callback,
):
    carb.log_info(f"[motion.extension] [run_call] Annotator callback")
    entries = {n: numpy.asarray(e.get_data()) for n, e in annotator.items()}
    # Expect numpy.uint8 or numpy.float64
    assert all(e.dtype in (numpy.uint8, numpy.float64) for e in entries.values()), {
        n: e.dtype for n, e in entries.items()
    }

    # Expect H×W×C with C = 3 (BGR) or 4 (BGRA).
    assert all(
        (
            (e.ndim == 1 and e.shape[0] == 0)
            or (
                e.ndim == 3
                and e.shape[0] > 0
                and e.shape[1] > 0
                and e.shape[2] in (3, 4)
            )
        )
        for e in entries.values()
    ), {n: e.shape for n, e in entries.items()}
    # BGRA/BGR -> RGBA/RGB
    entries = {
        n: (
            e
            if e.size == 0
            else (e[..., [2, 1, 0]] if e.shape[2] == 3 else e[..., [2, 1, 0, 3]])
        )
        for n, e in entries.items()
    }
    # numpy.uint8
    entries = {
        n: (
            (numpy.clip(e, 0.0, 1.0) * 255).astype(numpy.uint8)
            if e.dtype is not numpy.uint8
            else e
        )
        for n, e in entries.items()
    }
    entries = {n: numpy.ascontiguousarray(e) for n, e in entries.items()}
    entries = {
        n: {
            "dtype": str(e.dtype),
            "shape": str(e.shape),
        }
        for n, e in entries.items()
    }
    carb.log_info(
        f"[motion.extension] [run_call] Annotator callback - camera: {entries}"
    )

    carb.log_info(f"[motion.extension] [run_call] Articulation callback")
    state = dict(
        zip(
            list(itertools.chain.from_iterable(articulation.dof_paths)),
            list(
                itertools.chain.from_iterable(
                    numpy.asarray(articulation.get_dof_positions()).tolist()
                )
            ),
        )
    )
    state = {k: v for k, v in state.items() if k in joint}
    carb.log_info(
        f"[motion.extension] [run_call] Articulation callback - state: {state}"
    )

    carb.log_info(f"[motion.extension] [run_call] Link callback")
    position, quaternion = link.get_world_poses()  # quaternion: w, x, y, z
    position, quaternion = (
        numpy.asarray(position),
        numpy.asarray(quaternion)[:, [1, 2, 3, 0]],
    )  # quaternion: x, y, z, w
    pose = {
        e: {
            "position": {
                "x": float(p[0]),
                "y": float(p[1]),
                "z": float(p[2]),
            },
            "orientation": {
                "x": float(q[0]),
                "y": float(q[1]),
                "z": float(q[2]),
                "w": float(q[3]),
            },
        }
        for e, p, q in zip(link.paths, position, quaternion)
    }
    carb.log_info(f"[motion.extension] [run_call] Link callback - pose: {pose}")
    data = json.dumps(
        {
            "joint": state,
            "camera": entries,
            "pose": pose,
        },
        sort_keys=True,
    ).encode()
    carb.log_info(f"[motion.extension] [run_call] Callback: {data}")

    return callback(data) if callback is not None else data


async def run_tick(
    session,
    interface,
    channel,
    device,
    articulation,
    controller,
    provider,
    gamepad,
    se3,
    joint,
    link,
    annotator,
    loop,
):
    carb.log_info(f"[motion.extension] [run_call] [run_tick] Timeline playing")
    omni.timeline.get_timeline_interface().play()
    carb.log_info(f"[motion.extension] [run_call] [run_tick] Timeline in play")

    while True:
        await omni.kit.app.get_app().next_update_async()
        data = f_data(
            None,
            session=session,
            interface=interface,
            channel=channel,
            articulation=articulation,
            controller=controller,
            joint=joint,
            link=link,
            annotator=annotator,
            callback=None,
        )

        try:
            carb.log_info(f"[motion.extension] [run_call] [run_tick] Data {data}")
            await channel.publish_data(session, data)
            carb.log_info(
                f"[motion.extension] [run_call] [run_tick] Channel callback done"
            )
            step = await interface.tick(data)
            carb.log_info(
                f"[motion.extension] [run_call] [run_tick] Interface step {step}"
            )
            f_step(
                device=device,
                articulation=articulation,
                controller=controller,
                provider=provider,
                gamepad=gamepad,
                se3=se3,
                joint=joint,
                link=link,
                step=step,
            )
            carb.log_info(f"[motion.extension] [run_call] [run_tick] Step step={step}")
        except Exception as e:
            carb.log_info(f"[motion.extension] [run_call] [run_tick] Exception: {e}")
        carb.log_info(f"[motion.extension] [run_call] [run_tick] Data done")


async def run_norm(
    session,
    channel,
    queue,
    device,
    articulation,
    controller,
    provider,
    gamepad,
    se3,
    joint,
    link,
    annotator,
    loop,
):
    @throttle(0.2)
    def callback(data):
        try:
            carb.log_info(f"[motion.extension] [run_call] [run_norm] Data {data}")
            asyncio.run_coroutine_threadsafe(channel.publish_data(session, data), loop)
            carb.log_info(
                f"[motion.extension] [run_call] [run_norm] Channel callback done"
            )
        except Exception as e:
            carb.log_info(f"[motion.extension] [run_call] [run_norm] Callback: {e}")
            raise

    carb.log_info(f"[motion.extension] [run_call] [run_norm] subscription")
    subscription = (
        omni.kit.app.get_app()
        .get_update_event_stream()
        .create_subscription_to_pop(
            functools.partial(
                f_data,
                session=session,
                channel=channel,
                articulation=articulation,
                controller=controller,
                joint=joint,
                link=link,
                annotator=annotator,
                callback=callback,
            )
        )
    )
    carb.log_info(f"[motion.extension] [run_call] [run_norm] Timeline playing")
    omni.timeline.get_timeline_interface().play()
    carb.log_info(f"[motion.extension] [run_call] [run_norm] Timeline in play")

    while True:
        await omni.kit.app.get_app().next_update_async()
        step = []
        while True:
            try:
                step.append(queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        carb.log_info(f"[motion.extension] [run_call] [run_norm] Interface step {step}")
        if len(step) == 0:
            continue
        f_step(
            device=device,
            articulation=articulation,
            controller=controller,
            provider=provider,
            gamepad=gamepad,
            se3=se3,
            joint=joint,
            link=link,
            step=step,
        )
        carb.log_info(f"[motion.extension] [run_call] [run_norm] Step step={step}")


async def run_call(session, call):
    carb.log_info(f"[motion.extension] [run_call] Loaded session {session}")

    with open("/storage/node/session.json", "r") as f:
        metadata = json.loads(f.read())
    carb.log_info(f"[motion.extension] [run_call] Loaded metadata {metadata}")

    camera = metadata["camera"]
    carb.log_info(f"[motion.extension] [run_call] Loaded camera {camera}")

    joint = metadata["joint"]
    carb.log_info(f"[motion.extension] [run_call] Loaded joint {joint}")

    link = metadata["link"]
    carb.log_info(f"[motion.extension] [run_call] Loaded link {link}")

    ctx = omni.usd.get_context()
    if ctx.get_stage():
        carb.log_info("[motion.extension] [run_call] Closing existing stage...")
        await ctx.close_stage_async()
        carb.log_info("[motion.extension] [run_call] Existing stage closed")
        await omni.kit.app.get_app().next_update_async()

    carb.log_info("[motion.extension] [run_call] Opening stage...")
    await ctx.open_stage_async(
        "file:///storage/node/scene/scene.usd",
        load_set=omni.usd.UsdContextInitialLoadSet.LOAD_ALL,
    )

    carb.log_info("[motion.extension] [run_call] Waiting stage...")
    stage = ctx.get_stage()
    while stage is None:
        carb.log_info("[motion.extension] [run_call] Waiting loading...")
        await omni.kit.app.get_app().next_update_async()
        stage = ctx.get_stage()
    assert stage

    carb.log_info(f"[motion.extension] [run_call] Stage loaded")

    provider = carb.input.acquire_input_provider()
    gamepad = provider.create_gamepad("VirtualPad", "virt-0")  # name, id
    provider.set_gamepad_connected(gamepad, True)

    carb.log_info(f"[motion.extension] [run_call] Gamepad: {gamepad}")
    se3 = isaaclab.devices.gamepad.Se3Gamepad(
        isaaclab.devices.gamepad.Se3GamepadCfg(
            dead_zone=0.05,
            pos_sensitivity=0.2,
            rot_sensitivity=0.4,
        )
    )
    carb.log_info(f"[motion.extension] [run_call] Gamepad SE3: {se3}")

    articulation = [
        str(e.GetPath())
        for e in stage.Traverse()
        if e.HasAPI(pxr.UsdPhysics.ArticulationRootAPI)
    ]
    carb.log_info(f"[motion.extension] [run_call] Articulation: {articulation}")

    articulation = isaacsim.core.experimental.prims.Articulation(articulation)
    carb.log_info(f"[motion.extension] [run_call] Articulation: {articulation}")

    device = "cuda"
    controller = isaaclab.controllers.DifferentialIKController(
        isaaclab.controllers.DifferentialIKControllerCfg(
            command_type="pose",  # "position" | "pose"
            use_relative_mode=True,  # True → 6-D delta commands
            ik_method="pinv",  # "pinv" | "svd" | "trans" | "dls"
            ik_params={"k_val": 1.0},  # or {"lambda_val": 0.05} for DLS, etc.
        ),
        num_envs=1,
        device=device,
    )
    carb.log_info(
        f"[motion.extension] [run_call] Articulation Controller: {controller}"
    )

    carb.log_info(
        f"[motion.extension] [run_call] Articulation Joint: {articulation.dof_paths}"
    )
    joint = list(
        e
        for e in itertools.chain.from_iterable(articulation.dof_paths)
        if ("*" in joint or e in joint)
    )
    carb.log_info(f"[motion.extension] [run_call] Articulation Joint: {joint}")

    carb.log_info(
        f"[motion.extension] [run_call] Articulation Link: {articulation.link_paths}"
    )
    link = isaacsim.core.experimental.prims.XformPrim(
        paths=list(
            e
            for e in itertools.chain.from_iterable(articulation.link_paths)
            if ("*" in link or e in link)
        )
    )
    carb.log_info(f"[motion.extension] [run_call] Articulation Link: {link.paths}")

    camera = (
        {
            str(pxr.UsdGeom.Camera(e).GetPrim().GetPath()): camera["*"]
            for e in stage.Traverse()
            if e.IsA(pxr.UsdGeom.Camera) and e.IsActive()
        }
        if "*" in camera
        else camera
    )
    carb.log_info(f"[motion.extension] [run_call] Camera: {camera}")

    with open("/run/motion/camera.json", "w") as f:
        f.write(json.dumps(camera))
    carb.log_info(f"[motion.extension] [run_call] Camera: /run/motion/camera.json")

    render = {
        e: omni.replicator.core.create.render_product(e, (v["width"], v["height"]))
        for e, v in camera.items()
    }
    carb.log_info(f"[motion.extension] [run_call] Render: {render}")

    writer = omni.replicator.core.WriterRegistry.get("RTSPWriter")
    writer.initialize(
        rtsp_stream_url="rtsp://127.0.0.1:8554/RTSPWriter",
        rtsp_rgb=True,
    )
    carb.log_info(f"[motion.extension] [run_call] Writer: {writer}")

    annotator = {
        e: omni.replicator.core.AnnotatorRegistry.get_annotator("rgb")
        for e, v in camera.items()
    }
    carb.log_info(f"[motion.extension] [run_call] Annotator: {annotator}")

    if len(render):
        writer.attach(list(render.values()))
        carb.log_info(f"[motion.extension] [run_call] Writer attached")
        for k, v in render.items():
            annotator[k].attach(v)
        carb.log_info(f"[motion.extension] [run_call] Annotator attached")
    else:
        carb.log_info(f"[motion.extension] [run_call] Writer/Annotator attach skipped")

    try:
        carb.log_info(f"[motion.extension] [run_call] Callback call")
        await call(
            device=device,
            articulation=articulation,
            controller=controller,
            provider=provider,
            gamepad=gamepad,
            se3=se3,
            joint=joint,
            link=link,
            annotator=annotator,
        )
        carb.log_info(f"[motion.extension] [run_call] Callback done")

    except Exception as e:
        carb.log_info(f"[motion.extension] [run_call] [Exception]: {e}")
    finally:
        if len(render):
            with contextlib.suppress(Exception):
                for k, v in render.items():
                    annotator[k].detach(v)
            carb.log_info(f"[motion.extension] [run_call] Camera annotator detached")
            with contextlib.suppress(Exception):
                writer.detach(list(camera.values()))
            carb.log_info(f"[motion.extension] [run_call] Camera detached")
        else:
            carb.log_info(
                f"[motion.extension] [run_call] Writer/Annotator detach skipped"
            )


async def run_node(session: str, tick: bool):
    carb.log_info(f"[motion.extension] [run_node] session={session} tick={tick}")

    queue = asyncio.Queue()

    async def f(msg):
        carb.log_info(f"[motion.extension] [run_node] step {msg}")
        step = json.loads(msg.data)
        carb.log_info(f"[motion.extension] [run_node] step {step}")
        await queue.put(step)
        carb.log_info(f"[motion.extension] [run_node] step queue")

    # Channel
    channel = Channel()
    await channel.start()
    carb.log_info(f"[motion.extension] [run_node] channel start")
    subscribe = await channel.subscribe_step(session, f)
    carb.log_info(f"[motion.extension] [run_node] channel subscribe")

    loop = asyncio.get_running_loop()

    try:
        await run_call(
            session,
            (
                functools.partial(
                    run_tick,
                    session=session,
                    channel=channel,
                    queue=queue,
                    loop=loop,
                )
                if tick
                else functools.partial(
                    run_norm,
                    session=session,
                    channel=channel,
                    queue=queue,
                    loop=loop,
                )
            ),
        )
    except Exception as e:
        carb.log_info(f"[motion.extension] [run_node] [Exception]: {e}")
    finally:
        carb.log_info(f"[motion.extension] [run_node] channel unsubscribe")
        await subscribe.unsubscribe()
        carb.log_info(f"[motion.extension] [run_node] channel close")
        await channel.close()


async def main():
    with open("/storage/node/node.json", "r") as f:
        meta = json.loads(f.read())
    carb.log_info(f"[motion.extension] [main] meta={meta}")

    session, tick = meta["session"], meta["tick"]
    await run_node(session, tick)


class MotionExtension(omni.ext.IExt):
    def __init__(self):
        self.task = None
        super().__init__()

    def on_startup(self, ext_id):
        carb.log_info(f"[motion.extension] Startup [{ext_id}]")

        self.task = asyncio.create_task(main())

    def on_shutdown(self):
        carb.log_info("[motion.extension] Shutdown")
        if self.task and not self.task.done():
            self.task.cancel()
