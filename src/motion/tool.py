import argparse
import asyncio
import logging
import math
import pathlib
import time
import uuid

import motion

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


async def f_tick(
    session,
    joystick_index=0,
):

    import pygame

    class XboxTeleop:
        """
        Xbox joystick teleop controller using pygame, matching ROS2 teleop_twist_joy (Humble)
        for base mappings, with two extras:
          1) Momentary rotation mode (hold rotation_button) to repurpose the left stick into roll/pitch
          2) Exit button to cleanly stop the async loop

        --------------------------------------------------------------------------
        ROS2 OUTPUTS (naming and structure follow ROS2 messages):
          - Twist (geometry_msgs/Twist semantics):
              twist["linear"]  = (x, y, z)   # m/s
              twist["angular"] = (x, y, z)   # rad/s
          - Pose (geometry_msgs/Pose semantics):
              pose["position"]    = (x, y, z)
              pose["orientation"] = (x, y, z, w)  # quaternion (ROS2 order = xyzw)

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
        BEHAVIOR (this class):
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
                twist.linear.{x,y,z} = twist.angular.z = 0.0

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
            change the line in compute_twist(): linear.x = -a1 * linear_scale.
          - This class integrates a synthetic pose for convenience; teleop_twist_joy
            normally publishes Twist only (no pose integration).

        DIFFERENCE WITH ISAAC SIM 5.0.0:
          - Isaac’s get_world_pose returns quaternion as (w, x, y, z) [scalar-first],
            whereas this class (ROS2-style) returns (x, y, z, w) [xyzw].
          - Isaac exposes velocities via separate getters (get_linear_velocities,
            get_angular_velocities) rather than a combined Twist object.

        CALLBACKS:
          - Callback must be async: `async def callback(pose, twist): ...`
          - The callback is awaited inside loop_async() after each step.
        """

        def __init__(
            self,
            joystick_index=joystick_index,
            max_linear=0.7,
            max_linear_turbo=1.5,
            max_angular=0.4,
            deadzone=0.08,
            enable_button=2,
            turbo_button=5,
            rotation_button=9,
            exit_button=1,
            callback=None,
        ):

            pygame.init()
            pygame.joystick.init()
            if pygame.joystick.get_count() == 0:
                raise RuntimeError("No joystick found.")
            self.joystick = pygame.joystick.Joystick(joystick_index)
            self.joystick.init()

            self.max_linear = max_linear
            self.max_linear_turbo = max_linear_turbo
            self.max_angular = max_angular
            self.deadzone = deadzone

            self.enable_button = enable_button
            self.turbo_button = turbo_button
            self.rotation_button = rotation_button
            self.exit_button = exit_button
            self.callback = callback  # must be async

            # Internal pose state (Euler integrated; converted to quaternion for ROS2-style pose)
            self.x = self.y = self.z = 0.0
            self.roll = self.pitch = self.yaw = 0.0
            self.last_time = time.time()

            logging.info(
                f"Joystick initialized: {self.joystick.get_name()} "
                f"axes={self.joystick.get_numaxes()} buttons={self.joystick.get_numbuttons()}"
            )

        # -------------------
        # Helper functions
        # -------------------
        def euler_to_quaternion_xyzw(self, roll: float, pitch: float, yaw: float):
            """Convert Euler RPY (rad) to ROS2 quaternion (x, y, z, w)."""
            cy, sy = math.cos(yaw * 0.5), math.sin(yaw * 0.5)
            cp, sp = math.cos(pitch * 0.5), math.sin(pitch * 0.5)
            cr, sr = math.cos(roll * 0.5), math.sin(roll * 0.5)

            w = cr * cp * cy + sr * sp * sy
            x = sr * cp * cy - cr * sp * sy
            y = cr * sp * cy + sr * cp * sy
            z = cr * cp * sy - sr * sp * cy
            return (x, y, z, w)

        def apply_deadzone(self, value: float) -> float:
            return value if abs(value) >= self.deadzone else 0.0

        def safe_axis(self, index: int) -> float:
            return (
                self.joystick.get_axis(index)
                if index < self.joystick.get_numaxes()
                else 0.0
            )

        def button_pressed(self, index: int) -> bool:
            return (index < self.joystick.get_numbuttons()) and (
                self.joystick.get_button(index) == 1
            )

        def read_axes(self):
            """Read first four axes: a0=LX, a1=LY, a2=RX, a3=RY."""
            pygame.event.pump()
            return (
                self.apply_deadzone(self.safe_axis(0)),
                self.apply_deadzone(self.safe_axis(1)),
                self.apply_deadzone(self.safe_axis(2)),
                self.apply_deadzone(self.safe_axis(3)),
            )

        def compute_twist(self, a0: float, a1: float, a2: float, a3: float):
            """
            Compute Twist values from joystick input.

            Returns a dict shaped like geometry_msgs/Twist:
              {
                "linear":  (x, y, z),  # m/s
                "angular": (x, y, z),  # rad/s
              }
            """
            deadman = self.button_pressed(self.enable_button)
            turbo = self.button_pressed(self.turbo_button)
            rotation_hold = self.button_pressed(self.rotation_button)

            linear_scale = self.max_linear * (self.max_linear_turbo if turbo else 1.0)
            angular_scale = self.max_angular

            lin_x = lin_y = lin_z = ang_x = ang_y = ang_z = 0.0
            if deadman:
                if rotation_hold:
                    ang_x = a1 * angular_scale  # roll
                    ang_y = a0 * angular_scale  # pitch
                else:
                    lin_x = a1 * linear_scale
                    ang_z = a0 * angular_scale
            return {"linear": (lin_x, lin_y, lin_z), "angular": (ang_x, ang_y, ang_z)}

        def integrate_pose(self, twist, dt: float):
            """Integrate a synthetic pose from Twist (ROS2-style)."""
            lin_x, lin_y, lin_z = twist["linear"]
            ang_x, ang_y, ang_z = twist["angular"]

            self.x += lin_x * dt
            self.y += lin_y * dt
            self.z += lin_z * dt
            self.roll += ang_x * dt
            self.pitch += ang_y * dt
            self.yaw += ang_z * dt

            self.roll = (self.roll + math.pi) % (2 * math.pi) - math.pi
            self.pitch = (self.pitch + math.pi) % (2 * math.pi) - math.pi
            self.yaw = (self.yaw + math.pi) % (2 * math.pi) - math.pi

        def get_pose(self):
            """Return ROS2-style Pose dict (position + quaternion xyzw)."""
            q_xyzw = self.euler_to_quaternion_xyzw(self.roll, self.pitch, self.yaw)
            return {"position": (self.x, self.y, self.z), "orientation": q_xyzw}

        def run_once(self):
            """Do one joystick step and return (pose, twist)."""
            now = time.time()
            dt = max(0.001, min(now - self.last_time, 0.5))
            self.last_time = now

            a0, a1, a2, a3 = self.read_axes()
            twist = self.compute_twist(a0, a1, a2, a3)
            self.integrate_pose(twist, dt)
            pose = self.get_pose()
            return pose, twist

        async def loop_async(self, rate_hz: float = 20.0):
            """Async loop; always awaits async callback."""
            period = 1.0 / max(1.0, rate_hz)
            logging.info(
                "Running (async): hold ENABLE (deadman), hold ROTATION (LS click) for roll/pitch, "
                "TURBO scales linear.x, EXIT (B) quits."
            )
            try:
                while True:
                    pose, twist = self.run_once()

                    if self.callback:
                        await self.callback(pose, twist)

                    if self.button_pressed(self.exit_button):
                        logging.info("Exit button pressed. Quitting...")
                        break

                    await asyncio.sleep(period)
            finally:
                pygame.quit()

    async def f(pose, twist):
        logging.info(f"Callback received pose={pose}, twist={twist}")
        await stream.step({"pose": pose, "twist": twist})

    async with session.stream(start=None) as stream:

        controller = XboxTeleop(callback=f)
        await controller.loop_async(rate_hz=20.0)


async def f_live(client, file, image, device, duration):
    log.info(f"[Scene] Creating from {file} (image={image}, device={device}) ...")
    scene = client.scene.create(pathlib.Path(file), image, device)
    log.info(f"[Scene {scene.uuid}] Created")

    log.info("[Session] Creating...")
    async with client.session.create(scene) as session:
        log.info(f"[Session {session.uuid}] Starting playback...")
        await session.play()

        log.info(f"[Session {session.uuid}] Waiting for play ...")
        await session.wait("play", timeout=300.0)
        log.info(f"[Session {session.uuid}] Playing (status-confirmed)")

        interval = 15
        for count in range(0, duration, interval):
            await asyncio.sleep(interval)
            log.info(f"[Session {session.uuid}] Elapsed {count}/{duration} ...")

        log.info(f"[Session {session.uuid}] Stopping ...")
        await session.stop()

        log.info(f"[Session {session.uuid}] Waiting for stop ...")
        await session.wait("stop", timeout=60.0)
        log.info(f"[Session {session.uuid}] Stopped (status-confirmed)")

        client.session.delete(session)

    client.scene.delete(scene)


async def main():
    parser = argparse.ArgumentParser()

    mode = parser.add_subparsers(dest="mode", required=True)

    mode_parser = argparse.ArgumentParser(add_help=False)
    mode_parser.add_argument("--base", default="http://127.0.0.1:8080")

    live_parser = mode.add_parser("live", parents=[mode_parser])
    live_parser.add_argument("--duration", type=int, default=3600)
    live_parser.add_argument("--file", required=True)
    live_parser.add_argument("--image", default="count")
    live_parser.add_argument("--device", default="cpu")

    tick_parser = mode.add_parser("tick", parents=[mode_parser])
    tick_parser.add_argument("--session", type=uuid.UUID, required=True)

    args = parser.parse_args()

    match args.mode:
        case "live":
            client = motion.client(args.base)
            await f_live(
                client,
                args.file,
                args.image,
                args.device,
                args.duration,
            )
        case "tick":
            async with motion.Session(args.base, str(args.session)) as session:
                await f_tick(session)
        case other:
            raise ValueError(f"Unsupported mode {other}")

    log.info("[Tool] Done")


if __name__ == "__main__":
    asyncio.run(main())
