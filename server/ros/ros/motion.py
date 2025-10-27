import asyncio
import json
import threading

import rclpy
from builtin_interfaces.msg import Duration
from rclpy.node import Node
from rclpy.qos import DurabilityPolicy, HistoryPolicy, QoSProfile, ReliabilityPolicy
from sensor_msgs.msg import JointState, Joy
from trajectory_msgs.msg import JointTrajectory, JointTrajectoryPoint

from .channel import Channel
from .interface import Interface


class Motion(Node):

    def __init__(self, session, channel, interface, loop):
        super().__init__("motion")
        self.session = session
        self.channel = channel
        self.interface = interface
        self.loop = loop

        qos = QoSProfile(
            reliability=ReliabilityPolicy.RELIABLE,
            durability=DurabilityPolicy.TRANSIENT_LOCAL,
            history=HistoryPolicy.KEEP_LAST,
            depth=10,
        )

        self.subscription = self.create_subscription(
            JointState, "joint_states", self.listener_callback, 10
        )
        self.joint = self.create_publisher(JointTrajectory, "joint_trajectory", qos)
        self.gamepad = self.create_publisher(Joy, "joy", 10)
        self.timer = self.create_timer(0.5, self.timer_callback)

    def timer_callback(self):
        data = self.interface.recv()
        self.get_logger().info(f'Interface recv: "{data}"')
        if data is None:
            return
        data = data.decode()
        data = json.loads(data)

        if "gamepad" in data:
            assert len(data["gamepad"]) == 1, f"{data}"
            frame, entries = next(iter(data["gamepad"].items()))

            e_button = {
                "BUTTON_A": 0,
                "BUTTON_B": 1,
                "BUTTON_X": 2,
                "BUTTON_Y": 3,
                "BUTTON_LEFTSHOULDER": 4,
                "BUTTON_RIGHTSHOULDER": 5,
                "BUTTON_BACK": 6,
                "BUTTON_START": 7,
                "BUTTON_GUIDE": 8,
                "BUTTON_LEFTSTICK": 9,
                "BUTTON_RIGHTSTICK": 10,
            }

            e_axis = {
                "AXIS_LEFTX": 0,
                "AXIS_LEFTY": 1,
                "AXIS_TRIGGERLEFT": 2,
                "AXIS_RIGHTX": 3,
                "AXIS_RIGHTY": 4,
                "AXIS_TRIGGERRIGHT": 5,
            }
            e_pad = {
                "BUTTON_DPAD_UP": carb.input.GamepadInput.DPAD_UP,
                "BUTTON_DPAD_DOWN": carb.input.GamepadInput.DPAD_DOWN,
                "BUTTON_DPAD_LEFT": carb.input.GamepadInput.DPAD_LEFT,
                "BUTTON_DPAD_RIGHT": carb.input.GamepadInput.DPAD_RIGHT,
            }

            msg = Joy()
            msg.header.stamp = self.get_clock().now().to_msg()
            msg.header.frame_id = frame

            dpad = {
                "BUTTON_DPAD_LEFT": 0,
                "BUTTON_DPAD_RIGHT": 0,
                "BUTTON_DPAD_UP": 0,
                "BUTTON_DPAD_DOWN": 0,
            }
            for name, entry in entries:
                if name.startswith("BUTTON_DPAD_"):
                    dpad[name] = entry
                else:
                    if name.startswith("BUTTON_"):
                        msg.buttons[e_button[name]] = entry
                    elif name.startswith("AXIS_"):
                        msg.axes[e_axis[name]] = (
                            (float(entry) / 32767.0)
                            if entry >= 0
                            else (float(entry) / 32768.0)
                        )
                    else:
                        assert False, f"{name}, {entry}"
            if dpad["BUTTON_DPAD_LEFT"] != dpad["BUTTON_DPAD_RIGHT"]:
                msg.axes[6] = (
                    -1.0
                    if (dpad["BUTTON_DPAD_LEFT"] > dpad["BUTTON_DPAD_RIGHT"])
                    else 1.0
                )
            if dpad["BUTTON_DPAD_UP"] != dpad["BUTTON_DPAD_DOWN"]:
                msg.axes[7] = (
                    -1.0 if (dpad["BUTTON_DPAD_UP"] > dpad["BUTTON_DPAD_DOWN"]) else 1.0
                )

            self.get_logger().info(f'Publishing: "{msg}"')
            self.gamepad.publish(msg)
        elif "joint" in data:
            keys, values = zip(*data["joint"].items())
            point = JointTrajectoryPoint()
            point.time_from_start = Duration(sec=0)
            point.positions = values
            msg = JointTrajectory()
            msg.joint_names = keys
            msg.points = [point]

            self.get_logger().info(f'Publishing: "{msg}"')
            self.joint.publish(msg)
        else:
            assert False, f"{data}"
        self.get_logger().info(f'Published: "{msg}"')

    def listener_callback(self, msg):
        self.get_logger().info(f'I heard: "{msg}"')
        data = {"joint": dict(zip(msg.name, msg.position))}
        data = json.dumps(data)
        data = data.encode()
        asyncio.run_coroutine_threadsafe(
            self.channel.publish_data(self.session, data), self.loop
        ).result()
        self.get_logger().info(f'Channel send: "{data}"')
        self.interface.send(data)
        self.get_logger().info(f'Interface send: "{data}"')


def main(args=None):
    with open("/storage/node/node.json", "r") as f:
        meta = json.loads(f.read())

    session, tick = meta["session"], meta["tick"]
    assert not tick, f"meta={meta}"

    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()

    interface = Interface(tick=tick, sync=True)
    interface.ready()

    try:
        channel = Channel()
        asyncio.run_coroutine_threadsafe(channel.start(), loop).result()
        try:
            rclpy.init(args=args)

            node = Motion(
                session=session, channel=channel, interface=interface, loop=loop
            )

            rclpy.spin(node)

            # Destroy the node explicitly
            # (optional - otherwise it will be done automatically
            # when the garbage collector destroys the node object)
            node.destroy_node()
            rclpy.shutdown()
        finally:
            asyncio.run_coroutine_threadsafe(channel.close(), loop)
    finally:
        interface.close()


if __name__ == "__main__":
    main()
