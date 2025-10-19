import asyncio
import json
import threading

import rclpy
from builtin_interfaces.msg import Duration
from geometry_msgs.msg import TwistStamped
from rclpy.node import Node
from sensor_msgs.msg import JointState
from trajectory_msgs.msg import JointTrajectory, JointTrajectoryPoint
from rclpy.qos import QoSProfile, ReliabilityPolicy, DurabilityPolicy, HistoryPolicy

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
        self.twist = self.create_publisher(TwistStamped, "twist", 10)
        self.joint = self.create_publisher(JointTrajectory, "joint_trajectory", qos)
        self.timer = self.create_timer(0.5, self.timer_callback)

    def timer_callback(self):
        data = self.interface.recv()
        self.get_logger().info(f'Interface recv: "{data}"')
        if data is None:
            return
        data = data.decode()
        data = json.loads(data)

        if data.get("twist"):
            assert len(data["twist"]) == 1, f"{data}"
            key, value = next(iter(data["twist"].items()))
            assert key == "panda_hand", f"{data}"
            msg = TwistStamped()
            msg.header.stamp = self.get_clock().now().to_msg()
            msg.header.frame_id = "panda_hand"
            msg.twist.linear.x = value["linear"]["x"]
            msg.twist.linear.y = value["linear"]["y"]
            msg.twist.linear.z = value["linear"]["z"]
            msg.twist.angular.x = value["angular"]["x"]
            msg.twist.angular.y = value["angular"]["y"]
            msg.twist.angular.z = value["angular"]["z"]

            self.get_logger().info(f'Publishing: "{msg}"')
            self.twist.publish(msg)
        elif data.get("joint"):
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
