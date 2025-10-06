import asyncio
import json
import threading

import rclpy
from rclpy.node import Node
from std_msgs.msg import String

from .channel import Channel
from .interface import Interface


class Motion(Node):

    def __init__(self, session, channel, interface, loop):
        super().__init__("motion")
        self.session = session
        self.channel = channel
        self.interface = interface
        self.loop = loop

        self.subscription = self.create_subscription(
            String, "joint_states", self.listener_callback, 10
        )
        self.publisher = self.create_publisher(String, "joint_trajectory", 10)
        self.timer = self.create_timer(0.5, self.timer_callback)

    def timer_callback(self):
        data = self.interface.recv()
        self.get_logger().info(f'Interface recv: "{data}"')
        if data is None:
            return
        data = data.decode()
        data = json.loads(data)
        msg = String()
        msg.data = f"{data['ros']}"
        self.publisher.publish(msg)
        self.get_logger().info(f'Publishing: "{msg.data}"')

    def listener_callback(self, msg):
        self.get_logger().info(f'I heard: "{msg.data}"')
        data = {"ros": msg.data}
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
