from .scene import Scene


class Session:
    def __init__(self, scene: Scene):
        self.scene = scene

    def step(self):
        print(f"Step scene {self.scene}")

    def play(self):
        print(f"Play scene {self.scene}")
