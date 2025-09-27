import json
import shlex


def main():
    with open("/storage/node/node.json", "r") as f:
        meta = json.loads(f.read())

    session, model, tick = meta["session"], meta["model"], meta["tick"]

    assert model in ["model", "bounce", "remote"], f"{model} not supported"
    model = f"motion.{model}" if model == "model" else f"server.{model}"

    print(shlex.join(["python3", "-m", model]))


if __name__ == "__main__":
    main()
