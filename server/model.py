import json
import shlex


def main():
    with open("/storage/node/node.json", "r") as f:
        meta = json.loads(f.read())

    session, tick = meta["session"], meta["tick"]
    print(shlex.join(["python3", "-m", "server.bounce"]))


if __name__ == "__main__":
    main()
