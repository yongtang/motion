import logging

from server import runner

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("relay")


def main():
    log.info(f"[main] model start")
    with runner.context() as context:
        while True:
            log.info(f"[main] recv")
            data = context.data()
            log.info(f"[main] recv data={data}")
            context.step(data)
            log.info(f"[main] send step={data}")


if __name__ == "__main__":
    main()
