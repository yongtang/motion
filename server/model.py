import logging

from server import runner

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("count")


def main():
    log.info(f"[main] model start")
    with runner.context() as context:
        while True:
            log.info(f"[main] recv")
            data = context.data()
            log.info(f"[main] recv data={data}")
            step = data  # model = lambda e: e
            log.info(f"[main] model data={data} step={step}")
            context.step(step)
            log.info(f"[main] send step={step}")


if __name__ == "__main__":
    main()
