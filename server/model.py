from server import runner


def main():
    with runner.context() as context:
        while True:
            data = context.data()
            context.step(data)


if __name__ == "__main__":
    main()
