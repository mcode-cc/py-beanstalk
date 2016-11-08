
from argparse import ArgumentParser
from router.boom.client import CLI
from warehouse import Logger

__version__ = '0.1.0'


def main():
    ap = ArgumentParser('boom')
    ap.add_argument(
        "-w", "--watch",
        dest='watch',
        required=False,
        help="watch channel"
    )
    options = ap.parse_args()
    app = CLI(log=Logger("CLI"))
    app.bootstrap(channel=options.watch)
    app.run(channel=options.watch)


if __name__ == "__main__":
    main()
