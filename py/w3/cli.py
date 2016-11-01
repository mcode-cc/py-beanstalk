
from argparse import ArgumentParser
from warehouse import Logger
from router.boom.client import CLI

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
    app = CLI(log=Logger('Router'))
    app.run(channel=app.bootstrap(channel=options.watch))


if __name__ == "__main__":
    main()
