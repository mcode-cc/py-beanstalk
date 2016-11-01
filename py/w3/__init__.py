
from argparse import ArgumentParser
from warehouse import Logger
from router.boom import Router

__version__ = '0.1.0'


def logger():
    ap = ArgumentParser('boom')
    ap.add_argument(
        "-w", "--watch",
        dest='watch',
        required=True,
        help="watch channel"
    )
    options = ap.parse_args()
    app = Router(log=Logger('Router'))
    app.run(channel=options.watch)


def boom():
    ap = ArgumentParser('boom')
    ap.add_argument(
        "-w", "--watch",
        dest='watch',
        required=False,
        help="watch channel"
    )
    options = ap.parse_args()
    app = Router(log=Logger('Router'))
    app.bootstrap(channel=options.watch)
    app.run(channel=options.watch)

if __name__ == "__main__":
    boom()
