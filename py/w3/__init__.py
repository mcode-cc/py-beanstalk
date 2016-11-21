
from argparse import ArgumentParser
from logger import Logger
from router.boom import Router

__version__ = '0.1.0'


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
    app.bootstrap.run(**app.parse(options.watch))
    app.run(channel=options.watch)

if __name__ == "__main__":
    boom()
