
from argparse import ArgumentParser
from boom.client import CLI
from warehouse import Logger
from boom import Router

__version__ = '0.2.0'


if __name__ == "__main__":
    ap = ArgumentParser('boom')
    ap.add_argument(
        "-m", "--method",
        dest='method',
        required=True,
        help="method"
    )
    ap.add_argument(
        "-w", "--watch",
        dest='watch',
        required=False,
        help="watch channel"
    )
    options = ap.parse_args()
    if options.method == 'router':
        app = Router(log=Logger('Router'))
    else:
        app = CLI(log=Logger("CLI"))
    app.bootstrap.run(**app.parse(options.watch))
    app.run(channel=options.watch)
