# -*- coding: utf-8 -*-

from flask import Flask
from flask import request
import json

app = Flask(__name__)

HOST = '127.0.0.1'
PORT = 5555
TEXT_HTML = 'Finding mixed content with content security policy. ' \
            '<a href="%s/security/reporting/">%s/security/reporting/</a>'


def _intro():
    print 'Finding mixed content! Go to http://%s:%d' % (HOST, PORT)


def _html(req):
    return TEXT_HTML % (req.url_root, req.url_root)


@app.route('/', methods=['GET'])
def index():
    return _html(request)


@app.route('/reporting/', methods=['GET', 'POST'])
def tracking():
    if request.method == 'POST':
        data = request.get_json()
        data_json = json.dumps(
                data,
                indent=4,
                ensure_ascii=False
            )
        print data_json
        return 'OK'
    else:
        return _html(request)


if __name__ == '__main__':
    _intro()
    app.run(host=HOST, port=PORT, debug=True)