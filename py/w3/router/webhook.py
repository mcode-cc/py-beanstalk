# -*- coding: utf-8 -*-

from flask import Flask
from flask import request
app = Flask(__name__)

HOST = '0.0.0.0'
PORT = 5000
TEXT_HTML = 'Webhook server online! Go to <a href="https://bitbucket.com">Bitbucket</a> to configure your repository webhook for <a href="%s/webhook">%s/webhook</a>'


def _intro():
    print 'Webhook server online! Go to http://%s:%d' % (HOST, PORT)


def _html(req):
    return TEXT_HTML % (req.url_root, req.url_root)


@app.route('/', methods=['GET'])
def index():
    return _html(request)


@app.route('/webhook', methods=['GET', 'POST'])
def tracking():
    if request.method == 'POST':
        data = request.get_json()
        commit_author = data['actor']['username']
        commit_hash = data['push']['changes'][0]['new']['target']['hash'][:7]
        commit_url = data['push']['changes'][0]['new']['target']['links']['html']['href']
        print 'Webhook received! %s committed %s' % (commit_author, commit_hash)
        print commit_url
        return 'OK'
    else:
        return _html(request)


if __name__ == '__main__':
    _intro()
    app.run(host=HOST, port=PORT, debug=True)