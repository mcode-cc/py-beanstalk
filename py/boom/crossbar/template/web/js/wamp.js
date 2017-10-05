var WAMP = function (subscribe) {

    this.subscribe = subscribe;
    if (document.location.protocol === "http:") {
        this.url = "ws:" + "//" + document.location.hostname + ':8080' + "/ws";
    } else {
        this.url = "wss:" + "//" + document.location.hostname + ':8443' + "/ws";
    }
    this.uid = '';
    this.token = '';

    this.onchallenge = function (session, method, extra) {
        console.log("onchallenge", method, extra);
        if (method === "ticket") {
            return that.token;
        } else {
            throw "Don't know how to authenticate using '" + method + "'";
        }
    };

    this.send = function (message) {
        this.connection.session.call('press.root.online.send', [message], {}, { disclose_me: true }).then(
            function (res) {
                console.log("Send result: ", res);
            },
            function (err) {
                console.log("Send error: ", err);
            }
        );
    };

    this.callback = function (data) {
        this.uid = data.uid;
        this.token = data.access_token;

        this.connection = new autobahn.Connection({
            url: this.url,
            realm: "press.root",
            authmethods: ["ticket"],
            authid: that.uid,
            onchallenge: this.onchallenge
        });

        this.connection.onopen = function (session, details) {

            console.log("Connected session with ID " + session.id);
            console.log("Authenticated using method '" + details.authmethod + "' and provider '" + details.authprovider + "'");
            console.log("Authenticated with uid '" + details.authid + "' and role '" + details.authrole + "'");

            for (var i = 0; i < subscribe.length; i++) {
                session.subscribe(subscribe[i].uri, subscribe[i].received).then(
                    function (sub) {
                        console.log('Subscribed to topic', sub);
                    },
                    function (err) {
                        console.log('Failed to subscribe to topic: ', err);
                    }
                );
            }
        };

        // fired when connection was lost (or could not be established)
        //
        this.connection.onclose = function (reason, details) {
            console.log("Disconnected", reason, details.reason, details);
        };

        this.connection.open();
    };

    var that = this;

    this.open = function () {
        console.log("Ok, loaded", autobahn.version);

        var body = document.getElementsByTagName('BODY')[0];
        var temp_name = 'wamp_auth', script = document.createElement('script');
        script.setAttribute('src', '//identity.kpcdn.net/auth/uid/' + temp_name + '/');
        body.appendChild(script);

        window[temp_name] = function (data) {
            body.removeChild(script);
            delete window[temp_name];
            that.callback(data);
        };
    }
};