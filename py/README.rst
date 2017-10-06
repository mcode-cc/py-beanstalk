Boom.Router
=====


>>> from w3.router.boom import Router


    mta = Client()
    msg = mta({"test": "Какой то текст на русском языке"}, subscribe="press.root.subscribe.notify")
    # print(msg.as_dict())
    msg.send(tube="inbox")
    mta.queue.watch(DEFAULT_TUBE)
    print(mta.reserve(timeout=0, drop=True))