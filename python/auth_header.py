import pyarrow.flight as flight
import base64


def basic(login, password):
    encoded = base64.b64encode((login + ':' + password).encode()).decode()
    return ESAuthMiddleware("Basic %s" % encoded)


def api_key(api_key):
    return ESAuthMiddleware("ApiKey %s" % api_key)


def token(token):
    return ESAuthMiddleware("Bearer %s" % token)


class ESAuthMiddleware(flight.ClientMiddlewareFactory, flight.ClientMiddleware):

    def __init__(self, auth, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.auth = auth

    def start_call(self, info):
        return self

    def sending_headers(self):
        return {"authorization": self.auth}
