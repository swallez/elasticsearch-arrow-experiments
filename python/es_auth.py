import pyarrow.flight as flight
import base64


def basic(login, password):
    encoded = base64.b64encode((login + ':' + password).encode()).decode()
    return ESAuthHandler("Basic %s" % encoded)


def api_key(api_key):
    return ESAuthHandler("ApiKey %s" % api_key)


def token(token):
    return ESAuthHandler("Bearer %s" % token)


class ESAuthHandler(flight.ClientAuthHandler):

    def __init__(self, auth, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.auth = auth
        self.token = None

    def authenticate(self, outgoing, incoming):
        outgoing.write(self.auth)
        self.token = incoming.read()

    def get_token(self):
        return self.token
