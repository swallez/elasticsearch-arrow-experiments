package co.elastic.es_flight.client;

import org.apache.arrow.flight.auth.ClientAuthHandler;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Iterator;


/**
 * An authentication handler that is configured with the value of the Authorization header.
 */
public class ESClientAuthenticator implements ClientAuthHandler {

    public static ESClientAuthenticator basic(String login, String password) {
        String auth = Base64.getEncoder().encodeToString((login + ":" + password).getBytes(StandardCharsets.UTF_8));
        auth = "Basic " + auth;
        return new ESClientAuthenticator(auth);
    }

    public static ESClientAuthenticator apiKey(String apiKey) {
        return new ESClientAuthenticator("ApiKey " + apiKey);
    }

    public static ESClientAuthenticator bearer(String token) {
        return new ESClientAuthenticator("Bearer" + token);
    }

    private byte[] auth;
    private byte[] token;

    public ESClientAuthenticator(String auth) {
        this.auth = auth.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void authenticate(ClientAuthSender outgoing, Iterator<byte[]> incoming) {
        outgoing.send(auth);
        this.token = incoming.next();
        this.auth = null;
    }

    @Override
    public byte[] getCallToken() {
        return token;
    }
}
