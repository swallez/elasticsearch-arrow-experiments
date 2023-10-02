package co.elastic.es_flight.server;

import org.apache.arrow.flight.auth.ServerAuthHandler;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Optional;

/**
 * An auth handler that just returns the information provided by the client, that will later be found
 * as CallContext.peerIdentity().
 */
public class ESServerAuthHandler implements ServerAuthHandler {
    @Override
    public Optional<String> isValid(byte[] token) {
        if (token == null) {
            // Empty string prevents rejection as unauthenticated and will trigger fallback to
            // authenticating using the request's Authorization header, if present.
            return Optional.of("");
        } else {
            return Optional.of(new String(token, StandardCharsets.UTF_8));
        }
    }

    @Override
    public boolean authenticate(ServerAuthSender outgoing, Iterator<byte[]> incoming    ) {
        byte[] token = incoming.next();
        outgoing.send(token);
        return true;
    }
}
