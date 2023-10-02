package co.elastic.es_flight.client;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightClientMiddleware;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class ESClientAuthMiddleware implements FlightClientMiddleware.Factory, FlightClientMiddleware {

    public static ESClientAuthMiddleware basic(String login, String password) {
        String auth = Base64.getEncoder().encodeToString((login + ":" + password).getBytes(StandardCharsets.UTF_8));
        return new ESClientAuthMiddleware("Basic " + auth);
    }

    private final String auth;

    public ESClientAuthMiddleware(String auth) {
        this.auth = auth;
    }

    @Override
    public FlightClientMiddleware onCallStarted(CallInfo info) {
        return this;
    }

    @Override
    public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
        outgoingHeaders.insert("authorization", auth);
    }

    @Override
    public void onHeadersReceived(CallHeaders incomingHeaders) {
    }

    @Override
    public void onCallCompleted(CallStatus status) {
    }
}
