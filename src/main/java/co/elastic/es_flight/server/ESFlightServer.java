package co.elastic.es_flight.server;

import co.elastic.es_flight.esql.ESQLQuery;
import co.elastic.es_flight.esql.ESQLResponse;
import co.elastic.es_flight.client.ESFlightTestClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.RequestContext;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.commons.configuration2.INIConfiguration;

import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

// Don't forget JVM option --add-opens=java.base/java.nio=ALL-UNNAMED

public class ESFlightServer implements AutoCloseable {

    public static final String password;
    public static final String login;
    public static final String url;

    static {
        INIConfiguration iniConfiguration = new INIConfiguration();
        try (FileReader fileReader = new FileReader("config.ini")) {
            iniConfiguration.read(fileReader);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        var esConfig = iniConfiguration.getSection("elasticsearch");
        url = esConfig.getString("url");
        login = esConfig.getString("login");
        password = esConfig.getString("password");
    }

    public static void main(String[] args) throws Exception {
        System.out.println(Location.forGrpcInsecure("0.0.0.0", 33333).getUri());

        ESFlightServer server = new ESFlightServer(
            URI.create(url)
        );

        server.start();
        System.out.println("Started");

        ESFlightTestClient.main(null);

        // Wait forever
        new CompletableFuture<Void>().get();
    }

    private final HttpClient hc;

    private final BufferAllocator allocator;
    private final FlightServer flightServer;
    private final URI esUrl;

    public ESFlightServer(URI esUrl) throws Exception {

        this.hc = HttpClient
            .newBuilder()
            .sslContext(SSLUtils.yesSSLContext())
            .build();

        this.esUrl = esUrl;

        this.allocator = new RootAllocator();

        var producer = new Producer();

        this.flightServer = FlightServer.builder()
            .allocator(allocator)
            .location(new Location("grpc+tcp://0.0.0.0:33333/"))
            .middleware(CallInfoMiddleware.KEY, CallInfoMiddleware.FACTORY)
            .authHandler(new ESServerAuthHandler())
            .producer(producer)
            .build();
    }

    public void start() throws IOException {
        this.flightServer.start();
    }

    @Override
    public void close() throws Exception {
        flightServer.close();
        allocator.close();
    }

    /**
     * The Arrow Flight request handler.
     */
    public class Producer extends NoOpFlightProducer {

        private final ObjectMapper mapper = new ObjectMapper();
        private final URI queryUrl = ESFlightServer.this.esUrl.resolve("_query");


        @Override
        public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
        }

        @Override
        public void listActions(CallContext context, StreamListener<ActionType> listener) {
            listener.onNext(new ActionType("esql", "Execute an ES|QL query"));
            listener.onCompleted();
        }

        @Override
        public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {

            // Auth: check Flight auth token first, then the request's Authorization header.
            String auth = context.peerIdentity();
            if (auth == null || auth.isEmpty()) {
                CallInfoMiddleware callInfo = context.getMiddleware(CallInfoMiddleware.KEY);
                auth = callInfo.incomingHeaders.get("authorization");
            }

            if (auth == null) {
                listener.error(new StatusRuntimeException(Status.UNAUTHENTICATED));
                return;
            }

            // FIXME: should have a prefix to distinguish a set of commands
            String query = new String(ticket.getBytes(), StandardCharsets.UTF_8);

            try {
                // Prepare the http request
                var esqlQuery = new ESQLQuery(query);
                byte[] bytes = mapper.writeValueAsBytes(esqlQuery);

                HttpRequest request = HttpRequest
                    .newBuilder(queryUrl)
                    .header("Content-Type", "application/json")
                    .header("Authorization", auth)
                    .POST(HttpRequest.BodyPublishers.ofByteArray(bytes))
                    .build();

                // Send request
                HttpResponse<byte[]> response = hc.send(request, HttpResponse.BodyHandlers.ofByteArray());

                // Parse response
                if (response.statusCode() != 200) {
                    throw new RuntimeException("Elasticsearch returned error " + response.statusCode());
                }

                ESQLResponse esqlResponse = mapper.readValue(response.body(), ESQLResponse.class);

                // Convert to Arrow
                esqlResponse.toArrow(allocator, listener);


            } catch (Exception e) {
                e.printStackTrace();
                listener.error(e);
            }
        }
    }

    /**
     * Make call information available to Flight request handlers.
     */
    public static class CallInfoMiddleware implements FlightServerMiddleware {

        private final CallInfo info;
        private final CallHeaders incomingHeaders;
        private final RequestContext context;

        public CallInfoMiddleware(CallInfo info, CallHeaders incomingHeaders, RequestContext context) {
            this.info = info;
            this.incomingHeaders = incomingHeaders;
            this.context = context;
        }

        @Override
        public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
        }

        @Override
        public void onCallCompleted(CallStatus status) {
        }

        @Override
        public void onCallErrored(Throwable err) {
        }

        private static class Factory implements FlightServerMiddleware.Factory<CallInfoMiddleware> {
            @Override
            public CallInfoMiddleware onCallStarted(CallInfo info, CallHeaders incomingHeaders, RequestContext context) {
                return new CallInfoMiddleware(info, incomingHeaders, context);
            }
        }

        public static final FlightServerMiddleware.Factory<CallInfoMiddleware> FACTORY = new Factory();
        public static final FlightServerMiddleware.Key<CallInfoMiddleware> KEY = FlightServerMiddleware.Key.of("CallInfoMiddleware");
    }
}
