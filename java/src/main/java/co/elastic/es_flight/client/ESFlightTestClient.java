package co.elastic.es_flight.client;

import co.elastic.es_flight.server.ESFlightServer;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.nio.charset.StandardCharsets;

public class ESFlightTestClient {

    public static void main(String[] args) throws Exception {

        Location location = new Location("grpc+tcp://localhost:33333");
        BufferAllocator allocator = new RootAllocator();

        FlightClient flightClient = FlightClient.builder(allocator, location)
            .intercept(ESClientAuthMiddleware.basic(ESFlightServer.login, ESFlightServer.password))
            .build();

//        flightClient.authenticate(ESClientAuthenticator.basic("elastic", ESFlightServer.password));

        FlightStream stream = flightClient.getStream(
            new Ticket("from employees |  stats count = count(id) by language".getBytes(StandardCharsets.UTF_8))
        );

        VectorSchemaRoot root = stream.getRoot();
        while (stream.next()) {
            System.out.print(root.contentToTSVString());
        }

        stream.close();
        flightClient.close();
    }
}
