package io.github.tanejagagan.flight.sql.server;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.github.tanejagagan.flight.sql.common.util.AuthUtils;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.io.IOException;

public class Main {

    public static final String CONFIG_PATH = "flight-sql-duckdb";

    public static void main(String[] args) {
        Config config = ConfigFactory.load().getConfig(CONFIG_PATH);
        int port = config.getInt("port");
        String host = config.getString("host");
        Location location = Location.forGrpcInsecure(host, port);
        CallHeaderAuthenticator authenticator = AuthUtils.getAuthenticator(config);
        try (BufferAllocator allocator = new RootAllocator()){
             FlightServer flightServer = FlightServer.builder(
                             allocator,
                             location,
                             new DuckDBFlightSqlProducer(location))
                     .headerAuthenticator(authenticator)
                     .build();

            Thread severThread = new Thread(() -> {
                try {
                    flightServer.start();
                    System.out.println("S1: Server (Location): Listening on port " + flightServer.getPort());
                    flightServer.awaitTermination();
                } catch (IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            severThread.start();
        }
    }
}
