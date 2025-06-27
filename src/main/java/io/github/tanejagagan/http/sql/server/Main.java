package io.github.tanejagagan.http.sql.server;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpHandlers;
import io.github.tanejagagan.sql.commons.ConnectionPool;
import org.apache.arrow.memory.RootAllocator;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.github.tanejagagan.http.sql.server.Routers.*;

public class Main {

    public static class Args {
        @Parameter(names = {"--conf"}, description = "Configurations" )
        private List<String> configs;
    }

    public static void main(String[] args) throws IOException, SQLException {
        var argv = new Args();

        JCommander.newBuilder()
                .addObject(argv)
                .build()
                .parse(args);
        var configMap = new HashMap<String, String>();
        if(argv.configs !=null) {
            argv.configs.forEach(c -> {
                var e = c.indexOf("=");
                var key = c.substring(0, e);
                var value = c.substring(e, c.length() - 1);
                configMap.put(key, value);
            });
        }
        var connection = ConnectionPool.getConnection();
        connection.close();
        var port = Integer.parseInt(configMap.getOrDefault("port", "8080"));
        startServer(port);
    }

    private static void startServer(int port) throws IOException {
        var notAllowedHandler = HttpHandlers.of(405, Headers.of("Allow", "POST"), "");
        RootAllocator allocator = new RootAllocator();
        com.sun.net.httpserver.HttpServer server = com.sun.net.httpserver.HttpServer.create(new InetSocketAddress(port), 0);
        ExecutorService executor = Executors.newFixedThreadPool(10);
        server.setExecutor(executor);
        server.createContext("/",
                HttpHandlers.handleOrElse(path("/query").and(
                                get.or(post.and(contentType(ContentTypes.APPLICATION_JSON)))),
                        new QueryHandler(allocator), notAllowedHandler));
        server.start();
    }
}
