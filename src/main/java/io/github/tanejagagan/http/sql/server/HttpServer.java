package io.github.tanejagagan.http.sql.server;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpHandlers;
import org.apache.arrow.memory.RootAllocator;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.github.tanejagagan.http.sql.server.Routers.*;

public class HttpServer {
    public static void main(String[] args) throws IOException {
        var notAllowedHandler = HttpHandlers.of(405, Headers.of("Allow", "POST"), "");
        RootAllocator allocator = new RootAllocator();
            com.sun.net.httpserver.HttpServer server = com.sun.net.httpserver.HttpServer.create(new InetSocketAddress(8080), 0);
            ExecutorService executor = Executors.newFixedThreadPool(10);
            server.setExecutor(executor);
            server.createContext("/",
                    HttpHandlers.handleOrElse(path("/query").and(post.or(get)).and(contentType(ContentTypes.APPLICATION_JSON)),
                            new QueryHandler(allocator), notAllowedHandler));
            server.start();
    }
}
