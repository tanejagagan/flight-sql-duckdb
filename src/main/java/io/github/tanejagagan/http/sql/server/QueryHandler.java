package io.github.tanejagagan.http.sql.server;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.github.tanejagagan.flight.sql.common.Headers;
import io.github.tanejagagan.sql.commons.ConnectionPool;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class QueryHandler implements HttpHandler {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Logger logger = LoggerFactory.getLogger(QueryHandler.class);

    BufferAllocator allocator;

    public QueryHandler(BufferAllocator allocator) {
        this.allocator = allocator;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try {
            handleInternal(exchange);
        } catch (HttpException e) {
            if( e instanceof InternalErrorException  i) {
                logger.atError().setCause(e).log("Error");
            }
            var msg = e.getMessage().getBytes();
            try( var os = exchange.getResponseBody()) {
                exchange.sendResponseHeaders(e.errorCode, msg.length);
                os.write(msg);
                os.flush();
            }
        }
    }


    public void handleInternal(HttpExchange exchange) throws IOException {
        String query = null;
        if (exchange.getRequestMethod().equals("POST")) {
            var inputStream = exchange.getRequestBody();
            var queryObject = MAPPER.readValue(inputStream, QueryObject.class);
            query = queryObject.query();
        } else if (exchange.getRequestMethod().equals("GET")) {
            var uri = exchange.getRequestURI();
            var param = getQueryParameters(uri);
            query = param.get("q");
            if (query == null) {
                var msg = "Missing required parameter : q";
                throw new BadRequestException(400, msg);
            }
        }
        var headers = exchange.getRequestHeaders();
        int fetchSize = Headers.getValue(headers, Headers.HEADER_FETCH_SIZE,
                Headers.DEFAULT_ARROW_FETCH_SIZE, Integer.class);

        try (var connection = ConnectionPool.getConnection();
             var reader = ConnectionPool.getReader(connection, allocator, query, fetchSize);
             var vsr = reader.getVectorSchemaRoot();
             var os = exchange.getResponseBody();
             ArrowStreamWriter writer = new ArrowStreamWriter(vsr, null, Channels.newChannel(os))) {
            var respHeader = exchange.getResponseHeaders();
            respHeader.set(io.github.tanejagagan.http.sql.server.Headers.CONTENT_TYPE, ContentTypes.APPLICATION_ARROW);
            exchange.sendResponseHeaders(200, 0);
            writer.start();
            while (reader.loadNextBatch()) {
                writer.writeBatch();
            }
            writer.end();
        } catch (SQLException e) {
            throw new BadRequestException(400, e.getMessage());
        } catch (Exception e) {
            throw new InternalErrorException(500, e.getMessage());
        }
    }

    public static Map<String, String> getQueryParameters(URI uri) {
        Map<String, String> queryParams = new HashMap<>();
        String query = uri.getQuery();
        if (query != null && !query.isEmpty()) {
            String[] pairs = query.split("&");
            for (String pair : pairs) {
                int idx = pair.indexOf("=");
                String key = idx > 0 ? URLDecoder.decode(pair.substring(0, idx), StandardCharsets.UTF_8) : pair;
                String value = idx > 0 && pair.length() > idx + 1 ? URLDecoder.decode(pair.substring(idx + 1), StandardCharsets.UTF_8) : "";
                queryParams.put(key, value);
            }
        }
        return queryParams;
    }
}
