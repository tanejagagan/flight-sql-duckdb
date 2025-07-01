package io.github.tanejagagan.http.sql.x;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import io.github.tanejagagan.flight.sql.common.Headers;
import io.github.tanejagagan.http.sql.server.*;
import io.github.tanejagagan.sql.commons.ConnectionPool;
import io.helidon.http.Header;
import io.helidon.http.HeaderName;
import io.helidon.http.HeaderNames;
import io.helidon.http.Status;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;

import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class QueryService  implements HttpService {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final BufferAllocator allocator;

    public QueryService(BufferAllocator allocator) {
        this.allocator = allocator;
    }
    @Override
    public void routing(HttpRules rules) {
        rules.get("/", this::handleGetX)
                .post("/", this::handlePostX);
    }



    public void handleGetX(ServerRequest request,
                           ServerResponse response) throws IOException {
        var query = request.requestedUri().query().get("q");
        handleY(request, response, query);
    }

    public void handleY(ServerRequest request,
                        ServerResponse response, String query) throws IOException {
        var fetchSizeHeader = request.headers().value(HeaderNames.create(io.github.tanejagagan.flight.sql.common.Headers.HEADER_FETCH_SIZE));
        int fetchSize = fetchSizeHeader.map(Integer::parseInt).orElse(Headers.DEFAULT_ARROW_FETCH_SIZE);
        try (var connection = ConnectionPool.getConnection();
             var reader = ConnectionPool.getReader(connection, allocator, query, fetchSize);
             var vsr = reader.getVectorSchemaRoot();
             var os = response.outputStream();
             ArrowStreamWriter writer = new ArrowStreamWriter(vsr, null, Channels.newChannel(os))) {
            var respHeaders = response.headers();
            respHeaders.set(HeaderNames.CONTENT_TYPE, ContentTypes.APPLICATION_ARROW);
            response.status(Status.OK_200);
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

    public void handlePostX(ServerRequest request,
                               ServerResponse response) throws IOException {
        var queryObject = MAPPER.readValue(request.content().inputStream(), QueryObject.class);
        handleY(request, response, queryObject.query());
    }

}
