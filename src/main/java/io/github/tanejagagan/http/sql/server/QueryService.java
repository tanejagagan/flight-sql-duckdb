package io.github.tanejagagan.http.sql.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.tanejagagan.flight.sql.common.Headers;
import io.github.tanejagagan.sql.commons.ConnectionPool;
import io.helidon.http.HeaderNames;
import io.helidon.http.HeaderValues;
import io.helidon.http.Status;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.Channels;
import java.sql.SQLException;

public class QueryService implements HttpService {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Logger logger = LoggerFactory.getLogger(QueryService.class);
    private final BufferAllocator allocator;

    public QueryService(BufferAllocator allocator) {
        this.allocator = allocator;
    }

    @Override
    public void routing(HttpRules rules) {
        rules.get("/", this::handleGet)
                .post("/", this::handlePost);
    }

    private void handleGet(ServerRequest request,
                           ServerResponse response) {
        var query = request.requestedUri().query().get("q");
        handle(request, response, query);
    }

    public void handlePost(ServerRequest request,
                           ServerResponse response) throws IOException {
        var queryObject = MAPPER.readValue(request.content().inputStream(), QueryObject.class);
        handle(request, response, queryObject.query());
    }

    private void handle(ServerRequest request,
                        ServerResponse response, String query) {
        try {
            handleInternal(request, response, query);
        } catch (HttpException e) {
            if (e instanceof InternalErrorException) {
                logger.atError().setCause(e).log("Error");
            }
            var msg = e.getMessage().getBytes();
            response.status(e.errorCode);
            response.send(msg);
        }
    }

    private void handleInternal(ServerRequest request,
                                ServerResponse response, String query) {

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
}
