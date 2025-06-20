package io.github.tanejagagan.flight.sql.server;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.tanejagagan.flight.sql.common.UnauthorizedException;

public interface SqlAuthorizer {
    JsonNode authorize(String user, String database, String schema, JsonNode query) throws UnauthorizedException;
}
