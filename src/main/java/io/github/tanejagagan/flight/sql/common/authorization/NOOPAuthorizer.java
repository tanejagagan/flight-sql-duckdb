package io.github.tanejagagan.flight.sql.common.authorization;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.tanejagagan.flight.sql.common.UnauthorizedException;
import io.github.tanejagagan.flight.sql.server.SqlAuthorizer;

public class NOOPAuthorizer implements SqlAuthorizer {
    @Override
    public JsonNode authorize(String user, String database, String schema, JsonNode query) throws UnauthorizedException {
        return query;
    }
}
