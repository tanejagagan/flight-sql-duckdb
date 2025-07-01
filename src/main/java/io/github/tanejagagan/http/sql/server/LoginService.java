package io.github.tanejagagan.http.sql.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class LoginHandler implements HttpHandler {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Logger logger = LoggerFactory.getLogger(QueryHandler.class);

    @Override
    public void handle(HttpExchange exchange) throws IOException {

    }

    /**
     *
     * @param exchange
     * @throws IOException
     * match with username and password and return jwt token
     */
    private void handleInternal(HttpExchange exchange) throws IOException {

    }
}
