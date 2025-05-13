package io.github.tanejagagan.flight.sql.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import org.apache.arrow.flight.FlightDescriptor;

import java.io.IOException;

public record StatementHandle(String query, long queryId, String producerId) {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    byte[] serialize() throws JsonProcessingException {
        return objectMapper.writeValueAsBytes(this);
    }

    public static StatementHandle deserialize(byte[] bytes) {
        try {
            return objectMapper.readValue(bytes, StatementHandle.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static StatementHandle deserialize(ByteString bytes) {
        return deserialize(bytes.toByteArray());
    }

    public static StatementHandle fromFlightDescriptor(FlightDescriptor flightDescriptor) {
        return deserialize(flightDescriptor.getCommand());
    }
}
