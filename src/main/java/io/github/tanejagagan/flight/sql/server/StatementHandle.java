package io.github.tanejagagan.flight.sql.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.github.tanejagagan.flight.sql.common.util.CryptoUtils;
import org.apache.arrow.flight.FlightDescriptor;

import javax.annotation.Nullable;
import java.io.IOException;

public record StatementHandle(String query, long queryId, String producerId,
                              @Nullable String queryChecksum) {

    public StatementHandle(String query, long queryId, String producerId){
        this(query, queryId, producerId, null);
    }


    private static final ObjectMapper objectMapper = new ObjectMapper();

    byte[] serialize() throws JsonProcessingException {
        return objectMapper.writeValueAsBytes(this);
    }

    public boolean isValid(String key) {
        return CryptoUtils.generateHMACSHA1(key, queryId + ":" + query).equals( queryChecksum);
    }

    public StatementHandle signed(String key) {
        String checksum = CryptoUtils.generateHMACSHA1(key, queryId + ":" + query);
        return new StatementHandle(this.query, this.queryId, this.producerId(), checksum);
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
