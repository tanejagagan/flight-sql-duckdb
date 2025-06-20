package io.github.tanejagagan.flight.sql.common;

import org.apache.arrow.flight.FlightConstants;
import org.apache.arrow.flight.FlightProducer;

import java.util.Map;
import java.util.UnknownFormatConversionException;
import java.util.function.Function;

public class Headers {

    private static final Map<Class<?>, Function<String, Object>> EXTRACTOR = Map.of(
            Integer.class, Integer::parseInt,
            Long.class, Long::parseLong,
            Boolean.class, Boolean::parseBoolean,
            String.class, a -> a
    );
    public static final String HEADER_FETCH_SIZE = "fetch_size";
    public static final String HEADER_DATABASE = "database";
    public static final String HEADER_SCHEMA = "schema";
    public static final String HEADER_SPLIT_SIZE = "split_size";
    public static final String HEADER_PARALLELIZE = "parallelize";
    public static final String HEADER_PARTITION_DATA_TYPES = "partition_data_types";
    public static final String HEADER_DATA_SCHEMA = "data_schema";

    public static <T> T getValue(FlightProducer.CallContext context, String key,  T defaultValue, Class<T> tClass) {
        var header =  context.getMiddleware(FlightConstants.HEADER_KEY);
        var fromHeaderString  = header.headers().get(key);
        if(fromHeaderString == null) {
            return defaultValue;
        }
        var fn = EXTRACTOR.get(tClass);
        if(fn == null) {
            throw new UnknownFormatConversionException(tClass.getName());
        }
        return (T) fn.apply(fromHeaderString);
    }
}
