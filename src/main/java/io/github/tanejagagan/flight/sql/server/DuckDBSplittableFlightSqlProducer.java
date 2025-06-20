package io.github.tanejagagan.flight.sql.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import io.github.tanejagagan.flight.sql.common.Headers;
import io.github.tanejagagan.sql.commons.ExpressionFactory;
import io.github.tanejagagan.sql.commons.FileStatus;
import io.github.tanejagagan.sql.commons.Transformations;
import io.github.tanejagagan.sql.commons.planner.SplitPlanner;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.google.protobuf.Any.pack;
import static com.google.protobuf.ByteString.copyFrom;
import static io.github.tanejagagan.sql.commons.ExpressionFactory.createFunction;

public class DuckDBSplittableFlightSqlProducer extends DuckDBFlightSqlProducer {

    public static long DEFAULT_SPLIT_SIZE = 128 * 1024 * 1024;

    public static long DEFAULT_AGGREGATE_SPLIT_SIZE = 1024 * 1024 * 1024;

    public DuckDBSplittableFlightSqlProducer(Location location,
                                             String producerId,
                                             String secretKey,
                                             BufferAllocator allocator,
                                             String warehousePath) {
        super(location, producerId, secretKey, allocator, warehousePath);
    }


    public FlightInfo getFlightInfoStatementSplittable(
            final FlightSql.CommandStatementQuery request,
            final CallContext context,
            final FlightDescriptor descriptor) {
        var partitionDatatype = getPartitionDatatype(context);
        var query = request.getQuery();
        try {
            var tree = Transformations.parseToTree(query);
            var splitSize = getSplitSize(tree, context);
            var splits = SplitPlanner.getSplits(tree,
                    partitionDatatype, splitSize);
            var list = splits.stream().map(split -> {
                var copy = tree.deepCopy();
                replaceFromClause(copy, "read_parquet", split.stream().map(FileStatus::fileName).toArray(String[]::new));
                try {
                    var sql = Transformations.parseToSql(copy);
                    StatementHandle handle = newStatementHandle(sql);
                    final ByteString serializedHandle =
                            copyFrom(handle.serialize());
                    return FlightSql.TicketStatementQuery.newBuilder().setStatementHandle(serializedHandle).build();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }).toList();
            return getFlightInfoForSchema(list, descriptor, null, getLocation());
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected <T extends Message> FlightInfo getFlightInfoForSchema(
            final List<T> requests, final FlightDescriptor descriptor,
            final Schema schema, Location location) {
        var endpoints = requests.stream().map( request ->  {
            var ticket = new Ticket(pack(request).toByteArray());
            return new FlightEndpoint(ticket, location);
        }).toList();
        return new FlightInfo(schema, descriptor, endpoints, -1, -1);
    }


    private boolean parallelize(CallContext context) {
        return Headers.getValue(context, Headers.HEADER_PARALLELIZE, false, Boolean.class);
    }

    @Override
    public void getStreamStatement(
            final FlightSql.TicketStatementQuery ticketStatementQuery,
            final CallContext context,
            final ServerStreamListener listener) {

    }

    public static void replaceFromClause(JsonNode tree, String format, String[] paths) {
        var formatToFunction = Map.of("parquet", "read_parquet", "json", "read_json", "csv", "read_csv");
        var functionName = formatToFunction.get(format);
        var from = (ObjectNode) Transformations.getFirstStatementNode(tree).get("from_table");
        var listChildren = new ArrayNode(JsonNodeFactory.instance);
        for (String path : paths) {
            listChildren.add(ExpressionFactory.constant(path));
        }
        var listFunction = createFunction("list_value", "main", "", listChildren);
        var parquetChildren = new ArrayNode(JsonNodeFactory.instance);
        parquetChildren.add(listFunction);
        var readParquetFunction = createFunction(functionName, "",  "",  parquetChildren );
        from.set("function", readParquetFunction);
    }

    private static boolean hasAggregation(JsonNode jsonNode) {
        return false;
    }

    private static long getSplitSize(JsonNode jsonNode, CallContext callContext) {
        return Headers.getValue(callContext, Headers.HEADER_SPLIT_SIZE, DEFAULT_SPLIT_SIZE, Long.class);
    }



    private static String[][] getPartitionDatatype(CallContext context) {
        var value = Headers.getValue(context, Headers.HEADER_PARTITION_DATA_TYPES, null, String.class);
        if(value == null) {
            return new String[0][0];
        } else {
            return parserDataType(value);
        }
    }

    private static String[][] parserDataType(String datatypeString) {
        return Arrays.stream(datatypeString.split(","))
                .map( d -> {
                 var splits = d.split(" ");
                 var res = new String[2];
                 res[0] = splits[0].trim();
                 res[1] = splits[1].trim();
                 return res;
                }).toArray(String[][]::new);
    }
}
