package io.github.tanejagagan.flight.sql.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.protobuf.*;
import io.github.tanejagagan.flight.sql.common.FlightStreamReader;
import io.github.tanejagagan.flight.sql.common.Headers;
import io.github.tanejagagan.sql.commons.ConnectionPool;
import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.FlightSqlUtils;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.protobuf.Any.pack;
import static com.google.protobuf.ByteString.copyFrom;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.Objects.isNull;
import static org.duckdb.DuckDBConnection.DEFAULT_SCHEMA;

/**
 * It's a simple implementation which support most of the construct for reading as well as bulk writing to parquet file.
 * For now only property which is supported is database, schema and fetch size which are supplied as the connection parameter
 * and available in the header. More options will be supported in the future version.
 * Future implementation note for statement we check if its SET or RESET statement and based on that use cookies to set unset the values
 */
public class DuckDBFlightSqlProducer implements FlightSqlProducer, AutoCloseable {

    protected static final Calendar DEFAULT_CALENDAR = JdbcToArrowUtils.getUtcCalendar();

    private static final int DEFAULT_ARROW_BATCH_SIZE = 10000;
    private final static Logger logger = LoggerFactory.getLogger(DuckDBFlightSqlProducer.class);
    private final Location location;
    private final String producerId;
    private final BufferAllocator allocator;
    private final ExecutorService executorService = Executors.newFixedThreadPool(10);
    private final String warehousePath;
    private static AtomicLong sqlIdCounter = new AtomicLong();

    private final Cache<Long, StatementContext<PreparedStatement>> preparedStatementLoadingCache;
    private final Cache<Long, StatementContext<Statement>> statementLoadingCache;

    public DuckDBFlightSqlProducer(Location location){
        this(location, UUID.randomUUID().toString());
    }

    public DuckDBFlightSqlProducer(Location location, String producerId) {
        this(location, producerId, new RootAllocator(),  System.getProperty("user.dir") + "/warehouse");
    }

    public DuckDBFlightSqlProducer(Location location,
                                   String producerId,
                                   BufferAllocator allocator,
                                   String warehousePath) {
        this.location = location;
        this.producerId = producerId;
        this.allocator = allocator;
        preparedStatementLoadingCache =
                CacheBuilder.newBuilder()
                        .maximumSize(4000)
                        .expireAfterAccess(10, TimeUnit.MINUTES)
                        .removalListener(new StatementRemovalListener<PreparedStatement>())
                        .build();
        statementLoadingCache =
                CacheBuilder.newBuilder()
                        .maximumSize(4000)
                        .expireAfterWrite(10, TimeUnit.MINUTES)
                        .removalListener(new StatementRemovalListener<>())
                        .build();
        this.warehousePath = warehousePath;
    }

    @Override
    public void createPreparedStatement(FlightSql.ActionCreatePreparedStatementRequest request, final CallContext context, StreamListener<Result> listener) {
        // Running on another thread
        final Connection connection;
        try {
            connection = getConnection(context);
        } catch (NoSuchCatalogSchemaError e) {
            handleNoSuchDBSchema(listener, e);
            return;
        }

        StatementHandle handle = newStatementHandle(request.getQuery());
        Future<?> unused =
                executorService.submit(
                        () -> {
                            try {
                                final ByteString serializedHandle =
                                        copyFrom(handle.serialize());
                                // Ownership of the connection will be passed to the context. Do NOT close!

                                final PreparedStatement preparedStatement =
                                        connection.prepareStatement(
                                                request.getQuery(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                                final StatementContext<PreparedStatement> preparedStatementContext =
                                        new StatementContext<>(preparedStatement, request.getQuery());
                                preparedStatementLoadingCache.put(
                                        handle.queryId(), preparedStatementContext);

                                final Schema parameterSchema =
                                        JdbcToArrowUtils.jdbcToArrowSchema(preparedStatement.getParameterMetaData(), DEFAULT_CALENDAR);

                                final ResultSetMetaData metaData = preparedStatement.getMetaData();
                                final ByteString bytes =
                                        isNull(metaData)
                                                ? ByteString.EMPTY
                                                : ByteString.copyFrom(
                                                serializeMetadata(JdbcToArrowUtils.jdbcToArrowSchema(metaData, DEFAULT_CALENDAR)));
                                final FlightSql.ActionCreatePreparedStatementResult result =
                                        FlightSql.ActionCreatePreparedStatementResult.newBuilder()
                                                .setDatasetSchema(bytes)
                                                .setParameterSchema(copyFrom(serializeMetadata(parameterSchema)))
                                                .setPreparedStatementHandle(serializedHandle)
                                                .build();
                                listener.onNext(new Result(pack(result).toByteArray()));
                            } catch (final SQLException e) {
                                listener.onError(
                                        CallStatus.INTERNAL
                                                .withDescription("Failed to create prepared statement: " + e)
                                                .toRuntimeException());
                                return;
                            } catch (final Throwable t) {
                                listener.onError(
                                        CallStatus.INTERNAL
                                                .withDescription("Unknown error: " + t)
                                                .toRuntimeException());
                                return;
                            }
                            listener.onCompleted();
                        });

    }

    @Override
    public void closePreparedStatement(FlightSql.ActionClosePreparedStatementRequest request, CallContext context, StreamListener<Result> listener) {
        // Running on another thread
        Future<?> unused =
                executorService.submit(
                        () -> {
                            try {
                                StatementHandle statementHandle = StatementHandle.deserialize(request.getPreparedStatementHandle());
                                preparedStatementLoadingCache.invalidate(statementHandle.queryId());
                            } catch (final Exception e) {
                                listener.onError(e);
                                return;
                            }
                            listener.onCompleted();
                        });
    }


    @Override
    public FlightInfo getFlightInfoPreparedStatement(
            final FlightSql.CommandPreparedStatementQuery command,
            final CallContext context,
            final FlightDescriptor descriptor) {

        StatementHandle statementHandle = StatementHandle.deserialize(command.getPreparedStatementHandle());
        StatementContext<PreparedStatement> statementContext =
                preparedStatementLoadingCache.getIfPresent(statementHandle.queryId());
        if (statementContext == null) {
            handleContextNotFound();
        }
        return getFlightInfoForSchema(command, descriptor, null);
    }

    @Override
    public FlightInfo getFlightInfoStatement(
            final FlightSql.CommandStatementQuery request,
            final CallContext context,
            final FlightDescriptor descriptor) {
        StatementHandle handle = newStatementHandle(request.getQuery());
        try {
            final ByteString serializedHandle =
                    copyFrom(handle.serialize());

            // Ownership of the connection will be passed to the context. Do NOT close!
            final Connection connection ;
            try {
                connection = getConnection(context);
            } catch (NoSuchCatalogSchemaError e) {
                throw FlightRuntimeExceptionFactory.of(new CallStatus(CallStatus.INVALID_ARGUMENT.code(),null, e.getMessage(), null));
            }
            final Statement statement =
                    connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            final String query = request.getQuery();
            final StatementContext<Statement> statementContext = new StatementContext<>(statement, query);
            statementLoadingCache.put(handle.queryId(), statementContext);
            FlightSql.TicketStatementQuery ticket =
                    FlightSql.TicketStatementQuery.newBuilder().setStatementHandle(serializedHandle).build();
            return getFlightInfoForSchema(
                    ticket, descriptor, null);
        } catch (final SQLException  | JsonProcessingException e) {
            logger.error(
                    format("There was a problem executing the prepared statement: <%s>.", e.getMessage()), e);
            throw CallStatus.INTERNAL.withCause(e).toRuntimeException();
        }
    }

    @Override
    public SchemaResult getSchemaStatement(FlightSql.CommandStatementQuery command, CallContext context,
                                           FlightDescriptor descriptor) {
        return null;
    }


    @Override
    public void getStreamPreparedStatement(FlightSql.CommandPreparedStatementQuery command, CallContext context,
                                           ServerStreamListener listener) {
        StatementHandle statementHandle = StatementHandle.deserialize(command.getPreparedStatementHandle());
        StatementContext<PreparedStatement> statementContext =
            preparedStatementLoadingCache.getIfPresent(statementHandle.queryId());
        if (statementContext == null) {
            handleContextNotFound();
        }
        final PreparedStatement statement = statementContext.getStatement();
        streamResultSet(() -> (DuckDBResultSet) statement.executeQuery(),
            allocator, getBatchSize(context),
            listener);
    }

    @Override
    public void getStreamStatement(
            final FlightSql.TicketStatementQuery ticketStatementQuery,
            final CallContext context,
            final ServerStreamListener listener) {
        StatementHandle statementHandle = StatementHandle.deserialize(ticketStatementQuery.getStatementHandle());
        try  {
            final StatementContext<Statement> statementContext =
                    statementLoadingCache.getIfPresent(statementHandle.queryId());
            if (statementContext == null) {
                handleContextNotFound();
            }
            Statement statement = statementContext.getStatement();
            streamResultSet(() -> {
                statement.execute(statementContext.getQuery());
                return (DuckDBResultSet) statement.getResultSet();
            }, allocator, getBatchSize(context), listener);
        } finally {
            statementLoadingCache.invalidate(statementHandle.queryId());
        }
    }

    @Override
    public Runnable acceptPutStatement(FlightSql.CommandStatementUpdate command, CallContext context,
                                       FlightStream flightStream, StreamListener<PutResult> ackStream) {
        return null;
    }

    @Override
    public Runnable acceptPutPreparedStatementUpdate(FlightSql.CommandPreparedStatementUpdate command,
                                                     CallContext context, FlightStream flightStream,
                                                     StreamListener<PutResult> ackStream) {
        return null;
    }

    @Override
    public Runnable acceptPutPreparedStatementQuery(FlightSql.CommandPreparedStatementQuery command,
                                                    CallContext context, FlightStream flightStream,
                                                    StreamListener<PutResult> ackStream) {
        return null;
    }

    @Override
    public Runnable acceptPutStatementBulkIngest(
            FlightSql.CommandStatementIngest command,
            CallContext context,
            FlightStream flightStream,
            StreamListener<PutResult> ackStream) {
        Map<String, String > optionMap = command.getOptionsMap();
        String path = optionMap.get("path");
        final String completePath = warehousePath + "/" + path;
        String format = optionMap.getOrDefault("format", "parquet");
        String partitionColumnString = optionMap.get("partition");
        List<String> partitionColumns;
        if(partitionColumnString != null) {
            partitionColumns = Arrays.stream(partitionColumnString.split(",")).toList();
        } else {
            partitionColumns = List.of();
        }

        return () -> {
            FlightStreamReader reader = FlightStreamReader.of(flightStream, allocator);
            try {
                ConnectionPool.bulkIngestToFile(reader, allocator, completePath, partitionColumns, format);
                ackStream.onNext(PutResult.empty());
            } catch (SQLException e) {
                ackStream.onError(e);
            } finally {
                ackStream.onCompleted();
            }
        };
    }

    @Override
    public void cancelFlightInfo(
            CancelFlightInfoRequest request, CallContext context, StreamListener<CancelStatus> listener) {
        Ticket ticket = request.getInfo().getEndpoints().get(0).getTicket();
        final Any command;
        try {
            command = Any.parseFrom(ticket.getBytes());
        } catch (InvalidProtocolBufferException e) {
            listener.onError(e);
            return;
        }
        if (command.is(FlightSql.TicketStatementQuery.class)) {
            cancelStatement(
                    FlightSqlUtils.unpackOrThrow(command, FlightSql.TicketStatementQuery.class), context, listener);
        } else if (command.is(FlightSql.CommandPreparedStatementQuery.class)) {
            cancelPreparedStatement(
                    FlightSqlUtils.unpackOrThrow(command, FlightSql.CommandPreparedStatementQuery.class),
                    context,
                    listener);
        }
    }

    private void cancelStatement(final FlightSql.TicketStatementQuery ticketStatementQuery,
                                 CallContext context,
                                 StreamListener<CancelStatus> listener) {
        StatementHandle statementHandle = StatementHandle.deserialize(ticketStatementQuery.getStatementHandle());
        cancel(statementHandle, listener);
    }

    private void cancelPreparedStatement(FlightSql.CommandPreparedStatementQuery ticketPreparedStatementQuery,
                                         CallContext context,
                                         StreamListener<CancelStatus> listener) {
        final StatementHandle statementHandle = StatementHandle.deserialize(ticketPreparedStatementQuery.getPreparedStatementHandle());
        cancel(statementHandle, listener);
    }

    private void cancel(StatementHandle statementHandle,
                        StreamListener<CancelStatus> listener) {
        try {
            StatementContext<Statement> statementContext =
                    statementLoadingCache.getIfPresent(statementHandle.queryId());
            if (statementContext == null) {
                handleContextNotFound(listener);
                return;
            }

            Statement statement = statementContext.getStatement();
            listener.onNext(CancelStatus.CANCELLING);
            try {
                statement.cancel();
            } catch (SQLException e) {
                listener.onError(FlightRuntimeExceptionFactory.of(CallStatus.INTERNAL));
                logger.atError().setCause(e).log("error canceling");
            }
            listener.onNext(CancelStatus.CANCELLED);
        } finally {
            listener.onCompleted();
            statementLoadingCache.invalidate(statementHandle.queryId());
        }
    }

    @Override
    public FlightInfo getFlightInfoSqlInfo(FlightSql.CommandGetSqlInfo request,
                                           CallContext context, FlightDescriptor descriptor) {
        return null;
    }

    @Override
    public void getStreamSqlInfo(FlightSql.CommandGetSqlInfo command,
                                 CallContext context, ServerStreamListener listener) {
        throwUnimplemented("getStreamSqlInfo");
    }

    @Override
    public FlightInfo getFlightInfoTypeInfo(FlightSql.CommandGetXdbcTypeInfo request,
                                            CallContext context, FlightDescriptor descriptor) {
        return null;
    }

    @Override
    public void getStreamTypeInfo(FlightSql.CommandGetXdbcTypeInfo request, CallContext context,
                                  ServerStreamListener listener) {
        throwUnimplemented("getStreamTypeInfo");
    }

    @Override
    public FlightInfo getFlightInfoCatalogs(
            final FlightSql.CommandGetCatalogs request,
            final CallContext context,
            final FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor, Schemas.GET_CATALOGS_SCHEMA);
    }

    @Override
    public void getStreamCatalogs(final CallContext context, final ServerStreamListener listener) {
        streamResultSet(DuckDBDatabaseMetadataUtil::getCatalogs, context, allocator, listener);
    }

    @Override
    public FlightInfo getFlightInfoSchemas(FlightSql.CommandGetDbSchemas request, CallContext context,
                                           FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor, Schemas.GET_SCHEMAS_SCHEMA);
    }

    @Override
    public void getStreamSchemas(FlightSql.CommandGetDbSchemas command, CallContext context, ServerStreamListener listener) {
        final String catalog = command.hasCatalog() ? command.getCatalog() : null;
        final String schemaFilterPattern =
                command.hasDbSchemaFilterPattern() ? command.getDbSchemaFilterPattern() : null;
        streamResultSet(connection ->
                        DuckDBDatabaseMetadataUtil.getSchemas(connection, catalog, schemaFilterPattern),
                context, allocator, listener);
    }

    @Override
    public FlightInfo getFlightInfoTables(
            final FlightSql.CommandGetTables request,
            final CallContext context,
            final FlightDescriptor descriptor) {
        Schema schemaToUse = Schemas.GET_TABLES_SCHEMA;
        if (!request.getIncludeSchema()) {
            schemaToUse = Schemas.GET_TABLES_SCHEMA_NO_SCHEMA;
        }
        return getFlightInfoForSchema(request, descriptor, schemaToUse);
    }

    @Override
    public void getStreamTables(
            final FlightSql.CommandGetTables command,
            final CallContext context,
            final ServerStreamListener listener) {

        final String catalog = command.hasCatalog() ? command.getCatalog() : null;
        final String schemaFilterPattern =
                command.hasDbSchemaFilterPattern() ? command.getDbSchemaFilterPattern() : null;
        final String tableFilterPattern =
                command.hasTableNameFilterPattern() ? command.getTableNameFilterPattern() : null;
        final ProtocolStringList protocolStringList = command.getTableTypesList();
        final int protocolSize = protocolStringList.size();
        final String[] tableTypes =
                protocolSize == 0 ? null : protocolStringList.toArray(new String[protocolSize]);
        streamResultSet(connection ->
            DuckDBDatabaseMetadataUtil.getTables(connection, catalog, schemaFilterPattern, tableFilterPattern, tableTypes),
                context, allocator, listener);
    }

    @Override
    public FlightInfo getFlightInfoTableTypes(FlightSql.CommandGetTableTypes request, CallContext context,
                                              FlightDescriptor descriptor) {
        return null;
    }

    @Override
    public void getStreamTableTypes(CallContext context, ServerStreamListener listener) {
        throwUnimplemented("getStreamTableTypes");
    }

    @Override
    public FlightInfo getFlightInfoPrimaryKeys(FlightSql.CommandGetPrimaryKeys request, CallContext context,
                                               FlightDescriptor descriptor) {
        return null;
    }

    @Override
    public void getStreamPrimaryKeys(FlightSql.CommandGetPrimaryKeys command, CallContext context,
                                     ServerStreamListener listener) {
        throwUnimplemented("getStreamPrimaryKeys");
    }

    @Override
    public FlightInfo getFlightInfoExportedKeys(FlightSql.CommandGetExportedKeys request, CallContext context,
                                                FlightDescriptor descriptor) {
        return null;
    }

    @Override
    public FlightInfo getFlightInfoImportedKeys(FlightSql.CommandGetImportedKeys request, CallContext context,
                                                FlightDescriptor descriptor) {
        return null;
    }

    @Override
    public FlightInfo getFlightInfoCrossReference(FlightSql.CommandGetCrossReference request, CallContext context,
                                                  FlightDescriptor descriptor) {
        return null;
    }

    @Override
    public void getStreamExportedKeys(FlightSql.CommandGetExportedKeys command, CallContext context,
                                      ServerStreamListener listener) {
        throwUnimplemented("getStreamExportedKeys");
    }

    @Override
    public void getStreamImportedKeys(FlightSql.CommandGetImportedKeys command, CallContext context,
                                      ServerStreamListener listener) {
        throwUnimplemented("getStreamImportedKeys");
    }

    @Override
    public void getStreamCrossReference(FlightSql.CommandGetCrossReference command, CallContext context,
                                        ServerStreamListener listener) {
        throwUnimplemented("getStreamCrossReference");
    }

    @Override
    public void close() throws Exception {
        AutoCloseables.close(this.allocator);
    }

    @Override
    public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
        throwUnimplemented("listFlights");
    }

    private void throwUnimplemented(String name) {
        logger.info("Unimplemented method error {}", name);
        throw CallStatus.UNIMPLEMENTED.toRuntimeException();
    }

    private static class StatementRemovalListener<T extends Statement>
            implements RemovalListener<Long, StatementContext<T>> {
        @Override
        public void onRemoval(final RemovalNotification<Long, StatementContext<T>> notification) {
            try {
                assert notification.getValue() != null;
                notification.getValue().close();
            } catch (final Exception e) {
                // swallow
            }
        }
    }

    private static ByteBuffer serializeMetadata(final Schema schema) {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            MessageSerializer.serialize(new WriteChannel(Channels.newChannel(outputStream)), schema);
            return ByteBuffer.wrap(outputStream.toByteArray());
        } catch (final IOException e) {
            throw new RuntimeException("Failed to serialize schema", e);
        }
    }

    protected <T extends Message> FlightInfo getFlightInfoForSchema(
            final T request, final FlightDescriptor descriptor, final Schema schema) {
        final Ticket ticket = new Ticket(pack(request).toByteArray());
        // TODO Support multiple endpoints.
        final List<FlightEndpoint> endpoints = singletonList(new FlightEndpoint(ticket, location));
        return new FlightInfo(schema, descriptor, endpoints, -1, -1);
    }

    private static DuckDBConnection getConnection(final CallContext context) throws NoSuchCatalogSchemaError {
        CallHeaders headers = context.getMiddleware(FlightConstants.HEADER_KEY).headers();
        String database = headers.get(Headers.HEADER_DATABASE);
        String schema = headers.get(Headers.HEADER_SCHEMA);
        if (schema == null) {
            schema = DEFAULT_SCHEMA;
        }
        String dbSchema = String.format("%s.%s", database, schema);
        String[] sqls = {String.format("USE %s", dbSchema)};
        try {
            return ConnectionPool.getConnection(sqls);
        } catch (Exception e ){
            throw new NoSuchCatalogSchemaError(dbSchema);
        }
    }

    private static int getBatchSize(final CallContext context) {
        CallHeaders headers = context.getMiddleware(FlightConstants.HEADER_KEY).headers();
        String batchSize = headers.get(Headers.HEADER_FETCH_SIZE);
        if(batchSize == null) {
            return DEFAULT_ARROW_BATCH_SIZE;
        } else {
            return Integer.parseInt(batchSize);
        }
    }

    private interface ResultSetSupplierFromConnection {
        DuckDBResultSet get(DuckDBConnection connection) throws SQLException;
    }

    private static void streamResultSet(ResultSetSupplierFromConnection supplier,
                                        CallContext context,
                                        BufferAllocator allocator,
                                        final ServerStreamListener listener) {

        try (DuckDBConnection connection = getConnection(context)) {
            streamResultSet(() -> supplier.get(connection), allocator, getBatchSize(context), listener);
        } catch (SQLException e) {
            listener.error(e);
            logger.atError().setCause(e).log("Error getting connection");
        } catch (NoSuchCatalogSchemaError e) {

        }
    }

    private interface ResultSetSupplier {
        DuckDBResultSet get() throws SQLException;
    }

    private static void streamResultSet(ResultSetSupplier supplier,
                                        BufferAllocator allocator,
                                        final int batchSize,
                                        final ServerStreamListener listener) {
        try (DuckDBResultSet resultSet = supplier.get();
             ArrowReader reader = (ArrowReader) resultSet.arrowExportStream(allocator, batchSize)) {
            listener.start(reader.getVectorSchemaRoot());
            while (reader.loadNextBatch()) {
                listener.putNext();
            }
        } catch (IOException | SQLException e) {
            CallStatus callStatus = new CallStatus(FlightStatusCode.INVALID_ARGUMENT, e, e.getMessage(), null);
            listener.error(FlightRuntimeExceptionFactory.of(callStatus));
        } finally {
            listener.completed();
        }
    }

    private static void streamResultSet(String sql,
                                        BufferAllocator allocator,
                                        CallContext context,
                                        final ServerStreamListener listener){
        try (DuckDBConnection connection = getConnection(context);
          var statement =  connection.createStatement()) {
            streamResultSet(() -> (DuckDBResultSet) statement.getResultSet(), allocator, getBatchSize(context), listener);
        } catch (SQLException e) {
            listener.error(e);
            logger.atError().setCause(e).log("Error getting connection");
        } catch (NoSuchCatalogSchemaError e) {
            handleNoSuchDBSchema(listener, e);
        }
    }

    private StatementHandle newStatementHandle(String query) {
        return new StatementHandle(query, sqlIdCounter.incrementAndGet(), producerId);
    }

    private static void handleNoSuchDBSchema(ServerStreamListener listener, NoSuchCatalogSchemaError exception){
        listener.error(FlightRuntimeExceptionFactory.of(new CallStatus(FlightStatusCode.INVALID_ARGUMENT, null, exception.getMessage(), null)));
        listener.completed();
    }

    private static void handleNoSuchDBSchema(StreamListener<Result> listener, NoSuchCatalogSchemaError exception){
        listener.onError(FlightRuntimeExceptionFactory.of(new CallStatus(FlightStatusCode.INVALID_ARGUMENT, null, exception.getMessage(), null)));
        listener.onCompleted();
    }

    private static void handleContextNotFound() {
        throw FlightRuntimeExceptionFactory.of(CallStatus.NOT_FOUND);
    }
    private static void handleContextNotFound(StreamListener<CancelStatus> listener) {
        listener.onError(FlightRuntimeExceptionFactory.of(CallStatus.NOT_FOUND));
    }
}
