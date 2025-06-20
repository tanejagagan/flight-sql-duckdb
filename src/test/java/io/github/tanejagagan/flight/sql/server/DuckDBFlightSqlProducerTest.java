package io.github.tanejagagan.flight.sql.server;


import io.github.tanejagagan.flight.sql.common.FlightStreamReader;
import io.github.tanejagagan.flight.sql.common.Headers;
import io.github.tanejagagan.flight.sql.common.util.AuthUtils;
import io.github.tanejagagan.sql.commons.ConnectionPool;
import io.github.tanejagagan.sql.commons.util.TestUtils;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DuckDBFlightSqlProducerTest {
    protected static final String LOCALHOST = "localhost";
    private static final Logger logger = LoggerFactory.getLogger(DuckDBFlightSqlProducerTest.class);
    private static final String USER = "admin";
    private static final String PASSWORD = "password";
    private static final String TEST_CATALOG = "producer_test_catalog";
    private static final String TEST_SCHEMA = "test_schema";
    private static final String TEST_TABLE = "test_table";
    private static final BufferAllocator clientAllocator = new RootAllocator(Integer.MAX_VALUE);
    private static final BufferAllocator serverAllocator = new RootAllocator(Integer.MAX_VALUE);
    private static final String LONG_RUNNING_QUERY = "with t as " +
            "(select len(split(concat('abcdefghijklmnopqrstuvwxyz:', generate_series), ':')) as len  from generate_series(1, 1000000000) )" +
            " select count(*) from t where len = 10";
    protected static FlightServer flightServer;
    protected static FlightSqlClient sqlClient;
    protected static String warehousePath;

    @BeforeAll
    public static void beforeAll() throws Exception {
        Path tempDir = Files.createTempDirectory("duckdb_" + DuckDBFlightSqlProducerTest.class.getName());
        warehousePath = Files.createTempDirectory("duckdb_warehouse_" + DuckDBFlightSqlProducerTest.class.getName()).toString();
        String[] sqls = {
                String.format("ATTACH '%s/file.db' AS %s", tempDir.toString(), TEST_CATALOG),
                String.format("USE %s", TEST_CATALOG),
                String.format("CREATE SCHEMA %s", TEST_SCHEMA),
                String.format("USE %s.%s", TEST_CATALOG, TEST_SCHEMA),
                String.format("CREATE TABLE %s (key string, value string)", TEST_TABLE),
                String.format("INSERT INTO %s VALUES ('k1', 'v1'), ('k2', 'v2')", TEST_TABLE)
        };
        ConnectionPool.executeBatch(sqls);
        setUpClientServer();
    }

    @AfterAll
    public static void afterAll() {
        clientAllocator.close();
    }

    private static void setUpClientServer() throws Exception {
        final Location serverLocation = Location.forGrpcInsecure(LOCALHOST, 55556);
        flightServer = FlightServer.builder(
                        serverAllocator,
                        serverLocation,
                        new DuckDBFlightSqlProducer(serverLocation,
                                UUID.randomUUID().toString(),
                                "change me",
                                serverAllocator, warehousePath))
                .headerAuthenticator(AuthUtils.getAuthenticator())
                .build()
                .start();
        sqlClient = new FlightSqlClient(FlightClient.builder(clientAllocator, serverLocation)
                .intercept(AuthUtils.createClientMiddlewareFactory(USER,
                        PASSWORD,
                        Map.of(Headers.HEADER_DATABASE, TEST_CATALOG,
                                Headers.HEADER_SCHEMA, TEST_SCHEMA)))
                .build());

    }


    @ParameterizedTest
    @ValueSource(strings = {"SELECT * FROM generate_series(10)",
            "SELECT * from " + TEST_CATALOG + "." + TEST_SCHEMA + "." + TEST_TABLE
    })
    public void testSimplePreparedStatementResults(String query) throws Exception {
        try (final FlightSqlClient.PreparedStatement preparedStatement =
                     sqlClient.prepare(query)) {
            try (final FlightStream stream =
                         sqlClient.getStream(preparedStatement.execute().getEndpoints().get(0).getTicket())) {
                TestUtils.isEqual(query, clientAllocator, FlightStreamReader.of(stream, clientAllocator));
            }

            // Read Again
            try (final FlightStream stream =
                         sqlClient.getStream(preparedStatement.execute().getEndpoints().get(0).getTicket())) {
                TestUtils.isEqual(query, clientAllocator, FlightStreamReader.of(stream, clientAllocator));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"SELECT * FROM generate_series(10)",
            "SELECT * from " + TEST_CATALOG + "." + TEST_SCHEMA + "." + TEST_TABLE
    })
    public void testStatement(String query) throws Exception {
        final FlightInfo flightInfo = sqlClient.execute(query);
        try (final FlightStream stream =
                     sqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket())) {
            TestUtils.isEqual(query, clientAllocator, FlightStreamReader.of(stream, clientAllocator));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"SELECT * FROM generate_series(" + DuckDBFlightSqlProducer.DEFAULT_ARROW_BATCH_SIZE * 3 + ")"
    })
    public void testStatementMultiBatch(String query) throws Exception {
        final FlightInfo flightInfo = sqlClient.execute(query);
        try (final FlightStream stream =
                     sqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket())) {
            TestUtils.isEqual(query, clientAllocator, FlightStreamReader.of(stream, clientAllocator));
        }
    }

    @Test
    public void testStatementSplittableHive() throws Exception {
        String[][] hivePartition = {
                {"dt", "date"},
                {"p", "string"}
        };
        String query = "select count(*) from read_parquet('example/hive_table')";
        try( var splittableClient = splittableClient(hivePartition)){
            var flightCallHeaders = new FlightCallHeaders();
            flightCallHeaders.insert(Headers.HEADER_SPLIT_SIZE, "1");
            var flightInfo = splittableClient.execute(query, new HeaderCallOption(flightCallHeaders));
            assertEquals(3, flightInfo.getEndpoints().size());
            for (var endpoint : flightInfo.getEndpoints()){
                try (final FlightStream stream = splittableClient.getStream(endpoint.getTicket(), new HeaderCallOption(flightCallHeaders))){
                    while (stream.next()){
                        stream.getRoot().contentToTSVString();
                    }
                }
            }
        }
    }

    @Test
    public void testStatementSplittableDelta() throws Exception {
        String[][] hivePartition = {
                {"dt", "date"},
                {"p", "string"}
        };
        String query = "select count(*) from read_parquet('example/delta_table')";
        try( var splittableClient = splittableClient(hivePartition)){
            var flightCallHeaders = new FlightCallHeaders();
            flightCallHeaders.insert(Headers.HEADER_SPLIT_SIZE, "1");
            var flightInfo = splittableClient.execute(query, new HeaderCallOption(flightCallHeaders));
            assertEquals(17, flightInfo.getEndpoints().size());
            for (var endpoint : flightInfo.getEndpoints()){
                try (final FlightStream stream = splittableClient.getStream(endpoint.getTicket(), new HeaderCallOption(flightCallHeaders))){
                    while (stream.next()){
                        stream.getRoot().contentToTSVString();
                    }
                }
            }
        }
    }

    @Test
    public void testBadStatement() throws Exception {
        String query = "SELECT x FROM generate_series(10)";
        final FlightInfo flightInfo = sqlClient.execute(query);
        try (final FlightStream stream =
                     sqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket())) {
            stream.next();
            throw new RuntimeException("It should not come here");
        } catch (FlightRuntimeException flightRuntimeException){
            // All good. Its expected to have this exception
        }
    }

    @Test
    public void testCancelQuery() throws SQLException {
        try (Connection connection = ConnectionPool.getConnection();
             Statement statement = connection.createStatement()) {
            Thread thread = new Thread(() -> {
                try {
                    Thread.sleep(200);
                    statement.cancel();
                } catch (InterruptedException | SQLException e) {
                    throw new RuntimeException(e);
                }
            });
            thread.start();
            try {
                statement.execute(LONG_RUNNING_QUERY);
                // It should not reach here. Expected to throw exception
            } catch (Exception e) {
                // Nothing to do
            }
        }
    }

    @Test
    public void testCancelRemoteStatement() throws Exception {
        final FlightInfo flightInfo = sqlClient.execute(LONG_RUNNING_QUERY);
        Thread thread = new Thread(() -> {
            try {
                Thread.sleep(200);
                sqlClient.cancelFlightInfo(new CancelFlightInfoRequest(flightInfo));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        thread.start();
        try (final FlightStream stream =
                     sqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket())) {
            while (stream.next()) {
                // It should now reach here
                throw new RuntimeException("Cancellation failed");
            }
        } catch (Exception e) {
            // Expected. Ignore it
        }
    }

    @Test
    public void testGetCatalogsResults() throws Exception {
        String expectedSql = "select distinct(database_name) as TABLE_CAT from duckdb_columns() order by database_name";
        try (final FlightStream stream =
                     sqlClient.getStream(sqlClient.getCatalogs().getEndpoints().get(0).getTicket())) {
            TestUtils.isEqual(expectedSql, clientAllocator, FlightStreamReader.of(stream, clientAllocator));
        }
    }

    @Test
    public void testGetTablesResultNoSchema() throws Exception {
        try (final FlightStream stream =
                     sqlClient.getStream(
                             sqlClient.getTables(null, null, null,
                                     null, false).getEndpoints().get(0).getTicket())) {
            int count = 0;
            while (stream.next()) {
                count += stream.getRoot().getRowCount();
            }
            assertEquals(1, count);
        }
    }                                                                                                                                          

    @Test
    public void testGetSchema() throws Exception {
        try (final FlightStream stream = sqlClient.getStream(
                sqlClient.getSchemas(null, null).getEndpoints().get(0).getTicket())) {
            int count = 0;
            while (stream.next()) {
                count += stream.getRoot().getRowCount();
            }
            assertEquals(7, count);
        }
    }

    @Test
    public void putStream() throws Exception {
        testPutStream("test_123.parquet");
    }

    @Test
    public void putStreamWithError() throws Exception {
        testPutStream("test_456.parquet");
        try {
            testPutStream("test_456.parquet");
        } catch (Exception e ){
          // Exception is expected.
        }
    }

    @Test
    public void testSetFetchSize() throws Exception {
        String query = "select * from generate_series(100)";
        var flightCallHeader = new FlightCallHeaders();
        flightCallHeader.insert(Headers.HEADER_FETCH_SIZE, Integer.toString(10));
        HeaderCallOption callOption = new HeaderCallOption(flightCallHeader);
        final FlightInfo flightInfo = sqlClient.execute(query, callOption);
        int batches = 0;
        try (final FlightStream stream =
                     sqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket(), callOption)) {
            while (stream.next()) {
                batches ++;
            }
            assertEquals(11, batches);
        }
    }

    @Test
    // These are disabled because the issue with driver which
    // does not let me troubleshoot the issue as it's a uber jar
    // and lines do not match.
    public void testMetadata() throws SQLException {
        try(Connection connection = getConnection()) {
            var databaseMetadata = connection.getMetaData();
            //databaseMetadata.getTables();
            try (var rs = databaseMetadata.getSchemas()) {
                while(rs.next()) {
                    System.out.printf("%s, %s\n",
                            rs.getString(1),
                            rs.getString(2));
                }
            }

            //databaseMetadata.getCatalogs();
            try (var rs = databaseMetadata.getCatalogs()) {
                while(rs.next()) {
                    System.out.printf("%s\n",
                            rs.getString(1));
                }
            }

            try( var rs = databaseMetadata.getTables(null, null, null, null)){
                while(rs.next()) {
                    System.out.printf("%s",
                            rs.getString(1));
                }
            }
        }
    }

    private void testPutStream(String filename) throws SQLException, IOException {
        String query = "select * from generate_series(10)";
        try(DuckDBConnection connection = ConnectionPool.getConnection();
            var reader = ConnectionPool.getReader( connection, clientAllocator, query, 1000 )) {
            var streamReader = new ArrowReaderWrapper(reader, clientAllocator);
            var executeIngestOption = new FlightSqlClient.ExecuteIngestOptions("",
                    FlightSql.CommandStatementIngest.TableDefinitionOptions.newBuilder().build(),
                    false, "", "", Map.of("path", filename));
            sqlClient.executeIngest(streamReader, executeIngestOption);
        }
    }

    private static Connection getConnection() throws SQLException {
        String url = String.format("jdbc:arrow-flight-sql://localhost:%s/?database=memory&useEncryption=0&user=%s&password=%s&retainAuth=true", flightServer.getPort(), USER, PASSWORD );
        return DriverManager.getConnection(url);
    }

    static class ArrowReaderWrapper extends ArrowStreamReader {
        ArrowReader arrowReader;
        public ArrowReaderWrapper(ArrowReader reader, BufferAllocator allocator){
            super((InputStream) new ByteArrayInputStream(new byte[0]), allocator);
            this.arrowReader = reader;
        }

        @Override
        protected Schema readSchema() throws IOException {
            return arrowReader.getVectorSchemaRoot().getSchema();
        }
        @Override
        public VectorSchemaRoot getVectorSchemaRoot() throws IOException {
            return arrowReader.getVectorSchemaRoot();
        }

        @Override
        public boolean loadNextBatch() throws IOException {
            return arrowReader.loadNextBatch();
        }
    }

    private FlightSqlClient splittableClient(String[][] hivePartition) {
        final Location serverLocation = Location.forGrpcInsecure(LOCALHOST, 55556);
        var partitionSchemaString = Arrays.stream(hivePartition).map(strings -> strings[0] + " " + strings[1]).collect(Collectors.joining(","));
        return new FlightSqlClient(FlightClient.builder(clientAllocator, serverLocation)
                .intercept(AuthUtils.createClientMiddlewareFactory(USER,
                        PASSWORD,
                        Map.of(Headers.HEADER_DATABASE, TEST_CATALOG,
                                Headers.HEADER_SCHEMA, TEST_SCHEMA,
                                Headers.HEADER_PARALLELIZE, "true",
                                Headers.HEADER_PARTITION_DATA_TYPES, partitionSchemaString)))
                .build());
    }
}
