package io.github.tanejagagan.flight.sql.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.github.tanejagagan.sql.commons.Transformations;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static io.github.tanejagagan.flight.sql.server.DuckDBSplittableFlightSqlProducer.replaceFromClause;

public class DuckDBSplittableFlightSqlProducerTest {

    @Test
    public void testReplaceFromClause() throws SQLException, JsonProcessingException {
        replaceFromClause(Transformations.parseToTree("select * from read_parquet(['a', 'b'])"),
                "parquet",
                new String[]{"c", "d"});

        replaceFromClause(Transformations.parseToTree("select * from read_parquet('c')"),
                "parquet",
                new String[]{"c", "d"});

    }
}
