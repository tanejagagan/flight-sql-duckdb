package io.github.tanejagagan.flight.sql.server;

import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;

public class DuckDBSplittableFlightSqlProducer extends DuckDBFlightSqlProducer{
    public DuckDBSplittableFlightSqlProducer(Location location,
                                             String producerId,
                                             String secretKey,
                                             BufferAllocator allocator,
                                             String warehousePath) {
        super(location, producerId, secretKey, allocator, warehousePath);
    }

    @Override
    public FlightInfo getFlightInfoStatement(
            final FlightSql.CommandStatementQuery request,
            final CallContext context,
            final FlightDescriptor descriptor) {
        if (isSplittable(request)) {
            return super.getFlightInfoStatement(request,context, descriptor);
        } else {
            return super.getFlightInfoStatement(request, context, descriptor);
        }
    }

    public boolean isSplittable(FlightSql.CommandStatementQuery request) {
        return false;
    }

    @Override
    public void getStreamStatement(
            final FlightSql.TicketStatementQuery ticketStatementQuery,
            final CallContext context,
            final ServerStreamListener listener) {
        super.getStreamStatement(ticketStatementQuery, context, listener);
    }
}
