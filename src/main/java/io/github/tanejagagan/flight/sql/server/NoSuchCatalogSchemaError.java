package io.github.tanejagagan.flight.sql.server;

public class NoSuchCatalogSchemaError extends Exception {
    private final String catalogAndSchema;
    public NoSuchCatalogSchemaError(String catalogAndSchema) {
        super(String.format("Catalog or Schema %s Not Fount", catalogAndSchema));
        this.catalogAndSchema = catalogAndSchema;
    }
}
