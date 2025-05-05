package io.github.tanejagagan.flight.sql.server;


public interface SqlValidator {

    ValidationResult validate(String query);

    interface ValidationResult { }

    class Ok implements ValidationResult { }

    record Error(String msg) implements ValidationResult { }

    Ok OK = new Ok();

    SqlValidator ALL_VALID = query -> OK;

    SqlValidator DATASOURCE_ONLY = query -> {
        throw new RuntimeException("Not implemented");
    };
}
