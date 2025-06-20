package io.github.tanejagagan.flight.sql.common;

public class UnauthorizedException extends Throwable {
    final String msg;
    public UnauthorizedException(String msg) {
        this.msg = msg;
    }
}
