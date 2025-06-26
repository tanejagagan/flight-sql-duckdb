package io.github.tanejagagan.http.sql.server;

public class InternalErrorException extends HttpException {

    public InternalErrorException(int errorCode, String msg) {
        super(errorCode, msg);
    }
}
