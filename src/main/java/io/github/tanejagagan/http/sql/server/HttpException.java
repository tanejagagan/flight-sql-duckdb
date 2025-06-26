package io.github.tanejagagan.http.sql.server;

abstract public class HttpException extends RuntimeException {
    public int errorCode;
    public HttpException(int errorCode, String msg) {
        super(msg);
        this.errorCode = errorCode;
    }
}
