package io.github.tanejagagan.http.sql.server;



public class BadRequestException extends HttpException {

    public BadRequestException(int errorCode, String msg) {
        super(errorCode, msg);
    }
}


