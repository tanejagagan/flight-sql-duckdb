package io.github.tanejagagan.http.sql.server;

import com.sun.net.httpserver.Request;

import java.util.function.Predicate;

public class Routers {



    public static final Predicate<Request> post = r -> r.getRequestMethod().equals("POST");
    public static final Predicate<Request> get = r -> r.getRequestMethod().equals("GET");

    public static Predicate<Request> path(String path) {
        return r -> {
            var pathFromUri = r.getRequestURI().getPath();
            return pathFromUri.startsWith(path);
        };
    }

    public static Predicate<Request> contentType(String contentType) {
        return r -> {
            var headerValue = r.getRequestHeaders().get(Headers.CONTENT_TYPE);
            if(headerValue == null || headerValue.isEmpty()) {
                return false;
            }
            return headerValue.get(0).equals(contentType);
        };
    }
}
