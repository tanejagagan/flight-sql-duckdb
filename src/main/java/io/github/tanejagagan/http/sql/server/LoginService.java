package io.github.tanejagagan.http.sql.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import io.github.tanejagagan.flight.sql.common.util.AuthUtils;
import io.helidon.http.Status;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import io.jsonwebtoken.Jwts;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.time.Duration;
import java.util.Calendar;

public class LoginService implements HttpService {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Logger logger = LoggerFactory.getLogger(LoginService.class);
    private final Config config;
    private final SecretKey secretKey;
    private final BasicCallHeaderAuthenticator.CredentialValidator authenticator;
    private final Duration expiration;

    public LoginService(Config config, SecretKey secretKey) {
        this.config = config;
        this.authenticator = AuthUtils.createCredentialValidator(config);
        this.secretKey = secretKey;
        this.expiration = config.getDuration("jwt.token.expiration");
    }
    @Override
    public void routing(HttpRules rules) {
        rules.post("/", this::handleLogin);
    }

    private void handleLogin(ServerRequest serverRequest, ServerResponse serverResponse) throws IOException {
        var inputStream = serverRequest.content().inputStream();
        var loginRequest = MAPPER.readValue(inputStream, LoginObject.class);
        try {
            authenticator.validate(loginRequest.username(), loginRequest.password());
            Calendar expiration = Calendar.getInstance();
            expiration.add(Calendar.MINUTE,
                    (int)this.expiration.toMinutes());
            String jwt = Jwts.builder()
                    .subject(loginRequest.username())
                    .expiration(expiration.getTime())
                    .signWith(secretKey).compact();
            serverResponse.send(jwt.getBytes());
        } catch (Exception e ){
            serverResponse.status(Status.UNAUTHORIZED_401);
            serverResponse.send();
        }
    }
}
