package io.github.tanejagagan.flight.sql.server.auth2;

import com.google.common.base.Strings;
import io.grpc.Metadata;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightRuntimeExceptionFactory;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.flight.auth2.AuthUtilities;
import org.apache.arrow.flight.auth2.BearerTokenAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.arrow.flight.grpc.MetadataAdapter;

import javax.crypto.SecretKey;
import java.util.Calendar;
import java.util.Date;

public class GeneratedJWTTokenAuthenticator extends BearerTokenAuthenticator {
    private final SecretKey key;
    private final int timeMinutes;
    JwtParser jwtParser;

    public GeneratedJWTTokenAuthenticator(CallHeaderAuthenticator initialAuthenticator, SecretKey key, int timeoutMinutes) {
        super(initialAuthenticator);
        this.key = key;
        this.jwtParser = Jwts.parser()     // (1)
                .verifyWith(key)      //     or a constant key used to verify all signed JWTs
                .build();
        this.timeMinutes = timeoutMinutes;
    }

    @Override
    protected AuthResult getAuthResultWithBearerToken(AuthResult authResult) {

        // We generate a dummy header and call appendToOutgoingHeaders with it.
        // We then inspect the dummy header and parse the bearer token if present in the header
        // and generate a new bearer token if a bearer token is not present in the header.
        final CallHeaders dummyHeaders = new MetadataAdapter(new Metadata());
        authResult.appendToOutgoingHeaders(dummyHeaders);
        String bearerToken =
                AuthUtilities.getValueFromAuthHeader(dummyHeaders, Auth2Constants.BEARER_PREFIX);
        final AuthResult authResultWithBearerToken;
        if (Strings.isNullOrEmpty(bearerToken)) {
            Calendar expiration = Calendar.getInstance();
            expiration.add(Calendar.MINUTE, timeMinutes);
            String jwt = Jwts.builder()
                    .subject(authResult.getPeerIdentity())
                    .expiration(expiration.getTime())
                    .signWith(key).compact();
            authResultWithBearerToken =
                    new AuthResult() {
                        @Override
                        public String getPeerIdentity() {
                            return authResult.getPeerIdentity();
                        }

                        @Override
                        public void appendToOutgoingHeaders(CallHeaders outgoingHeaders) {
                            authResult.appendToOutgoingHeaders(outgoingHeaders);
                            outgoingHeaders.insert(
                                    Auth2Constants.AUTHORIZATION_HEADER, Auth2Constants.BEARER_PREFIX + jwt);
                        }
                    };
        } else {
            // Use the bearer token supplied by the original auth result.
            authResultWithBearerToken = authResult;
        }
        return authResultWithBearerToken;
    }

    @Override
    protected AuthResult validateBearer(String bearerToken) {
        try {
            var jwt = jwtParser.parseSignedClaims(bearerToken);
            var payload = jwt.getPayload();
            var subject = payload.getSubject();
            var expiration = payload.getExpiration();
            if (expiration.before(new Date())) {
                throw FlightRuntimeExceptionFactory.of(new CallStatus(CallStatus.UNAUTHENTICATED.code(), null, "Expired", null));
            }
            return () -> subject;
        } catch (Exception e) {
            throw CallStatus.UNAUTHENTICATED.toRuntimeException();
        }
    }
}
