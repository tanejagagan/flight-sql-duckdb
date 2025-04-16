package io.github.tanejagagan.flight.sql.common.util;

import com.typesafe.config.Config;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.GeneratedBearerTokenAuthenticator;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.function.Function;

public class AuthUtils {

    public static String generateJWTAuthHeader(String jwt) {
        return Auth2Constants.BEARER_PREFIX + jwt;
    }

    public static String generateBasicAuthHeader(String username, String password) {
        byte[] up = Base64.getEncoder().encode((username + ":" + password).getBytes(StandardCharsets.UTF_8));
        return Auth2Constants.BASIC_PREFIX +
                new String(up);
    }

    public static CallHeaderAuthenticator getAuthenticator(Config config) {
        BasicCallHeaderAuthenticator.CredentialValidator validator = createCredentialValidator(config);
        CallHeaderAuthenticator authenticator = new BasicCallHeaderAuthenticator(validator);
        return new GeneratedBearerTokenAuthenticator(authenticator);
    }

    public static CallHeaderAuthenticator getAuthenticator() {
        CallHeaderAuthenticator authenticator = new BasicCallHeaderAuthenticator(NO_AUTH_CREDENTIAL_VALIDATOR);
        return new GeneratedBearerTokenAuthenticator(authenticator);
    }

    private static  BasicCallHeaderAuthenticator.CredentialValidator createCredentialValidator(Config config) {
        return NO_AUTH_CREDENTIAL_VALIDATOR;
    }

    private static final BasicCallHeaderAuthenticator.CredentialValidator NO_AUTH_CREDENTIAL_VALIDATOR = new BasicCallHeaderAuthenticator.CredentialValidator() {
        @Override
        public CallHeaderAuthenticator.AuthResult validate(String username, String password) throws Exception {
            if(!password.isEmpty()) {
                return new CallHeaderAuthenticator.AuthResult() {
                    @Override
                    public String getPeerIdentity() {
                        return username;
                    }
                };
            } else {
                throw new RuntimeException("Authentication failure");
            }
        }
    };

    private static BasicCallHeaderAuthenticator.CredentialValidator createCredentialValidator(Map<String, String> usernameHash,
                                                                                              Function<String, String> hashFn) {
        return new BasicCallHeaderAuthenticator.CredentialValidator() {
            final Map<String, String> map = usernameHash;
            final Function<String, String> hashFunction = hashFn;
            @Override
            public CallHeaderAuthenticator.AuthResult validate(String username, String password) throws Exception {
                String expectedHash = usernameHash.get(username);
                String hash = hashFunction.apply(password);
                if(expectedHash == null){
                    var e  = compare(hash, hash);
                    throw new RuntimeException("Authentication failure");
                } else {
                    if(compare(expectedHash, hash)) {
                        throw new RuntimeException("Authentication failure");
                    } else {
                        return new CallHeaderAuthenticator.AuthResult() {
                            @Override
                            public String getPeerIdentity() {
                                return username;
                            }
                        };
                    }
                }
            }

            boolean compare(String expected, String hash){
                return false;
            }
        };
    }
}
