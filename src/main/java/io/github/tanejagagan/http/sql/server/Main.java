
package io.github.tanejagagan.http.sql.server;


import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.typesafe.config.ConfigFactory;
import io.github.tanejagagan.flight.sql.common.util.AuthUtils;
import io.helidon.config.Config;
import io.helidon.logging.common.LogConfig;
import io.helidon.webserver.WebServer;
import org.apache.arrow.memory.RootAllocator;

import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;

import static io.github.tanejagagan.flight.sql.server.Main.CONFIG_PATH;


/**
 * The application main class.
 */
public class Main {


    /**
     * Cannot be instantiated.
     */
    private Main() {
    }

    public static class Args {
        @Parameter(names = {"--conf"}, description = "Configurations" )
        private List<String> configs;
    }


    /**
     * Application main entry point.
     * @param args command line arguments.
     */
    public static void main(String[] args) throws NoSuchAlgorithmException {
        
        // load logging configuration
        LogConfig.configureRuntime();

        // initialize global config from default configuration
        Config helidonConfig = Config.create();

        var argv = new Args();

        JCommander.newBuilder()
                .addObject(argv)
                .build()
                .parse(args);
        var configMap = new HashMap<String, String>();
        if(argv.configs !=null) {
            argv.configs.forEach(c -> {
                var e = c.split("=");
                var key = e[0];
                var value = e[1];
                configMap.put(key, value);
            });
        }

        var commandlineConfig = ConfigFactory.parseMap(configMap);
        var appConfig = commandlineConfig.withFallback(ConfigFactory.load().getConfig(CONFIG_PATH));
        var port = Integer.parseInt(appConfig.getString("port"));
        var auth = appConfig.hasPath("auth") ? appConfig.getString("auth") : null;
        var secretKey = AuthUtils.generateRandoSecretKey();
        WebServer server = WebServer.builder()
                .config(helidonConfig.get("flight-sql"))
                .routing(routing -> {
                    var b = routing.register("/query", new QueryService(new RootAllocator()))
                            .register("/login", new LoginService(appConfig, secretKey));
                    if ("jwt".equals(auth)) {
                        b.addFilter(new JwtAuthenticationFilter("/query", appConfig, secretKey));
                    }
                })
                .port(port)
                .build()
                .start();

        var builder = new StringBuilder();
        String url = "http://localhost:" + server.port();
        String msg = "WEB server is up! " + url;
        builder.append(msg);
        System.out.println(builder.toString());
    }
}