
package io.github.tanejagagan.http.sql.x;


import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.typesafe.config.ConfigFactory;
import io.helidon.config.Config;
import io.helidon.logging.common.LogConfig;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import org.apache.arrow.memory.RootAllocator;

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
    public static void main(String[] args) {
        
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


        WebServer server = WebServer.builder()
                .config(helidonConfig.get("flight-sql"))
                .routing(Main::routing)
                .port(port)
                .build()
                .start();


        System.out.println("WEB server is up! http://localhost:" + server.port() + "/simple-greet");

    }


    /**
     * Updates HTTP Routing.
     */
    static void routing(HttpRouting.Builder routing) {
        routing.register("query", new QueryService(new RootAllocator()));
    }
}