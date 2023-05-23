package sd2223.trab1.servers.rest.Kafka;

import org.glassfish.jersey.server.ResourceConfig;
import sd2223.trab1.api.java.Feeds;
import sd2223.trab1.servers.Domain;
import sd2223.trab1.servers.rest.AbstractRestServer;
import utils.Args;

import java.util.logging.Level;
import java.util.logging.Logger;

public class KafkaRestFeedsServer extends AbstractRestServer {
    public static final int PORT = 6567;

    private static Logger Log = Logger.getLogger(KafkaRestFeedsServer.class.getName());


    KafkaRestFeedsServer() {
        super(Log, Feeds.SERVICENAME, PORT);
    }

    @Override
    protected void registerResources(ResourceConfig config) {
        String secret = Args.valueOf("-secret", "EMPTY");
        config.register(Args.valueOf("-push", true) ? new KafkaRestFeedsPushResource(secret) : new KafkaRestFeedsPullResource(secret));
    }

    public static void main(String[] args) throws Exception {
        Args.use(args);
        Domain.set(args[0], Long.valueOf(args[1]));
        Log.setLevel(Level.INFO);
        new KafkaRestFeedsServer().start();
    }
}
