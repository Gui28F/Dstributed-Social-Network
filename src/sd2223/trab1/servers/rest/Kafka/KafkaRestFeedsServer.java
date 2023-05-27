package sd2223.trab1.servers.rest.Kafka;

import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.*;
import java.util.logging.Logger;

import sd2223.trab1.api.java.Feeds;
import sd2223.trab1.servers.Domain;
import sd2223.trab1.servers.rest.AbstractRestServer;
import utils.Args;

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
        org.slf4j.Logger kafkaLogger = LoggerFactory.getLogger("org.apache.kafka");

        // Set the logging level to a higher level (e.g., INFO)
        ((ch.qos.logback.classic.Logger) kafkaLogger).setLevel(ch.qos.logback.classic.Level.OFF);
        Args.use(args);
        Domain.set(args[0], 2);
        new KafkaRestFeedsServer().start();
    }
}
