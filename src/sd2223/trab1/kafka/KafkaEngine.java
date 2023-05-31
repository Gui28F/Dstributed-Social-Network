package sd2223.trab1.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class KafkaEngine {
    public static final String POST_MESSAGE = "postMessageKafka";
    public static final String REMOVE_FROM_PERSONAL_FEED = "removeFromPersonalFeedKafka";
    public static final String SUB_USER = "subUserKafka";
    public static final String UNSUBSCRIBE_USER = "unsubscribeUserKafka";
    public static final String DELETE_USER_FEED = "deleteUserFeedKafka";
    private KafkaPublisher publisher;
    private static final String FROM_BEGINNING = "earliest";
    private static final String KAFKA_BROKERS = "kafka:9092";
    private static KafkaEngine impl;
    private KafkaEngine() {
        this.publisher = KafkaPublisher.createPublisher(KAFKA_BROKERS);
    }

    public static KafkaEngine getInstance() {
        if (impl == null)
            impl = new KafkaEngine();
        return impl;
    }

    public long send(String topic, Function msg) {
        long offset = publisher.publish(topic, msg);
        if (offset >= 0)
            System.out.println("Message published with sequence number: " + offset);
        else
            System.err.println("Failed to publish message");
        return offset;
    }
    public KafkaSubscriber createSubscriber(String topic) {
        return KafkaSubscriber.createSubscriber(KAFKA_BROKERS, List.of(topic), FROM_BEGINNING);
    }

}
