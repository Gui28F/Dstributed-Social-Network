package sd2223.trab1.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class KafkaEngine {
    public static final String POST_MESSAGE = "postMessageKafka";
    public static final String REMOVE_FROM_PERSONAL_FEED = "removeFromPersonalFeedKafka";
    public static final String GET_MESSAGE = "getMessageKafka";
    public static final String GET_MESSAGES = "getMessagesKafka";
    public static final String SUB_USER = "subUserKafka";
    public static final String UNSUBSCRIBE_USER = "unsubscribeUserKafka";
    public static final String LIST_SUBS = "listSubsKafka";
    public static final String DELETE_USER_FEED = "deleteUserFeedKafka";
    public static final String PULL_GET_TIME_FILTERED_PERSONAL_FEED = "pull_getTimeFilteredPersonalFeedKafka";
    public static final String PUSH_PUSH_MESSAGE = "push_PushMessageKafka";
    public static final String PUSH_UPDATE_FOLLOWERS = "push_updateFollowersKafka";
    public static final String DELETE_FROM_USER_FEED = "deleteFromUserFeedKafka";
    private KafkaPublisher publisher;
    private static final String FROM_BEGINNING = "earliest";
    private static final String KAFKA_BROKERS = "kafka:9092";
    private String[] topics = {POST_MESSAGE, REMOVE_FROM_PERSONAL_FEED, GET_MESSAGE, GET_MESSAGES, SUB_USER, UNSUBSCRIBE_USER,
            LIST_SUBS, DELETE_USER_FEED, PULL_GET_TIME_FILTERED_PERSONAL_FEED, PUSH_PUSH_MESSAGE, PUSH_UPDATE_FOLLOWERS,
            DELETE_FROM_USER_FEED};
    static final String TOPIC = "topic";
    private static KafkaEngine impl;

    private KafkaEngine() {
        this.publisher = KafkaPublisher.createPublisher(KAFKA_BROKERS);
    }

    public static KafkaEngine getInstance() {
        if (impl == null)
            impl = new KafkaEngine();
        return impl;
    }

    public long send(Function msg) {
        long offset = publisher.publish(TOPIC, msg);
        if (offset >= 0)
            System.out.println("Message published with sequence number: " + offset);
        else
            System.err.println("Failed to publish message");
        return offset;
    }

    public KafkaSubscriber createSubscriber() {
        return KafkaSubscriber.createSubscriber(KAFKA_BROKERS, List.of(TOPIC), FROM_BEGINNING);
        //subscriber.start(true, (r) -> {
        //   System.out.printf("SeqN: %s %d %s\n", r.topic(), r.offset(), r.value());
        //});
    }

}
