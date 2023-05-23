package sd2223.trab1.servers.rest.Kafka;

import jakarta.inject.Singleton;
import sd2223.trab1.api.Message;
import sd2223.trab1.api.java.FeedsPull;
import sd2223.trab1.api.rest.FeedsServicePull;
import sd2223.trab1.servers.java.kafka.JavaFeedsPullKafka;

import java.util.List;

@Singleton
public class KafkaRestFeedsPullResource extends KafkaRestFeedsResource<FeedsPull> implements FeedsServicePull {

    public KafkaRestFeedsPullResource(String secret) {
        super(new JavaFeedsPullKafka(secret));
    }

    @Override
    public List<Message> pull_getTimeFilteredPersonalFeed(Long version, String user, long time) {
        return super.fromJavaResult(impl.pull_getTimeFilteredPersonalFeed(user, time));
    }

}
