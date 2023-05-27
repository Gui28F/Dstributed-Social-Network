package sd2223.trab1.servers.rest.Kafka;

import jakarta.inject.Singleton;
import sd2223.trab1.api.PushMessage;
import sd2223.trab1.api.java.FeedsPush;
import sd2223.trab1.api.rest.FeedsServicePush;
import sd2223.trab1.servers.java.JavaFeedsPush;

@Singleton
public class KafkaRestFeedsPushResource extends KafkaRestFeedsResource<FeedsPush> implements FeedsServicePush {

    public KafkaRestFeedsPushResource(String secret) {
        super(new JavaFeedsPush(secret));
    }

    @Override
    public void push_PushMessage(Long version, PushMessage msg) {
        super.fromJavaResult(impl.push_PushMessage(msg));
    }

    @Override
    public void push_updateFollowers(Long version, String user, String follower, boolean following) {
        super.fromJavaResult(impl.push_updateFollowers(user, follower, following));
    }
}
