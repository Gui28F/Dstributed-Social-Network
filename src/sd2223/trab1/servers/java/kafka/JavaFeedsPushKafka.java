package sd2223.trab1.servers.java.kafka;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import sd2223.trab1.api.Message;
import sd2223.trab1.api.PushMessage;
import sd2223.trab1.api.java.FeedsPush;
import sd2223.trab1.api.java.Result;
import sd2223.trab1.kafka.Function;
import sd2223.trab1.kafka.KafkaEngine;
import sd2223.trab1.servers.Domain;
import sd2223.trab1.servers.java.JavaFeedsPushPreconditions;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static sd2223.trab1.api.java.Result.ErrorCode.NOT_FOUND;
import static sd2223.trab1.api.java.Result.error;
import static sd2223.trab1.api.java.Result.ok;
import static sd2223.trab1.clients.Clients.FeedsPushClients;

public class JavaFeedsPushKafka extends FeedsCommonKafka<FeedsPush> implements FeedsPush {

    private static final long PERMANENT_REMOVAL_DELAY = 30;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    final Map<Long, Set<String>> msgs2users = new ConcurrentHashMap<>();

    public JavaFeedsPushKafka(String secret) {
        super(new JavaFeedsPushPreconditions(), secret);
    }

    @Override
    public Result<Long> postMessage(String user, String pwd, Message msg) {
        Object[] parameters = {user, pwd, msg};
        Long nSeq = KafkaEngine.getInstance().send(new Function(KafkaEngine.POST_MESSAGE, parameters));
        return (Result<Long>) sync.waitForResult(nSeq);
    }

    public Result<Long> postMessageKafka(String user, String pwd, Message msg) {
        var res = super.postMessageKafka(user, pwd, msg);
        if (res.isOK()) {
            var followees = feeds.get(user).followees();
            var subscribers = followees.stream()
                    .map(FeedUser::from)
                    .collect(Collectors.groupingBy(FeedUser::domain, Collectors.mapping(FeedUser::user, Collectors.toSet())));
            scheduler.execute(() -> {
                for (var e : subscribers.entrySet()) {
                    var domain = e.getKey();
                    var users = e.getValue();
                    if (domain.equals(Domain.get()))
                        while (!push_PushMessage(new PushMessage(users, msg)).isOK()) ;
                    else
                        while (!FeedsPushClients.get(domain).push_PushMessage(new PushMessage(users, msg)).isOK()) ;
                }
            });
        }
        return res;
    }

    /**
     * @Override public Result<Message> getMessage(String user, long mid) {
     * Object[] parameters = {user, mid};
     * Long nSeq = KafkaEngine.getInstance().send(new Function(KafkaEngine.GET_MESSAGE, parameters));
     * return (Result<Message>) sync.waitForResult(nSeq);
     * }
     */
    @Override
    public Result<Message> getMessage(String user, long mid) {
        var preconditionsResult = preconditions.getMessage(user, mid);
        if (!preconditionsResult.isOK())
            return preconditionsResult;

        var ufi = feeds.get(user);
        if (ufi == null)
            return error(NOT_FOUND);

        synchronized (ufi.user()) {
            if (!ufi.messages().contains(mid))
                return error(NOT_FOUND);

            return ok(messages.get(mid));
        }
    }

    /**
     * @Override public Result<List<Message>> getMessages(String user, long time) {
     * Object[] parameters = {user, time};
     * Long nSeq = KafkaEngine.getInstance().send(new Function(KafkaEngine.GET_MESSAGES, parameters));
     * return (Result<List<Message>>) sync.waitForResult(nSeq);
     * }
     */
    @Override
    public Result<List<Message>> getMessages(String user, long time) {
        var preconditionsResult = preconditions.getMessages(user, time);
        if (!preconditionsResult.isOK())
            return preconditionsResult;

        return ok(super.getTimeFilteredPersonalFeed(user, time));
    }


    @Override
    public Result<Void> push_updateFollowers(String user, String follower, boolean following) {
        Object[] parameters = {user, follower, following};
        Long nSeq = KafkaEngine.getInstance().send(new Function(KafkaEngine.PUSH_UPDATE_FOLLOWERS, parameters));
        return (Result<Void>) sync.waitForResult(nSeq);
    }

    public Result<Void> push_updateFollowersKafka(String user, String follower, boolean following) {
        var preconditionsResult = preconditions.push_updateFollowers(user, follower, following);
        if (!preconditionsResult.isOK())
            return preconditionsResult;

        var followees = feeds.computeIfAbsent(user, FeedInfo::new).followees();

        if (following)
            followees.add(follower);
        else
            followees.remove(follower);
        return ok();
    }

    /*@Override
    public Result<Void> push_PushMessage(PushMessage pm) {
        System.out.println("pm push "+ pm);
        Object[] parameters = {pm};
        Long nSeq = KafkaEngine.getInstance().send(new Function(KafkaEngine.PUSH_PUSH_MESSAGE, parameters));
        return (Result<Void>) sync.waitForResult(nSeq);
    }*/
    @Override
    public Result<Void> push_PushMessage(PushMessage pm) {
        var msg = pm.getMessage();
        super.messages.put(msg.getId(), msg);

        for (var s : pm.getSubscribers()) {
            feeds.computeIfAbsent(s, FeedInfo::new).messages().add(msg.getId());
        }
        msgs2users.computeIfAbsent(msg.getId(), (k) -> ConcurrentHashMap.newKeySet()).addAll(pm.getSubscribers());
        return ok();
    }


    protected void deleteFromUserFeed(String user, Set<Long> mids) {
        Object[] parameters = {user, mids};
        Long nSeq = KafkaEngine.getInstance().send(new Function(KafkaEngine.DELETE_FROM_USER_FEED, parameters));
        sync.waitForResult(nSeq);
    }

    @Override
    protected void deleteFromUserFeedKafka(String user, Set<Long> mids) {
        for (var mid : mids) {
            var references = msgs2users.get(mid);
            if (references != null && references.remove(user) && references.isEmpty()) {
                scheduler.schedule(() -> {
                    super.messages.remove(mid);
                }, PERMANENT_REMOVAL_DELAY, TimeUnit.SECONDS);
            }
        }
    }
}
