
package sd2223.trab1.servers.java.kafka;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import sd2223.trab1.api.Message;
import sd2223.trab1.api.java.FeedsPull;
import sd2223.trab1.api.java.Result;
import sd2223.trab1.kafka.KafkaEngine;
import sd2223.trab1.servers.Domain;
import sd2223.trab1.servers.java.JavaFeedsPullPreconditions;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.*;

import static sd2223.trab1.api.java.Result.ErrorCode.*;
import static sd2223.trab1.api.java.Result.error;
import static sd2223.trab1.api.java.Result.ok;
import static sd2223.trab1.clients.Clients.FeedsPullClients;

public class JavaFeedsPullKafka extends FeedsCommonKafka<FeedsPull> implements FeedsPull {
    private static final long FEEDS_CACHE_EXPIRATION = 3000;

    public JavaFeedsPullKafka(String secret) {
        super(new JavaFeedsPullPreconditions(), secret);
    }

    final LoadingCache<FeedInfoKey, Result<List<Message>>> cache = CacheBuilder.newBuilder()
            .expireAfterWrite(Duration.ofMillis(FEEDS_CACHE_EXPIRATION)).removalListener((e) -> {
            }).build(new CacheLoader<>() {
                @Override
                public Result<List<Message>> load(FeedInfoKey info) throws Exception {
                    var res = FeedsPullClients.get(info.domain()).pull_getTimeFilteredPersonalFeed(info.user(), info.time());
                    if (res.error() == TIMEOUT)
                        return error(BAD_REQUEST);

                    return res;
                }
            });


    @Override
    public Result<Message> getMessage(String user, long mid) {
        Object[] parameters = {user, mid};
        Long nSeq = KafkaEngine.getInstance().send(KafkaEngine.GET_MESSAGE, parameters);
        synchronized (version) {
            try {
                while (version.getVersion() < nSeq)
                    version.wait();
            } catch (InterruptedException e) {
            }
        }
        return resultMap.get(nSeq);
    }

    public Result<Message> getMessageKafka(String user, long mid) {

        var preconditionsResult = preconditions.getMessage(user, mid);
        //TODO
        if (!preconditionsResult.isOK())
            if (preconditionsResult.error() == Result.ErrorCode.REDIRECTED)
                return Result.ok(preconditionsResult.value());
            else
                return preconditionsResult;

        FeedInfo ufi = feeds.get(user);
        if (ufi == null)
            return error(NOT_FOUND);

        synchronized (ufi.user()) {
            if (ufi.messages().contains(mid))
                return ok(messages.get(mid));

            var list = getMessages(user, -1L);
            if (!list.isOK())
                return error(list.error());

            var res = list.value().stream().filter(m -> m.getId() == mid).findFirst();
            return res.isPresent() ? ok(res.get()) : error(NOT_FOUND);
        }
    }

    @Override
    public Result<List<Message>> getMessages(String user, long time) {
        Object[] parameters = {user, time};
        Long nSeq = KafkaEngine.getInstance().send(KafkaEngine.GET_MESSAGES, parameters);
        synchronized (version) {
            try {
                while (version.getVersion() < nSeq)
                    version.wait();
            } catch (InterruptedException e) {
            }
        }
        return resultMap.get(nSeq);
    }

    public Result<List<Message>> getMessagesKafka(String user, long time) {

        var preconditionsResult = preconditions.getMessages(user, time);
        //TODO
        if (!preconditionsResult.isOK())
            if (preconditionsResult.error() == Result.ErrorCode.REDIRECTED)
                return Result.ok(preconditionsResult.value());
            else
                return preconditionsResult;

        FeedInfo ufi = feeds.get(user);
        if (ufi == null)
            return ok(Collections.emptyList());

        synchronized (ufi.user()) {
            var msgs = new ArrayList<Message>();
            msgs.addAll(ufi.messages().stream().map(messages::get).filter(m -> m.getCreationTime() > time).toList());

            for (var s : ufi.following())
                msgs.addAll(getCachedPersonalFeed(s, time));

            return ok(msgs);
        }
    }

    public Result<List<Message>> pull_getTimeFilteredPersonalFeed(String user, long time) {

        var preconditionsResult = preconditions.pull_getTimeFilteredPersonalFeed(user, time);
        if (!preconditionsResult.isOK())
            return preconditionsResult;

        return ok(super.getTimeFilteredPersonalFeed(user, time));
    }

    private List<Message> getCachedPersonalFeed(String name, long time) {
        try {
            if (FeedUser.from(name).isRemoteUser())
                return cache.get(FeedInfoKey.from(name, time)).value();
            else
                return super.getTimeFilteredPersonalFeed(name, time);
        } catch (Exception x) {
            x.printStackTrace();
        }
        return Collections.emptyList();
    }

    @Override
    protected void deleteFromUserFeed(String user, Set<Long> mids) {

        Object[] parameters = {user, mids};
        Long nSeq = KafkaEngine.getInstance().send(KafkaEngine.DELETE_FROM_USER_FEED, parameters);
        synchronized (version) {
            try {
                while (version.getVersion() < nSeq)
                    version.wait();
            } catch (InterruptedException e) {
            }
        }
    }

    protected void deleteFromUserFeedKafka(String user, Set<Long> mids) {
        messages.keySet().removeAll(mids);
    }


    static record FeedInfoKey(String user, String domain, long time) {
        static FeedInfoKey from(String name, long time) {
            var idx = name.indexOf('@');
            var domain = idx < 0 ? Domain.get() : name.substring(idx + 1);
            return new FeedInfoKey(name, domain, time);
        }
    }
}
