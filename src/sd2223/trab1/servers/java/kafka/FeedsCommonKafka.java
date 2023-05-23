package sd2223.trab1.servers.java.kafka;

import static sd2223.trab1.api.java.Result.ErrorCode.FORBIDDEN;
import static sd2223.trab1.api.java.Result.error;
import static sd2223.trab1.api.java.Result.ok;
import static sd2223.trab1.api.java.Result.ErrorCode.NOT_FOUND;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import sd2223.trab1.api.Message;
import sd2223.trab1.api.java.Feeds;
import sd2223.trab1.api.java.Result;
import sd2223.trab1.kafka.KafkaEngine;
import sd2223.trab1.kafka.KafkaSubscriber;
import sd2223.trab1.servers.Domain;

@SuppressWarnings("unchecked")
public abstract class FeedsCommonKafka<T extends Feeds> implements Feeds {
    private static final long FEEDS_MID_PREFIX = 1_000_000_000;

    protected AtomicLong serial = new AtomicLong(Domain.uuid() * FEEDS_MID_PREFIX);

    final protected T preconditions;

    private String secret;

    protected Map<Long, Result> resultMap;
    protected KafkaSubscriber subscriber;
    protected Version version;

    protected FeedsCommonKafka(T preconditions, String secret) {
        this.preconditions = preconditions;
        this.secret = secret;
        this.resultMap = new HashMap<>();
        this.version = new Version();
        this.subscriber = KafkaEngine.getInstance().createSubscriber();
        this.startSubscriber();
    }

    protected Map<Long, Message> messages = new ConcurrentHashMap<>();
    protected Map<String, FeedInfo> feeds = new ConcurrentHashMap<>();

    static protected record FeedInfo(String user, Set<Long> messages, Set<String> following, Set<String> followees) {
        public FeedInfo(String user) {
            this(user, new HashSet<>(), new HashSet<>(), ConcurrentHashMap.newKeySet());
        }
    }

    private void startSubscriber() {//TODO NAO PODE ESTAR AQUI SE NÃO VAI CONSEGUIR IR BUSCAR OS METODOS DAS SUBCLASSES
        subscriber.start(true, (r) -> {
            System.out.printf("SeqN: %s %d %s\n", r.topic(), r.offset(), Arrays.toString(r.value()));
            try {
                Method method = this.getClass().getDeclaredMethod(r.topic());
                Object[] parameters = r.value();
                Object obj = this.getClass().getDeclaredConstructor();
                method.invoke(obj, parameters);

            } catch (NoSuchMethodException e) {

            } catch (InvocationTargetException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public Result<Long> postMessage(String user, String pwd, Message msg) {
        Object[] parameters = {user, pwd, msg};
        Long nSeq = KafkaEngine.getInstance().send(KafkaEngine.POST_MESSAGE, parameters);
        synchronized (version) {
            try {
                while (version.getVersion() < nSeq)
                    version.wait();
            } catch (InterruptedException e) {

            }
        }
        return resultMap.get(nSeq);
    }

    public Result<Long> postMessageKafka(String user, String pwd, Message msg) {
        var preconditionsResult = preconditions.postMessage(user, pwd, msg);
        if (!preconditionsResult.isOK())
            return preconditionsResult;

        Long mid = serial.incrementAndGet();
        msg.setId(mid);
        msg.setCreationTime(System.currentTimeMillis());

        FeedInfo ufi = feeds.computeIfAbsent(user, FeedInfo::new);
        synchronized (ufi.user()) {
            ufi.messages().add(mid);
            messages.putIfAbsent(mid, msg);
        }
        return Result.ok(mid);
    }

    @Override
    public Result<Void> removeFromPersonalFeed(String user, long mid, String pwd) {

        Object[] parameters = {user, mid, pwd};
        Long nSeq = KafkaEngine.getInstance().send(KafkaEngine.REMOVE_FROM_PERSONAL_FEED, parameters);
        synchronized (version) {
            try {
                while (version.getVersion() < nSeq)
                    version.wait();
            } catch (InterruptedException e) {

            }
        }
        return resultMap.get(nSeq);
    }

    public Result<Void> removeFromPersonalFeedKafka(String user, long mid, String pwd) {

        var preconditionsResult = preconditions.removeFromPersonalFeed(user, mid, pwd);
        if (!preconditionsResult.isOK())
            return preconditionsResult;

        var ufi = feeds.get(user);
        if (ufi == null)
            return error(NOT_FOUND);

        synchronized (ufi.user()) {
            if (!ufi.messages().remove(mid))
                return error(NOT_FOUND);
        }

        deleteFromUserFeed(user, Set.of(mid));

        return ok();
    }


    protected List<Message> getTimeFilteredPersonalFeed(String user, long time) {
        var ufi = feeds.computeIfAbsent(user, FeedInfo::new);
        synchronized (ufi.user()) {
            return ufi.messages().stream().map(messages::get).filter(m -> m.getCreationTime() > time).toList();
        }
    }

    @Override
    public Result<Void> subUser(String user, String userSub, String pwd) {
        Object[] parameters = {user, userSub, pwd};
        Long nSeq = KafkaEngine.getInstance().send(KafkaEngine.SUB_USER, parameters);
        synchronized (version) {
            try {
                while (version.getVersion() < nSeq)
                    version.wait();
            } catch (InterruptedException e) {

            }
        }
        return resultMap.get(nSeq);
    }

    public Result<Void> subUserKafka(String user, String userSub, String pwd) {

        var preconditionsResult = preconditions.subUser(user, userSub, pwd);
        if (!preconditionsResult.isOK())
            return preconditionsResult;


        var ufi = feeds.computeIfAbsent(user, FeedInfo::new);
        synchronized (ufi.user()) {
            ufi.following().add(userSub);
        }
        return ok();
    }

    @Override
    public Result<Void> unsubscribeUser(String user, String userSub, String pwd) {
        Object[] parameters = {user, userSub, pwd};
        Long nSeq = KafkaEngine.getInstance().send(KafkaEngine.UNSUBSCRIBE_USER, parameters);
        synchronized (version) {
            try {
                while (version.getVersion() < nSeq)
                    version.wait();
            } catch (InterruptedException e) {

            }
        }
        return resultMap.get(nSeq);
    }

    public Result<Void> unsubscribeUserKafka(String user, String userSub, String pwd) {

        var preconditionsResult = preconditions.unsubscribeUser(user, userSub, pwd);
        if (!preconditionsResult.isOK())
            return preconditionsResult;

        FeedInfo ufi = feeds.computeIfAbsent(user, FeedInfo::new);
        synchronized (ufi.user()) {
            ufi.following().remove(userSub);
        }
        return ok();
    }

    @Override
    public Result<List<String>> listSubs(String user) {
        Object[] parameters = {user};
        Long nSeq = KafkaEngine.getInstance().send(KafkaEngine.LIST_SUBS, parameters);
        synchronized (version) {
            try {
                while (version.getVersion() < nSeq)
                    version.wait();
            } catch (InterruptedException e) {

            }
        }
        return resultMap.get(nSeq);
    }

    public Result<List<String>> listSubsKafka(String user) {

        var preconditionsResult = preconditions.listSubs(user);
        if (!preconditionsResult.isOK())
            return preconditionsResult;

        FeedInfo ufi = feeds.computeIfAbsent(user, FeedInfo::new);
        synchronized (ufi.user()) {
            return ok(new ArrayList<>(ufi.following()));
        }
    }

    @Override
    public Result<Void> deleteUserFeed(String user, String secret) {
        Object[] parameters = {user, secret};
        Long nSeq = KafkaEngine.getInstance().send(KafkaEngine.DELETE_USER_FEED, parameters);
        synchronized (version) {
            try {
                while (version.getVersion() < nSeq)
                    version.wait();
            } catch (InterruptedException e) {

            }
        }
        return resultMap.get(nSeq);
    }

    public Result<Void> deleteUserFeedKafka(String user, String secret) {
        if (!secret.equals(this.secret))
            return error(FORBIDDEN);
        var preconditionsResult = preconditions.deleteUserFeed(user, secret);
        if (!preconditionsResult.isOK())
            return preconditionsResult;

        FeedInfo ufi = feeds.remove(user);
        if (ufi == null)
            return error(NOT_FOUND);

        synchronized (ufi.user()) {
            deleteFromUserFeed(user, ufi.messages());
            for (var u : ufi.followees())
                ufi.following().remove(u);
        }
        return ok();
    }


    static public record FeedUser(String user, String name, String pwd, String domain) {
        private static final String EMPTY_PASSWORD = "";

        public static FeedUser from(String name, String pwd) {
            var idx = name.indexOf('@');
            var n = idx < 0 ? name : name.substring(0, idx);
            var d = idx < 0 ? Domain.get() : name.substring(idx + 1);
            return new FeedUser(name, n, pwd, d);
        }

        public static FeedUser from(String name) {
            return FeedUser.from(name, EMPTY_PASSWORD);
        }

        boolean isLocalUser() {
            return domain.equals(Domain.get());
        }

        public boolean isRemoteUser() {
            return !isLocalUser();
        }

    }

    abstract protected void deleteFromUserFeed(String user, Set<Long> mids);
}
