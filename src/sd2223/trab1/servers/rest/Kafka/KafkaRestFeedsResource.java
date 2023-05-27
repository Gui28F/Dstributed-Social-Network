package sd2223.trab1.servers.rest.Kafka;

import jakarta.inject.Singleton;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import sd2223.trab1.api.Message;
import sd2223.trab1.api.java.Feeds;
import sd2223.trab1.api.java.Result;
import sd2223.trab1.api.rest.FeedsService;
import sd2223.trab1.kafka.Function;
import sd2223.trab1.kafka.KafkaEngine;
import sd2223.trab1.kafka.KafkaSubscriber;
import sd2223.trab1.kafka.RecordProcessor;
import sd2223.trab1.kafka.sync.SyncPoint;
import sd2223.trab1.servers.rest.RestResource;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

@Singleton
public abstract class KafkaRestFeedsResource<T extends Feeds> extends RestResource implements FeedsService, RecordProcessor {
    //TODO ver se temos que repetie
    protected KafkaSubscriber subscriber;
    protected SyncPoint<Result> sync;
    final protected T impl;

    public KafkaRestFeedsResource(T impl) {
        this.impl = impl;
        this.subscriber = KafkaEngine.getInstance().createSubscriber();
        this.sync = new SyncPoint<>();
        subscriber.start(false, this);
    }

    @Override
    public void onReceive(ConsumerRecord<String, Function> r) {
        try {
            Function fun = r.value();
            Method[] methods = this.getClass().getMethods();
            Method method = Arrays.stream(methods).filter(f -> f.getName().equals(fun.getFunctionName())).findFirst().orElse(null);
            System.out.println(method);
            var res = method.invoke(this, fun.getParameters());
            var version = r.offset();
            sync.setResult(version, (Result) res);
        } catch (InvocationTargetException | IllegalAccessException e) {

        }
    }

    @Override
    public long postMessage(Long version, String user, String pwd, Message msg) {
        Object[] parameters = {user, pwd, msg};
        Long nSeq = KafkaEngine.getInstance().send(new Function(KafkaEngine.POST_MESSAGE, parameters));
        return super.fromJavaResult((Result<Long>) sync.waitForResult(nSeq));

    }

    public Result<Long> postMessageKafka(String user, String pwd, Message msg) {
        return impl.postMessage(user, pwd, msg);
    }

    @Override
    public void removeFromPersonalFeed(Long version, String user, long mid, String pwd) {
        Object[] parameters = {user, mid, pwd};
        Long nSeq = KafkaEngine.getInstance().send(new Function(KafkaEngine.REMOVE_FROM_PERSONAL_FEED, parameters));
        super.fromJavaResult((Result<Void>) sync.waitForResult(nSeq));
    }

    public Result<Void> removeFromPersonalFeedKafka(String user, long mid, String pwd) {
        return impl.removeFromPersonalFeed(user, mid, pwd);
    }

    @Override
    public Message getMessage(Long version, String user, long mid) {
        return super.fromJavaResult(impl.getMessage(user, mid));
    }

    @Override
    public List<Message> getMessages(Long version, String user, long time) {
        return super.fromJavaResult(impl.getMessages(user, time));
    }

    @Override
    public void subUser(Long version, String user, String userSub, String pwd) {
        Object[] parameters = {user, userSub, pwd};
        Long nSeq = KafkaEngine.getInstance().send(new Function(KafkaEngine.SUB_USER, parameters));
        super.fromJavaResult((Result<Void>) sync.waitForResult(nSeq));
    }

    public Result<Void> subUserKafka(String user, String userSub, String pwd) {
        return impl.subUser(user, userSub, pwd);
    }

    @Override
    public void unsubscribeUser(Long version, String user, String userSub, String pwd) {
        Object[] parameters = {user, userSub, pwd};
        Long nSeq = KafkaEngine.getInstance().send(new Function(KafkaEngine.UNSUBSCRIBE_USER, parameters));
        super.fromJavaResult((Result<Void>) sync.waitForResult(nSeq));
    }


    public Result<Void> unsubscribeUserKafka(String user, String userSub, String pwd) {
        return impl.unsubscribeUser(user, userSub, pwd);
    }

    @Override
    public List<String> listSubs(Long version, String user) {
        return super.fromJavaResult(impl.listSubs(user));
    }


    @Override
    public void deleteUserFeed(Long version, String user, String secret) {
        Object[] parameters = {user, secret};
        Long nSeq = KafkaEngine.getInstance().send(new Function(KafkaEngine.DELETE_USER_FEED, parameters));
        super.fromJavaResult((Result<Void>) sync.waitForResult(nSeq));
    }

    public Result<Void> deleteUserFeedKafka(String user, String secret) {
        return impl.deleteUserFeed(user, secret);
    }
}
