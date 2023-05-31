package sd2223.trab1.servers.rest.Kafka;

import jakarta.inject.Singleton;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
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
import sd2223.trab1.kafka.zookeeper.ZookeeperManager;
import sd2223.trab1.servers.Domain;
import sd2223.trab1.servers.java.JavaFeedsCommon;
import sd2223.trab1.servers.rest.RestResource;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

@Singleton
public abstract class KafkaRestFeedsResource<T extends Feeds> extends RestResource implements FeedsService, RecordProcessor {
    protected KafkaSubscriber subscriber;
    protected ZookeeperManager zookeeper;
    protected SyncPoint<Result> sync;
    final protected T impl;

    public KafkaRestFeedsResource(T impl) {
        this.impl = impl;
        zookeeper = ZookeeperManager.getInstance();
        this.subscriber = KafkaEngine.getInstance().createSubscriber(Domain.get());
        this.sync = new SyncPoint<>();
        subscriber.start(false, this);
    }

    @Override
    public void onReceive(ConsumerRecord<String, Function> r) {
        try {
            Function fun = r.value();
            Method[] methods = this.getClass().getMethods();
            Method method = Arrays.stream(methods).filter(f -> f.getName().equals(fun.getFunctionName())).findFirst().orElse(null);
            assert method != null;
            var res = method.invoke(this, fun.getParameters());
            var version = r.offset();
            sync.setResult(version, (Result) res);
        } catch (InvocationTargetException | IllegalAccessException e) {

        }
    }

    private <T> T generateResponseIfIsOK(Result<T> res, long nSeq) {
        if (res.isOK())
            throw new WebApplicationException(Response.status(statusCodeFrom(res)).
                    header(FeedsService.HEADER_VERSION, nSeq).
                    encoding(MediaType.APPLICATION_JSON).entity(res.value()).build());
        return super.fromJavaResult(res);
    }

    @Override
    public long postMessage(String user, String pwd, Message msg) {
        Object[] parameters = {user, pwd, msg};
        long id = 256 * (SyncPoint.getVersion() + 1) + Domain.uuid();
        msg.setId(id);
        long nSeq = KafkaEngine.getInstance().send(Domain.get(), new Function(KafkaEngine.POST_MESSAGE, parameters));
        Result<Long> res = (Result<Long>) sync.waitForResult(nSeq);
        return generateResponseIfIsOK(res, nSeq);
    }

    public Result<Long> postMessageKafka(String user, String pwd, Message msg) {
        return impl.postMessage(user, pwd, msg);
    }

    @Override
    public void removeFromPersonalFeed(String user, long mid, String pwd) {
        Object[] parameters = {user, mid, pwd};
        long nSeq = KafkaEngine.getInstance().send(Domain.get(), new Function(KafkaEngine.REMOVE_FROM_PERSONAL_FEED, parameters));
        Result<Void> res = (Result<Void>) sync.waitForResult(nSeq);
        generateResponseIfIsOK(res, nSeq);
    }

    public Result<Void> removeFromPersonalFeedKafka(String user, long mid, String pwd) {
        return impl.removeFromPersonalFeed(user, mid, pwd);
    }

    @Override
    public Message getMessage(Long version, String user, long mid) {
        if (zookeeper.isPrimary())
            sync.waitForResult(version);
        else if (version > SyncPoint.getVersion()) {
            String url = zookeeper.getPrimaryURI() + "/" + user + "/" + mid;
            System.out.println(url + " 1234");
            try {
                URI uri = new URI(url);
                throw new WebApplicationException(Response.temporaryRedirect(uri).
                        header(FeedsService.HEADER_VERSION, version).build());
            } catch (URISyntaxException e) {

            }

        }
        //sync.waitForResult(version);
        return super.fromJavaResult(impl.getMessage(user, mid));
    }

    @Override
    public List<Message> getMessages(Long version, String user, long time) {
        if (zookeeper.isPrimary())
            sync.waitForResult(version);
        else if (version > SyncPoint.getVersion()) {
            String url = zookeeper.getPrimaryURI() + "/" + user + "?time=" + time;
            try {
                URI uri = new URI(url);
                throw new WebApplicationException(Response.temporaryRedirect(uri).
                        header(FeedsService.HEADER_VERSION, version).build());
            } catch (URISyntaxException e) {

            }

        }
        //sync.waitForResult(version);
        return super.fromJavaResult(impl.getMessages(user, time));
    }

    @Override
    public void subUser(String user, String userSub, String pwd) {
        Object[] parameters = {user, userSub, pwd};
        long nSeq = KafkaEngine.getInstance().send(Domain.get(), new Function(KafkaEngine.SUB_USER, parameters));
        Result<Void> res = (Result<Void>) sync.waitForResult(nSeq);
        generateResponseIfIsOK(res, nSeq);
    }

    public Result<Void> subUserKafka(String user, String userSub, String pwd) {
        return impl.subUser(user, userSub, pwd);
    }

    @Override
    public void unsubscribeUser(String user, String userSub, String pwd) {
        Object[] parameters = {user, userSub, pwd};
        long nSeq = KafkaEngine.getInstance().send(Domain.get(), new Function(KafkaEngine.UNSUBSCRIBE_USER, parameters));
        Result<Void> res = (Result<Void>) sync.waitForResult(nSeq);
        generateResponseIfIsOK(res, nSeq);
    }


    public Result<Void> unsubscribeUserKafka(String user, String userSub, String pwd) {
        return impl.unsubscribeUser(user, userSub, pwd);
    }

    @Override
    public List<String> listSubs(Long version, String user) {
        if (zookeeper.isPrimary())
            sync.waitForResult(version);
        else if (version > SyncPoint.getVersion()) {
            String url = zookeeper.getPrimaryURI().toString() + "/sub/list/" + user;
            try {
                URI uri = new URI(url);
                throw new WebApplicationException(Response.temporaryRedirect(uri).
                        header(FeedsService.HEADER_VERSION, version).build());
            } catch (URISyntaxException e) {

            }

        }
        //  sync.waitForResult(version);
        return super.fromJavaResult(impl.listSubs(user));
    }

    @Override
    public void postServerInfo(String secret, String info, long version) {
        sync.setVersion(version);
        super.fromJavaResult(impl.postServerInfo(secret, info, version));
    }

    @Override
    public String getServerInfo(String secret) {
        return super.fromJavaResult(impl.getServerInfo(secret));
    }

    @Override
    public long getServerVersion(String secret) {
        return super.fromJavaResult(impl.getServerVersion(secret));
    }


    @Override
    public void deleteUserFeed(String user, String secret) {
        Object[] parameters = {user, secret};
        long nSeq = KafkaEngine.getInstance().send(Domain.get(), new Function(KafkaEngine.DELETE_USER_FEED, parameters));
        Result<Void> res = (Result<Void>) sync.waitForResult(nSeq);
        generateResponseIfIsOK(res, nSeq);
    }

    public Result<Void> deleteUserFeedKafka(String user, String secret) {
        return impl.deleteUserFeed(user, secret);
    }

}
