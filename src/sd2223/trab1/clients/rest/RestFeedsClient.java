package sd2223.trab1.clients.rest;

import static sd2223.trab1.api.java.Result.error;
import static sd2223.trab1.api.java.Result.ErrorCode.NOT_IMPLEMENTED;

import java.util.List;

import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import sd2223.trab1.api.Message;
import sd2223.trab1.api.java.Feeds;
import sd2223.trab1.api.java.Result;
import sd2223.trab1.api.rest.FeedsService;
import sd2223.trab1.api.rest.UsersService;
import sd2223.trab1.kafka.sync.SyncPoint;


public class RestFeedsClient extends RestClient implements Feeds {
    protected static final String PERSONAL = "personal";

    final protected WebTarget target;

    public RestFeedsClient(String serverURI) {
        super(serverURI);
        target = client.target(serverURI).path(FeedsService.PATH);
    }

    @Override
    public Result<Void> deleteUserFeed(String user, String secret) {
        return super.reTry(() -> clt_deleteUserFeed(user, secret));
    }

    @Override
    public Result<String> getServerInfo(String secret) {
        return super.reTry(() -> clt_getServerInfo(secret));
    }

    @Override
    public Result<Void> postServerInfo(String secret, String info) {
        return super.reTry(() -> clt_postServerInfo(secret, info));
    }

    @Override
    public Result<Long> getServerVersion(String secret) {
        return super.reTry(() -> clt_getServerVersion(secret));
    }

    @Override
    public Result<Message> getMessage(String user, long mid) {
        return super.reTry(() -> clt_getMessage(user, mid));
    }

    @Override
    public Result<List<Message>> getMessages(String user, long time) {
        return super.reTry(() -> clt_getMessages(user, time));
    }

    @Override
    public Result<Long> postMessage(String user, String pwd, Message msg) {
        return error(NOT_IMPLEMENTED);
    }

    @Override
    public Result<Void> removeFromPersonalFeed(String user, long mid, String pwd) {
        return error(NOT_IMPLEMENTED);
    }

    @Override
    public Result<Void> subUser(String user, String userSub, String pwd) {
        return error(NOT_IMPLEMENTED);
    }

    @Override
    public Result<Void> unsubscribeUser(String user, String userSub, String pwd) {
        return error(NOT_IMPLEMENTED);
    }

    @Override
    public Result<List<String>> listSubs(String user) {
        return error(NOT_IMPLEMENTED);
    }


    private Result<Long> clt_getServerVersion(String secret) {
        Response r = target.path("server/version")
                .queryParam(FeedsService.SECRET, secret)
                .request()
                .get();

        return super.toJavaResult(r, Long.class);
    }

    private Result<String> clt_getServerInfo(String secret) {
        Response r = target.path("server")
                .queryParam(FeedsService.SECRET, secret)
                .request()
                .get();

        return super.toJavaResult(r, String.class);
    }

    private Result<Void> clt_postServerInfo(String secret, String info) {
        Response r = target.path("server")
                .queryParam(FeedsService.SECRET, secret)
                .request()
                .post(Entity.entity(info, MediaType.APPLICATION_JSON_TYPE));

        return super.toJavaResult(r, Void.class);
    }


    private Result<Message> clt_getMessage(String user, long mid) {
        Response r = target.path(user).path(Long.toString(mid))
                .request()
                .header(FeedsService.HEADER_VERSION, SyncPoint.getVersion())
                .get();

        return super.toJavaResult(r, Message.class);
    }

    private Result<List<Message>> clt_getMessages(String user, long time) {
        Response r = target.path(user)
                .queryParam(FeedsService.TIME, time)
                .request()
                .header(FeedsService.HEADER_VERSION, SyncPoint.getVersion())
                .get();

        return super.toJavaResult(r, new GenericType<List<Message>>() {
        });
    }

    public Result<Void> clt_deleteUserFeed(String user, String secret) {
        Response r = target.path(PERSONAL).path(user)
                .queryParam(FeedsService.SECRET, secret)
                .request()
                .delete();

        return super.toJavaResult(r, Void.class);
    }
}
