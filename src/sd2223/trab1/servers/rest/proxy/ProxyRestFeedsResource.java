package sd2223.trab1.servers.rest.proxy;

import jakarta.inject.Singleton;
import sd2223.trab1.api.Message;
import sd2223.trab1.api.rest.FeedsService;
import sd2223.trab1.mastodon.Mastodon;
import sd2223.trab1.servers.rest.RestResource;

import java.util.List;

@Singleton
public class ProxyRestFeedsResource extends RestResource implements FeedsService {
    final protected Mastodon impl;

    public ProxyRestFeedsResource(boolean saveState) {
        this.impl = Mastodon.getInstance(saveState);
    }

    @Override
    public long postMessage(Long version, String user, String pwd, Message msg) {
        return super.fromJavaResult(impl.postMessage(user, pwd, msg));
    }

    @Override
    public void removeFromPersonalFeed(Long version, String user, long mid, String pwd) {
        super.fromJavaResult(impl.removeFromPersonalFeed(user, mid, pwd));
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
        super.fromJavaResult(impl.subUser(user, userSub, pwd));
    }

    @Override
    public void unsubscribeUser(Long version, String user, String userSub, String pwd) {
        super.fromJavaResult(impl.unsubscribeUser(user, userSub, pwd));
    }

    @Override
    public List<String> listSubs(Long version, String user) {
        return super.fromJavaResult(impl.listSubs(user));
    }

    @Override
    public void deleteUserFeed(Long version, String user, String secret) {
        super.fromJavaResult(impl.deleteUserFeed(user, secret));
    }
}
