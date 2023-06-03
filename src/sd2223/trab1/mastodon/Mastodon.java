package sd2223.trab1.mastodon;

import sd2223.trab1.api.java.Feeds;
import sd2223.trab1.api.Message;
import sd2223.trab1.api.java.Result;
import sd2223.trab1.mastodon.msgs.GetStatusResult;
import sd2223.trab1.mastodon.msgs.PostStatusArgs;
import sd2223.trab1.mastodon.msgs.PostStatusResult;
import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.model.OAuthRequest;
import com.github.scribejava.core.model.Response;
import com.github.scribejava.core.model.Verb;
import com.github.scribejava.core.oauth.OAuth20Service;
import com.google.gson.reflect.TypeToken;

import sd2223.trab1.servers.java.JavaFeedsPreconditions;
import sd2223.trab1.servers.java.JavaFeedsPushPreconditions;
import utils.JSON;

import java.security.SecureRandom;
import java.util.List;

import static sd2223.trab1.api.java.Result.ErrorCode.*;
import static sd2223.trab1.api.java.Result.error;
import static sd2223.trab1.api.java.Result.ok;
import static sd2223.trab1.clients.rest.RestClient.getErrorCodeFrom;

public class Mastodon implements Feeds {

    static String MASTODON_NOVA_SERVER_URI = "http://10.170.138.52:3000";
    static String MASTODON_SOCIAL_SERVER_URI = "https://mastodon.social";

    static String MASTODON_SERVER_URI = MASTODON_NOVA_SERVER_URI;

    //NOVA
    private static final String clientKey = "bTsA8mwUlJmbDI2jdpOiL1NI6L8WdsyPrIaMYmSMHQI";
    private static final String clientSecret = "DGkAHzR1InSQ7E07u7mUWAwuAprf8-Issva0sXLYunMc";
    private static final String accessTokenStr = "Avypx1TkKj1oXlyZcJ7qcgGPlyxKe8npFoW2_6ZraoI";

    static final String STATUSES_PATH = "/api/v1/statuses";
    static final String TIMELINES_PATH = "/api/v1/timelines/home?since_id=";
    static final String ACCOUNT_FOLLOWING_PATH = "/api/v1/accounts/%s/following";
    static final String SEARCH_ACCOUNTS_PATH = "/api/v1/accounts/search?q=";
    static final String ACCOUNT_FOLLOW_PATH = "/api/v1/accounts/%s/follow";
    static final String ACCOUNT_UNFOLLOW_PATH = "/api/v1/accounts/%s/unfollow";

    private static final int HTTP_OK = 200;

    protected OAuth20Service service;
    protected OAuth2AccessToken accessToken;

    private static Mastodon impl;
    private JavaFeedsPreconditions preconditions;

    protected Mastodon(boolean saveState) {
        preconditions = new JavaFeedsPushPreconditions();
        try {
            service = new ServiceBuilder(clientKey).apiSecret(clientSecret).build(MastodonApi.instance());
            accessToken = new OAuth2AccessToken(accessTokenStr);
            if (saveState)
                cleanStatus();
        } catch (Exception x) {
            x.printStackTrace();
            System.exit(0);
        }
    }

    synchronized public static Mastodon getInstance(boolean saveState) {
        if (impl == null) {
            impl = new Mastodon(saveState);
        }
        return impl;
    }

    private String getEndpoint(String path, Object... args) {
        var fmt = MASTODON_SERVER_URI + path;
        return String.format(fmt, args);
    }

    private void cleanStatus() {
        List<Message> msgs = getMessages("", 0).value();
        for (Message msg : msgs) {
            removeFromPersonalFeed("", msg.getId(), "");
        }
    }

    @Override
    public Result<Long> postMessage(String user, String pwd, Message msg) {
        var precond = preconditions.postMessage(user, pwd, msg);
        if (!precond.isOK())
            return precond;
        try {
            final OAuthRequest request = new OAuthRequest(Verb.POST, getEndpoint(STATUSES_PATH));
            JSON.toMap(new PostStatusArgs(msg.getText())).forEach((k, v) -> {
                request.addBodyParameter(k, v.toString());
            });

            service.signRequest(accessToken, request);

            Response response = service.execute(request);
            if (response.getCode() == HTTP_OK) {
                var res = JSON.decode(response.getBody(), PostStatusResult.class);
                return ok(res.getId());
            }

        } catch (Exception x) {
            x.printStackTrace();
        }
        return error(INTERNAL_ERROR);
    }

    @Override
    public Result<List<Message>> getMessages(String user, long time) {
        var precond = preconditions.getMessages(user, time);
        if (!precond.isOK())
            return precond;
        try {
            long id = time;
            id = id << 16;
            id++;
            final OAuthRequest request = new OAuthRequest(Verb.GET, getEndpoint(TIMELINES_PATH + id));

            service.signRequest(accessToken, request);

            Response response = service.execute(request);
            if (response.getCode() == HTTP_OK) {
                List<PostStatusResult> res = JSON.decode(response.getBody(), new TypeToken<List<PostStatusResult>>() {
                });

                return ok(res.stream().map(PostStatusResult::toMessage).toList());
            }
            return error(getErrorCodeFrom(response.getCode()));
        } catch (Exception x) {
            x.printStackTrace();
        }
        return error(Result.ErrorCode.INTERNAL_ERROR);
    }


    @Override
    public Result<Void> removeFromPersonalFeed(String user, long mid, String pwd) {
        var precond = preconditions.removeFromPersonalFeed(user, mid, pwd);
        if (!precond.isOK())
            return precond;
        try {
            final OAuthRequest request = new OAuthRequest(Verb.DELETE, getEndpoint(STATUSES_PATH + "/" + mid));

            service.signRequest(accessToken, request);

            Response response = service.execute(request);

            if (response.getCode() == HTTP_OK) {
                return ok();
            }
        } catch (Exception x) {
            x.printStackTrace();
        }
        return error(Result.ErrorCode.INTERNAL_ERROR);
    }

    @Override
    public Result<Message> getMessage(String user, long mid) {
        var precond = preconditions.getMessage(user, mid);
        if (!precond.isOK())
            return precond;
        try {
            final OAuthRequest request = new OAuthRequest(Verb.GET, getEndpoint(STATUSES_PATH + "/" + mid));

            service.signRequest(accessToken, request);

            Response response = service.execute(request);
            if (response.getCode() == HTTP_OK) {
                PostStatusResult res = JSON.decode(response.getBody(), new TypeToken<PostStatusResult>() {
                });
                return ok(res.toMessage());
            }
            return error(getErrorCodeFrom(response.getCode()));
        } catch (Exception x) {
            x.printStackTrace();
        }
        return error(Result.ErrorCode.INTERNAL_ERROR);
    }

    private Result<Long> getUserID(String user) {
        try {
            String userName = user.split("@")[0];
            final OAuthRequest request = new OAuthRequest(Verb.GET, getEndpoint(SEARCH_ACCOUNTS_PATH + userName));
            service.signRequest(accessToken, request);
            Response response = service.execute(request);
            if (response.getCode() == HTTP_OK) {
                List<PostStatusResult> res = JSON.decode(response.getBody(), new TypeToken<List<PostStatusResult>>() {
                });
                if (!res.isEmpty())
                    return ok(res.get(0).getId());
                else return error(NOT_FOUND);
            }
        } catch (Exception x) {
            x.printStackTrace();
        }
        return error(Result.ErrorCode.INTERNAL_ERROR);
    }

    @Override
    public Result<Void> subUser(String user, String userSub, String pwd) {
        var precond = preconditions.subUser(user, userSub, pwd);
        if (!precond.isOK())
            return precond;
        Result<Long> res = getUserID(userSub);
        long id;
        if (res.isOK())
            id = res.value();
        else return error(NOT_FOUND);
        try {
            final OAuthRequest request = new OAuthRequest(Verb.POST, getEndpoint(String.format(ACCOUNT_FOLLOW_PATH, id)));
            service.signRequest(accessToken, request);
            Response response = service.execute(request);
            if (response.getCode() == HTTP_OK) {
                return ok();
            }
        } catch (Exception x) {
            x.printStackTrace();
        }
        return error(Result.ErrorCode.INTERNAL_ERROR);
    }

    @Override
    public Result<Void> unsubscribeUser(String user, String userSub, String pwd) {
        var precond = preconditions.unsubscribeUser(user, userSub, pwd);
        if (!precond.isOK())
            return precond;
        Result<Long> res = getUserID(userSub);
        long id;
        if (res.isOK())
            id = res.value();
        else return error(NOT_FOUND);
        try {
            final OAuthRequest request = new OAuthRequest(Verb.POST, getEndpoint(String.format(ACCOUNT_UNFOLLOW_PATH, id)));
            service.signRequest(accessToken, request);
            Response response = service.execute(request);
            if (response.getCode() == HTTP_OK) {
                return ok();
            }
        } catch (Exception x) {
            x.printStackTrace();
        }
        return error(Result.ErrorCode.INTERNAL_ERROR);
    }

    @Override
    public Result<List<String>> listSubs(String user) {
        var precond = preconditions.listSubs(user);
        if (!precond.isOK())
            return precond;
        Result<Long> res = getUserID(user);
        long id;
        if (res.isOK())
            id = res.value();
        else return error(CONFLICT);
        try {
            final OAuthRequest request = new OAuthRequest(Verb.GET, getEndpoint(String.format(ACCOUNT_FOLLOWING_PATH, id)));

            service.signRequest(accessToken, request);

            Response response = service.execute(request);

            if (response.getCode() == HTTP_OK) {
                List<GetStatusResult> resultList = JSON.decode(response.getBody(), new TypeToken<List<GetStatusResult>>() {
                });
                return ok(resultList.stream().map(GetStatusResult::toAccountName).toList());
            }
        } catch (Exception x) {
            x.printStackTrace();
        }
        return error(Result.ErrorCode.INTERNAL_ERROR);
    }
    @Override
    public Result<Void> deleteUserFeed(String user, String secret) {
        return error(NOT_IMPLEMENTED);
    }

    @Override
    public Result<String> getServerInfo(String secret) {
        return null;
    }

    @Override
    public Result<Void> postServerInfo(String secret, String info, long version) {
        return null;
    }

    @Override
    public Result<Long> getServerVersion(String secret) {
        return null;
    }
}
