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
import utils.JSON;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;

import static sd2223.trab1.api.java.Result.ErrorCode.*;
import static sd2223.trab1.api.java.Result.error;
import static sd2223.trab1.api.java.Result.ok;

public class Mastodon implements Feeds {

    static String MASTODON_NOVA_SERVER_URI = "http://10.170.138.52:3000";
    static String MASTODON_SOCIAL_SERVER_URI = "https://mastodon.social";

    static String MASTODON_SERVER_URI = MASTODON_SOCIAL_SERVER_URI;

    private static final String clientKey = "z59r22ZOfNEhS7S6JG8J6uhELhKv29zJhrNYcKWkmOs";
    private static final String clientSecret = "O8nk_cHc_A0c_cYR4XINJ-abYJbtOl6vW9UArYCr7Ms";
    private static final String accessTokenStr = "9hvx0IaruSKxThq6q-LtqhqhVYS1kehyMb1zz5HTt2I";

    static final String STATUSES_PATH = "/api/v1/statuses";
    static final String TIMELINES_PATH = "/api/v1/timelines/home?since_id=";
    static final String ACCOUNT_FOLLOWING_PATH = "/api/v1/accounts/%s/following";
    static final String VERIFY_CREDENTIALS_PATH = "/api/v1/accounts/verify_credentials";
    static final String SEARCH_ACCOUNTS_PATH = "/api/v1/accounts/search";
    static final String ACCOUNT_FOLLOW_PATH = "/api/v1/accounts/%s/follow";
    static final String ACCOUNT_UNFOLLOW_PATH = "/api/v1/accounts/%s/unfollow";

    private static final int HTTP_OK = 200;

    protected OAuth20Service service;
    protected OAuth2AccessToken accessToken;

    private static Mastodon impl;

    protected Mastodon() {
        try {
            service = new ServiceBuilder(clientKey).apiSecret(clientSecret).build(MastodonApi.instance());
            accessToken = new OAuth2AccessToken(accessTokenStr);
        } catch (Exception x) {
            x.printStackTrace();
            System.exit(0);
        }
    }

    synchronized public static Mastodon getInstance() {
        if (impl == null)
            impl = new Mastodon();
        return impl;
    }

    private String getEndpoint(String path, Object... args) {
        var fmt = MASTODON_SERVER_URI + path;
        return String.format(fmt, args);
    }

    @Override
    public Result<Long> postMessage(String user, String pwd, Message msg) {
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
        try {
            String id = String.valueOf(time * 24 * 60 * 60);


            final OAuthRequest request = new OAuthRequest(Verb.GET, getEndpoint(TIMELINES_PATH + id));

            service.signRequest(accessToken, request);

            Response response = service.execute(request);

            if (response.getCode() == HTTP_OK) {
                List<PostStatusResult> res = JSON.decode(response.getBody(), new TypeToken<List<PostStatusResult>>() {
                });

                return ok(res.stream().map(PostStatusResult::toMessage).toList());
            }
        } catch (Exception x) {
            x.printStackTrace();
        }
        return error(Result.ErrorCode.INTERNAL_ERROR);
    }


    @Override
    public Result<Void> removeFromPersonalFeed(String user, long mid, String pwd) {
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
        return error(NOT_IMPLEMENTED);
    }

    private Result<Long> getUserID(String user) {
        try {
            final OAuthRequest request = new OAuthRequest(Verb.GET, getEndpoint(SEARCH_ACCOUNTS_PATH + user));
            service.signRequest(accessToken, request);
            Response response = service.execute(request);
            if (response.getCode() == HTTP_OK) {
                List<PostStatusResult> res = JSON.decode(response.getBody(), new TypeToken<List<PostStatusResult>>() {
                });
                if (res.size() == 1)
                    return ok(res.get(0).getId());
                    //TODO retorna o que se a pesquisa der mais do que um user?
                else return error(CONFLICT);
            }
        } catch (Exception x) {
            x.printStackTrace();
        }
        return error(Result.ErrorCode.INTERNAL_ERROR);
    }

    @Override
    public Result<Void> subUser(String user, String userSub, String pwd) {
        Result<Long> res = getUserID(userSub);
        long id;
        if (res.isOK())
            id = res.value();
        else return error(CONFLICT);
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
        Result<Long> res = getUserID(userSub);
        long id;
        if (res.isOK())
            id = res.value();
        else return error(CONFLICT);
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
//TODO COMO È QUE SE FAZ E È SUPOSTO FAZER?
    @Override
    public Result<Void> deleteUserFeed(String user) {
        return error(NOT_IMPLEMENTED);
    }
}
