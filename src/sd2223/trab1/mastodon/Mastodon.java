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


    /*private static final String clientKey = "PjbCuhbYnjitka6UYn6onK2CX32jMYPqUfGovsOnwIA";
    private static final String clientSecret = "yTtbhCVIIPoFUpVyfjZyfKhUCslM14uj09vxn76QnDc";
    private static final String accessTokenStr = "yhWvdCGdiA7h-yQIpqAqDcGGeYgcOgrsHRmfhv9FG3U";
    */
    //quim_coubes
    /*private static final String clientKey = "z59r22ZOfNEhS7S6JG8J6uhELhKv29zJhrNYcKWkmOs";
    private static final String clientSecret = "O8nk_cHc_A0c_cYR4XINJ-abYJbtOl6vW9UArYCr7Ms";
    private static final String accessTokenStr = "-Qwar5svmwKh1yexOeCr4ONvYmMG8m8DC2eFWH0-ZyE";
*/

    /* private static final String clientKey = "df6stWVa3_nYHJKE-Rq2EIO-6Bjdwej707h2wgtQjV0";
     private static final String clientSecret = "maJWFGlXqirEQS0y9oSJcIXyhU2-0zJj7liyNEsAFbc";
     private static final String accessTokenStr = "Lx2ZetIS2xYCjzaJzUT-Nc2ddlpk7UuilVYBRzSJ7UI";
 */
    //NOVA
    private static final String clientKey = "bTsA8mwUlJmbDI2jdpOiL1NI6L8WdsyPrIaMYmSMHQI";
    private static final String clientSecret = "DGkAHzR1InSQ7E07u7mUWAwuAprf8-Issva0sXLYunMc";
    private static final String accessTokenStr = "Avypx1TkKj1oXlyZcJ7qcgGPlyxKe8npFoW2_6ZraoI";

    static final String STATUSES_PATH = "/api/v1/statuses";
    static final String TIMELINES_PATH = "/api/v1/timelines/home?since_id=";
    static final String ACCOUNT_FOLLOWING_PATH = "/api/v1/accounts/%s/following";
    static final String VERIFY_CREDENTIALS_PATH = "/api/v1/accounts/verify_credentials";
    static final String SEARCH_ACCOUNTS_PATH = "/api/v1/accounts/search?q=";
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
            //  cleanStatus();
        } catch (Exception x) {
            x.printStackTrace();
            System.exit(0);
        }
    }

    synchronized public static Mastodon getInstance() {
        if (impl == null) {
            impl = new Mastodon();
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
            long id = time;
            // id += random.nextLong(1000);
            //System.out.println(id);
            id = id << 16;
            id++;
            // System.out.println(id);
            // id += random.nextLong((long) Math.pow(2, 16));
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
        try {
            final OAuthRequest request = new OAuthRequest(Verb.GET, getEndpoint(STATUSES_PATH + "/" + mid));

            service.signRequest(accessToken, request);

            Response response = service.execute(request);
            if (response.getCode() == HTTP_OK) {
                System.out.println(response.getBody());
                PostStatusResult res = JSON.decode(response.getBody(), new TypeToken<PostStatusResult>() {
                });
                //Message msg = res.toMessage();
                // fillMsg(msg, user);
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
                    //TODO retorna o que se a pesquisa der mais do que um user?
                else return error(NOT_FOUND);
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
    public Result<Void> deleteUserFeed(String user, String secret) {
        return error(NOT_IMPLEMENTED);
    }
}
