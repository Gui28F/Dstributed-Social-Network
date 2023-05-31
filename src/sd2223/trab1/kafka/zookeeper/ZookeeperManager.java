package sd2223.trab1.kafka.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import sd2223.trab1.api.java.Result;
import sd2223.trab1.api.rest.FeedsService;
import sd2223.trab1.clients.rest.RestFeedsClient;
import sd2223.trab1.servers.java.JavaFeedsCommon;
import sd2223.trab1.servers.rest.AbstractRestServer;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import static sd2223.trab1.clients.Clients.FeedsClients;

public class ZookeeperManager implements Watcher {
    private static final String ROOT = "/feedsSystem";
    private static ZookeeperManager instance;
    private Zookeeper client;
    private String node;
    private String primaryURI;
    private boolean isPrimary;


    private ZookeeperManager() {
        // FeedsClients.get(Domain.get());
        try {
            System.out.println(1);
            this.client = Zookeeper.getInstance(ROOT);
            System.out.println(2);
            node = client.createNode(ROOT + FeedsService.PATH + "_", AbstractRestServer.SERVER_URI.getBytes(), CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println(3);
            selectPrimary(client.getChildren(ROOT, this));
            System.out.println(4);
            isPrimary = false;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static ZookeeperManager getInstance() {
        try {
            if (instance == null) {
                instance = new ZookeeperManager();
            }
        } catch (Exception e) {
        }
        return instance;
    }

    public boolean isPrimary() {
        return isPrimary;
    }

    public String getPrimaryURI() {
        return primaryURI;
    }

    private void selectPrimary(List<String> nodes) {
        Collections.sort(nodes);
        String primary = ROOT + '/' + nodes.get(0);
        if (node.equals(primary)) {
            isPrimary = true;

        } else {
            primaryURI = client.getData(primary);
            isPrimary = false;
        }

    }

    private Pair getMostUpdatedChildren() {
        String mostRecentServerURI = null;
        long maxVersion = 0L;
        for (String path : client.getChildren(ROOT, this)) {
            String serverURI = client.getData(path);
            RestFeedsClient c = new RestFeedsClient(serverURI);
            Result<Long> res = c.getServerVersion(JavaFeedsCommon.secret);
            if (res.isOK()) {
                if (res.value() > maxVersion || (res.value() == maxVersion && Objects.equals(serverURI, primaryURI))) {
                    maxVersion = res.value();
                    mostRecentServerURI = serverURI;
                }
            }
        }
        return new Pair(mostRecentServerURI, maxVersion);
    }

    private Result<String> getServerInfo(String serverURI) {
        RestFeedsClient c = new RestFeedsClient(serverURI);
        return c.getServerInfo(JavaFeedsCommon.secret);
    }

    private Result<Void> postServerInfo(String serverURI, String info, long version) {
        RestFeedsClient c = new RestFeedsClient(serverURI);
        return c.postServerInfo(JavaFeedsCommon.secret, info, version);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
            if (Objects.equals(client.getData(watchedEvent.getPath()), primaryURI)) {
                selectPrimary(client.getChildren(ROOT, this));
                Pair mostUpdatedServer = getMostUpdatedChildren();
                if (!mostUpdatedServer.getServerURI().equals(primaryURI)) {
                    Result<String> serverInfo = getServerInfo(mostUpdatedServer.getServerURI());
                    postServerInfo(primaryURI, serverInfo.value(), mostUpdatedServer.getVersion());
                }
            }
        }
    }
}

class Pair {
    private long version;
    private String serverURI;

    public Pair(String serverURI, long version) {
        this.serverURI = serverURI;
        this.version = version;
    }

    public long getVersion() {
        return version;
    }

    public String getServerURI() {
        return serverURI;
    }
}
