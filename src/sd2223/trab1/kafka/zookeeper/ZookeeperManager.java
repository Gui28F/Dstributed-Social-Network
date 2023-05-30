package sd2223.trab1.kafka.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import sd2223.trab1.api.java.Result;
import sd2223.trab1.api.rest.FeedsService;
import sd2223.trab1.clients.rest.RestFeedsClient;
import sd2223.trab1.servers.Domain;
import sd2223.trab1.servers.java.AbstractServer;
import sd2223.trab1.servers.java.JavaFeedsCommon;
import sd2223.trab1.servers.rest.AbstractRestServer;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

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

    private String getMostUpdatedChildren() {
        String mostRecentServerURI = null;
        long maxVersion = 0L;
        for (String path : client.getChildren(ROOT, this)) {
            String serverURI = client.getData(path);
            RestFeedsClient c = new RestFeedsClient(serverURI);
            Result<Long> res = c.getServerVersion(JavaFeedsCommon.secret);
            if (res.isOK()) {
                if (res.value() > maxVersion) {
                    maxVersion = res.value();
                    mostRecentServerURI = serverURI;
                }
            }
        }
        return mostRecentServerURI;
    }

    private Result<String> getServerInfo(String serverURI) {
        RestFeedsClient c = new RestFeedsClient(serverURI);
        return c.getServerInfo(JavaFeedsCommon.secret);
    }

    private Result<Void> postServerInfo(String serverURI, String info) {
        RestFeedsClient c = new RestFeedsClient(serverURI);
        return c.postServerInfo(JavaFeedsCommon.secret, info);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println("process" + watchedEvent.toString());
        if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
            if (Objects.equals(client.getData(watchedEvent.getPath()), primaryURI)) {
                selectPrimary(client.getChildren(ROOT, this));
                String mostUpdatedServerURI = getMostUpdatedChildren();
                if (!mostUpdatedServerURI.equals(primaryURI)) {
                    Result<String> serverInfo = getServerInfo(mostUpdatedServerURI);
                    postServerInfo(primaryURI, serverInfo.value());
                }
            }
        }
    }
}
