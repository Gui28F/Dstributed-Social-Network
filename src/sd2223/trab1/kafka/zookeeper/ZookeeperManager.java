package sd2223.trab1.kafka.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import sd2223.trab1.api.rest.FeedsService;
import sd2223.trab1.servers.Domain;

import static sd2223.trab1.clients.Clients.FeedsClients;

public class ZookeeperManager implements Watcher {
    private static final String ROOT = "/feedsSystem";
    private static ZookeeperManager instance;
    private Zookeeper client;
    private String node;

    public ZookeeperManager() {
       // FeedsClients.get(Domain.get());
        try {
            this.client = Zookeeper.getInstance(ROOT);
            node = client.createNode(ROOT + FeedsService.PATH + "_", FeedsClients.get(Domain.get()).toString().getBytes(), CreateMode.EPHEMERAL_SEQUENTIAL);
            //selectPrimary(client.getChildren(ROOT, this));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static ZookeeperManager getInstance() throws Exception {
        if (instance == null)
            instance = new ZookeeperManager();
        return instance;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {

    }
}
