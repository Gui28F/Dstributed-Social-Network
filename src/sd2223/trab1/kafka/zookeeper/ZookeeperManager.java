package sd2223.trab1.kafka.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import sd2223.trab1.api.rest.FeedsService;
import sd2223.trab1.servers.Domain;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Objects;

import static sd2223.trab1.clients.Clients.FeedsClients;

public class ZookeeperManager implements Watcher {
    private static final String ROOT = "/feedsSystem";
    private static ZookeeperManager instance;
    private Zookeeper client;
    private String node;
    private URI primaryURI;
    private boolean isPrimary;

    private ZookeeperManager() {
        // FeedsClients.get(Domain.get());
        try {
            this.client = Zookeeper.getInstance(ROOT);
            node = client.createNode(ROOT + FeedsService.PATH + "_", FeedsClients.get(Domain.get()).toString().getBytes(), CreateMode.EPHEMERAL_SEQUENTIAL);
            selectPrimary(client.getChildren(ROOT, this));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static ZookeeperManager getInstance() {
        try {
            if (instance == null)
                instance = new ZookeeperManager();
        } catch (Exception e) {

        }
        return instance;
    }

    public boolean isPrimary() {
        return isPrimary;
    }

    public URI getPrimaryURI() {
        return primaryURI;
    }

    private void selectPrimary(List<String> nodes) {
        String primary = nodes.get(0);
        try {
            if (node.equals(primary))
                isPrimary = true;
            else {
                primaryURI = new URI(client.getData(primary));
                isPrimary = false;
            }
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
   /* private String getMostUpdatedChildren(){
        try {
            for (String path : client.getChildren(ROOT, this)) {
                URI serverURI = new URI(client.getData(path));

            }
        }catch (URISyntaxException e){

        }
    }*/

    @Override
    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getType() == Event.EventType.NodeDeleted) {
            if(Objects.equals(client.getData(watchedEvent.getPath()), primaryURI.toString())) {
                selectPrimary(client.getChildren(ROOT, this));

            }

        }
    }
}
