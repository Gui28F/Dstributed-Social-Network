package sd2223.trab1.kafka.zookeeper;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;


public class Zookeeper implements Watcher {

    private ZooKeeper _client;
    private final int TIMEOUT = 5000;
    private static final String SERVERS = "kafka";
    private static Zookeeper instance;

    public Zookeeper(String path) throws Exception {
        this.connect();
        createNode(path, new byte[0], CreateMode.PERSISTENT);
    }

    public static Zookeeper getInstance(String path) throws Exception {
        if (instance == null)
            instance = new Zookeeper(path);
        return instance;
    }


    public synchronized ZooKeeper client() {
        if (_client == null || !_client.getState().equals(ZooKeeper.States.CONNECTED)) {
            throw new IllegalStateException("ZooKeeper is not connected.");
        }
        return _client;
    }

    private void connect() throws IOException, InterruptedException {
        var connectedSignal = new CountDownLatch(1);
        _client = new ZooKeeper(SERVERS, TIMEOUT, (e) -> {
            if (e.getState().equals(Watcher.Event.KeeperState.SyncConnected)) {
                connectedSignal.countDown();
            }
        });
        connectedSignal.await();
    }

    public String createNode(String path, byte[] data, CreateMode mode) {
        try {
            return client().create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
        } catch (KeeperException.NodeExistsException x) {
            return path;
        } catch (Exception x) {
            x.printStackTrace();
            return null;
        }
    }

    public List<String> getChildren(String path) {
        try {
            return client().getChildren(path, false);
        } catch (Exception x) {
            x.printStackTrace();
        }
        return Collections.emptyList();
    }

    public List<String> getChildren(String path, Watcher watcher) {
        try {
            return client().getChildren(path, watcher);
        } catch (Exception x) {
            x.printStackTrace();
        }
        return Collections.emptyList();
    }

    @Override
    public void process(WatchedEvent event) {
        // TODO Auto-generated method stub
        System.err.println(event);
    }
}
