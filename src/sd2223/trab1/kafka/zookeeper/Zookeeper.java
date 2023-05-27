package sd2223.trab1.kafka.zookeeper;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class Zookeeper implements Watcher {

	private ZooKeeper _client;
	private final int TIMEOUT = 5000;
	
	public Zookeeper(String servers) throws Exception {
		this.connect(servers, TIMEOUT);
	}

	public synchronized ZooKeeper client() {
		if (_client == null || !_client.getState().equals(ZooKeeper.States.CONNECTED)) {
			throw new IllegalStateException("ZooKeeper is not connected.");
		}
		return _client;
	}

	private void connect(String host, int timeout) throws IOException, InterruptedException {
		var connectedSignal = new CountDownLatch(1);
		_client = new ZooKeeper(host, TIMEOUT, (e) -> {
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

	public static void main(String[] args) throws Exception {

		String host = args.length == 0 ? "localhost" : args[0];
		
		var zk = new Zookeeper(host);

		String root = "/directory";

		var path = zk.createNode(root, new byte[0], CreateMode.PERSISTENT);
		System.err.println( path );
		
		zk.getChildren(root).forEach(System.out::println);

		for( int i = 0; i < 10; i++) {
			var newpath = zk.createNode(root + "/", new byte[0], CreateMode.EPHEMERAL_SEQUENTIAL);
			System.err.println( newpath );
		}

		zk.getChildren(root, (e) -> {
			zk.getChildren(root).forEach( System.out::println );
		});

		Thread.sleep(Integer.MAX_VALUE);
	}

	@Override
	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub
		System.err.println(event);
	}
}
