package com.james.zookeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class Zookeeper_Exist_API_Sync_Usage implements Watcher {
    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);
    private static ZooKeeper zk = null;

    public static void main(String[] args) throws InterruptedException, IOException, KeeperException {
        String path = "/zk-book-exist";
        zk = new ZooKeeper("localhost:2181", 5000, new Zookeeper_Exist_API_Sync_Usage());

        connectedSemaphore.await();

        zk.exists(path, true);
        zk.create(path, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.setData(path, "123".getBytes(), -1);
        zk.create(path + "/c1", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.delete(path + "/c1", -1);
        zk.delete(path, -1);

        Thread.sleep(Integer.MAX_VALUE);
    }

    @Override
    public void process(WatchedEvent event) {
        try {
            if (KeeperState.SyncConnected == event.getState()) {
                connectedSemaphore.countDown();
            } else if (event.getType() == EventType.NodeCreated) {
                System.out.println("Node(" + event.getPath() + ") created");
                zk.exists(event.getPath(), true);
            } else if (event.getType() == EventType.NodeDeleted) {
                System.out.println("Node(" + event.getPath() + ") deleted");
                zk.exists(event.getPath(), true);
            } else if (event.getType() == EventType.NodeDataChanged) {
                System.out.println("Node(" + event.getPath() + ") data changed");
                zk.exists(event.getPath(), true);
            }
        } catch (Exception e) {
            // do nothing
        }
    }
}
