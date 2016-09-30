package com.james.zookeeper;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class Zookeeper_GetChildren_API_Sync_Usage implements Watcher {
    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);
    private static ZooKeeper zk = null;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        String path = "/zk-book";
        zk = new ZooKeeper("localhost:2181", 5000, new Zookeeper_GetChildren_API_Sync_Usage());

        connectedSemaphore.await();

        //zk.create(path, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create(path + "/c3", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        List<String> childrenList = zk.getChildren(path, true);
        System.out.println(childrenList);

        zk.create(path + "/c4", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        childrenList = zk.getChildren(path, true);
        System.out.println(childrenList);
        Thread.sleep(Integer.MAX_VALUE);
    }

    @Override
    public void process(WatchedEvent event) {
        if (KeeperState.SyncConnected == event.getState()) {
            connectedSemaphore.countDown();
        } else if (event.getType() == EventType.NodeChildrenChanged) {
            try {
                System.out.println("ReGet Child: " + zk.getChildren(event.getPath(), true));
            } catch (Exception e) {
                // do nothing
            }
        }
    }
}