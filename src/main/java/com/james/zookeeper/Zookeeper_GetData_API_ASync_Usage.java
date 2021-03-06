package com.james.zookeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class Zookeeper_GetData_API_ASync_Usage implements Watcher {
    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);
    private static ZooKeeper zk = null;
    private static Stat stat = new Stat();

    public static void main(String[] args) throws InterruptedException, IOException, KeeperException {
        String path = "/zk-book-data";
        zk = new ZooKeeper("localhost:2181", 5000, new Zookeeper_GetData_API_ASync_Usage());

        connectedSemaphore.await();

        zk.create(path, "123".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        zk.getData(path, true, new IDataCallback(), -1);
        zk.setData(path, "123".getBytes(), 1);

        Thread.sleep(Integer.MAX_VALUE);
    }

    @Override
    public void process(WatchedEvent event) {
        if (KeeperState.SyncConnected == event.getState()) {
            connectedSemaphore.countDown();
        } else if (event.getType() == EventType.NodeDataChanged) {
            try {
                System.out.println(new String(zk.getData(event.getPath(), true, stat)));
                System.out.println(stat.getCzxid() + ", " + stat.getMzxid() + ", " + stat.getVersion());
            } catch (Exception e) {
                // do nothing
            }
        }
    }
}

class IDataCallback implements AsyncCallback.DataCallback {
    @Override
    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
        System.out.println(rc + ", " + path + ", " + new String(data));
        System.out.println(stat.getCzxid() + ", " + stat.getVersion());
    }
}