package com.james.zookeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;

public class Zookeeper_Constructor_Usage_With_SID_PASSWD implements Watcher {
    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);

    public static void main(String[] args) throws IOException, InterruptedException {
        ZooKeeper zookeeper = new ZooKeeper("localhost:2181", 5000, new Zookeeper_Constructor_Usage_With_SID_PASSWD());

        connectedSemaphore.await();

        long sessionId = zookeeper.getSessionId();
        byte[] passwd = zookeeper.getSessionPasswd();

        zookeeper = new ZooKeeper("localhost:2181", 5000, new Zookeeper_Constructor_Usage_With_SID_PASSWD(), 1l,
                "test".getBytes());

        zookeeper = new ZooKeeper("localhost:2181", 5000, new Zookeeper_Constructor_Usage_With_SID_PASSWD(), sessionId,
                passwd);

        Thread.sleep(Integer.MAX_VALUE);
    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println("Receive watched event: " + event);

        if (KeeperState.SyncConnected == event.getState()) {
            connectedSemaphore.countDown();
        }
    }
}
