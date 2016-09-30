package com.james.zookeeper;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class AuthSample {
    final static String PATH = "/zk-book-auth";

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        ZooKeeper zookeeper = new ZooKeeper("localhost:2181", 50000, null);
        zookeeper.addAuthInfo("digest", "foo:true".getBytes());

        zookeeper.create(PATH, "init".getBytes(), Ids.CREATOR_ALL_ACL, CreateMode.EPHEMERAL);

        Thread.sleep(Integer.MAX_VALUE);
    }
}
