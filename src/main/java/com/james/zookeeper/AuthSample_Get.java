package com.james.zookeeper;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

public class AuthSample_Get {
    final static String PATH = "/zk-book-auth";

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        ZooKeeper zookeeper1 = new ZooKeeper("localhost:2181", 5000, null);
        zookeeper1.addAuthInfo("digest", "foo:true".getBytes());
        zookeeper1.create(PATH, "init".getBytes(), Ids.CREATOR_ALL_ACL, CreateMode.EPHEMERAL);

        ZooKeeper zookeeper2 = new ZooKeeper("localhost:2181", 50000, null);
        zookeeper2.getData(PATH, false, null);
    }
}
