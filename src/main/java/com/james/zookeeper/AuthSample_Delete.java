package com.james.zookeeper;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

public class AuthSample_Delete {
    final static String PATH = "/zk-book-auth1";
    final static String PATH2 = "/zk-book-auth1/child";

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        ZooKeeper zookeeper1 = new ZooKeeper("localhost:2181", 5000, null);
        zookeeper1.addAuthInfo("digest", "foo:true".getBytes());
        zookeeper1.create(PATH, "init".getBytes(), Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
        zookeeper1.create(PATH2, "init".getBytes(), Ids.CREATOR_ALL_ACL, CreateMode.EPHEMERAL);

        try {
            ZooKeeper zookeeper2 = new ZooKeeper("localhost:2181", 50000, null);
            zookeeper2.delete(PATH2, -1);
        } catch (Exception e) {
            System.out.println("zookeeper2 failed to delete " + PATH2);
        }

        ZooKeeper zookeeper3 = new ZooKeeper("localhost:2181", 50000, null);
        zookeeper3.addAuthInfo("digest", "foo:true".getBytes());
        zookeeper3.delete(PATH2, -1);
        System.out.println("zookeeper3 succesfully delete " + PATH2);

        ZooKeeper zookeeper4 = new ZooKeeper("localhost:2181", 50000, null);
        zookeeper4.delete(PATH, -1);
        System.out.println("zookeeper4 succesfully delete " + PATH);
    }
}
