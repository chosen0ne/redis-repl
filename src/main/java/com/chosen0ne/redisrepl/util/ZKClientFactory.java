package com.chosen0ne.redisrepl.util;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;


public class ZKClientFactory {
    private static Logger logger = Logger.getLogger(ZKClientFactory.class);
    private static CuratorFramework client = null;

    ZKClientFactory() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                if (client != null) {
                    client.close();
                }
            }
        });
    }

    public static void loadKeeperConfigAndStart() {
        String connString = Conf.getZkConnectString();
        int sessionTimeoutMs = Conf.getZkSessionTimeoutMs();
        int connectionTimeoutMs = Conf.getZkConnTimeoutMs();
        int baseSleepTimeMs = Conf.getZkBaseSleepTimeMs();
        int maxRetries = Conf.getZkMaxRetry();
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(
                baseSleepTimeMs,
                maxRetries);
        client = CuratorFrameworkFactory.newClient(
                connString,
                sessionTimeoutMs,
                connectionTimeoutMs,
                retryPolicy);

        client.start();
    }

    public static CuratorFramework getCli(String namespace) {
        if (client == null) {
            loadKeeperConfigAndStart();
        }
        return client.usingNamespace(namespace);
    }
}
