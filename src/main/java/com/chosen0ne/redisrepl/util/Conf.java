package com.chosen0ne.redisrepl.util;

import com.chosen0ne.redisrepl.util.LogUtils;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Conf {
    private static final Logger logger = Logger.getLogger(Conf.class);
    private static final Properties properties = new Properties();

    private static final String CONF_FILE = "config.properties";
    private static volatile boolean hasLoaded = false;

    private static int DEFAULT_ZK_SESSION_TO_MS = 3000;
    private static int DEFAULT_ZK_CONN_TO_MS = 3000;
    private static int DEFAULT_ZK_BASE_SLEEP_MS = 1000;
    private static int DEFAULT_ZK_MAX_RETRY = 3;
    private static int DEFAULT_CONNECT_TIMES = 3;
    private static int DEFAULT_CONNECT_INTERVAL = 3;
    private static int DEFAULT_REPL_CRON_INTERVAL = 3;
    private static int DEFAULT_REPL_BASICTIEMR_INTERVAL = 1;
    private static int DEFAULT_WORKER_SLOTS = 20;
    private static int DEFAULT_MSG_MAX_BYTES = 5242880;

    public static void load(String confFilePath) throws IOException {
        if (confFilePath == null) {
            confFilePath = CONF_FILE;
        }

        LogUtils.info(logger, "loading config from file: {0}", confFilePath);
        try {
            properties.load(new FileInputStream(confFilePath));
            LogUtils.info(logger, "config values:\n{0}", prettyFormat());
        } catch (IOException e) {
            LogUtils.error(logger, e, "failed to load conf, file: {0}",
                    confFilePath);
            throw e;
        }

        hasLoaded = true;
    }

    private static String getVal(String key) {
        return properties.getProperty(key);
    }

    private static int getInt(String key, int defaultVal) {
        String val = getVal(key);
        try {
            return Integer.parseInt(val);
        } catch (NumberFormatException e) {
            LogUtils.info(logger, "failed to parse int of '{0}', use default: {1}", key, defaultVal);
            return defaultVal;
        }
    }

    private static boolean getBoolean(String key, boolean defaultVal) {
        String val = getVal(key);
        return "TRUE".equalsIgnoreCase(val);
    }

    public static boolean isLoaded() {
        return hasLoaded;
    }

    public static String getZkConnectString() {
        return getVal("zk.connect.string");
    }

    public static int getZkSessionTimeoutMs() {
        return getInt("zk.session.timeout", DEFAULT_ZK_SESSION_TO_MS);
    }

    public static int getZkConnTimeoutMs() {
        return getInt("zk.conn.timeout", DEFAULT_ZK_CONN_TO_MS);
    }

    public static int getZkBaseSleepTimeMs() {
        return getInt("zk.base.sleep", DEFAULT_ZK_BASE_SLEEP_MS);
    }

    public static int getZkMaxRetry() {
        return getInt("zk.max.retry", DEFAULT_ZK_MAX_RETRY);
    }

    public static boolean isKafkaProducerSync() {
        return getBoolean("kafka.producer.sync", true);
    }

    public static int getConnectTimes() {
        return getInt("connect.times", DEFAULT_CONNECT_TIMES);
    }

    public static int getConnectIntervalSec() {
        return getInt("connect.interval", DEFAULT_CONNECT_INTERVAL);
    }

    public static int getReplCheckIntervalSec() {
        return getInt("repl.check.interval", DEFAULT_REPL_CRON_INTERVAL);
    }

    public static int getReplBasicTimerIntervalSec() {
        return getInt("repl.basictimer.interval", DEFAULT_REPL_BASICTIEMR_INTERVAL);
    }

    public static boolean isKafkaProducerSend() {
        return getBoolean("kafka.producer.send", true);
    }

    public static int getWorkerSlots() {
        return getInt("worker.slots", DEFAULT_WORKER_SLOTS);
    }

    public static int getMsgMaxBytes() {
        return getInt("message.max.bytes", DEFAULT_MSG_MAX_BYTES);
    }

    private static String prettyFormat() {
        StringBuilder sb = new StringBuilder(1024);
        for (Object key: properties.keySet()) {
            sb.append("\t\t").append(key).append("=");
            Object val = properties.get(key);
            sb.append(val).append("\n");
        }

        return sb.toString();
    }
}
