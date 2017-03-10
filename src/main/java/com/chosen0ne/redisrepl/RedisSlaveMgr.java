package com.chosen0ne.redisrepl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.chosen0ne.redisrepl.progress.IProgressStore;
import com.chosen0ne.redisrepl.progress.ReplProgress;
import org.apache.log4j.Logger;

import com.chosen0ne.redisrepl.util.LogUtils;
import com.chosen0ne.redisrepl.cmdprocessor.CommandProcessor;
import com.chosen0ne.redisrepl.util.Conf;

public class RedisSlaveMgr {
    private static Logger logger = Logger.getLogger(RedisSlaveMgr.class);

    private Map<String, RedisSlave> slaves_ = new ConcurrentHashMap<String, RedisSlave>();
    private static RedisSlaveMgr instance_;
    private CommandProcessor processor_;
    private IProgressStore progressStore_;
    private volatile boolean started_;

    private ScheduledExecutorService sched_ = Executors.newScheduledThreadPool(1, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r, "slave-checker");
            return thread;
        }
    });

    private RedisSlaveMgr() {
        // TODO: add monitor of stats
    }

    public static RedisSlaveMgr getInstance() {
        if (instance_ != null) {
            return instance_;
        }

        synchronized (RedisSlaveMgr.class) {
            if (instance_ != null) {
                return instance_;
            }

            instance_ = new RedisSlaveMgr();
        }

        return instance_;
    }

    public RedisSlaveMgr conf(String confFilePath) throws IOException {
        Conf.load(confFilePath);

        return instance_;
    }

    public void start() throws Exception {
        if (started_) {
            LogUtils.error(logger, "RedisSlaveMgr is already started!");
            return;
        }

        if (processor_ == null) {
            LogUtils.error(logger, "Command Processor must be provided.");
            throw new RuntimeException("Command Processor must be provided.");
        }

        if (progressStore_ == null) {
            LogUtils.error(logger, "Progress Store must be provided.");
            throw new RuntimeException("progress Store must be provided.");
        }

        if (!Conf.isLoaded()) {
            LogUtils.info(logger, "Config isn't loaded, default config in classpath will be used.");
            Conf.load(null);
        }

        sched_.scheduleAtFixedRate(
                new SlaveCheckRunnable(),
                2,
                Conf.getReplCheckIntervalSec(),
                TimeUnit.SECONDS);

        started_ = true;
    }

    public synchronized void addMaster(
            String host,
            int port,
            Map<String, Object> attrs) {
        String slaveKey = slaveKey(host, port);
        RedisSlave slave = slaves_.get(slaveKey);
        if (slave != null && slave.getReplStatus() != RedisSlave.ReplStatus.REPL_CONNECT) {
            LogUtils.error(logger, "slave of the master is already running, " +
                    "hostPort:{0}/{1}, status:{2}", host, port, slave.getReplStatus());
            return;
        }

        if (processor_ == null) {
            throw new RuntimeException("No command processor.");
        }

        slave = new RedisSlave(host, port, processor_, attrs);
        slaves_.put(slaveKey, slave);

        LogUtils.info(logger, "add the slave of the master, hostPort:{1}/{2}, attrs:{3}",
                host, port, attrs);
    }

    public void removeMaster(String host, int port) {
        String slaveKey = slaveKey(host, port);
        if (!slaves_.containsKey(slaveKey)) {
            LogUtils.error(logger, "master does not exist, hostport:{0}/{1}",
                    host, port);
            return;
        }
        RedisSlave slave = slaves_.get(slaveKey);
        slaves_.remove(slaveKey);
        slave.stop();
        
        LogUtils.info(logger, "remove the slave of the master, hostport:{0}/{1}",
                host, port);
    }

    public RedisSlaveMgr processor(CommandProcessor processor) {
        processor_ = processor;

        return instance_;
    }

    public RedisSlaveMgr progressStore(IProgressStore progressStore) {
        progressStore_ =  progressStore;
        ReplProgress.setProgressStore(progressStore);

        return instance_;
    }

    private String slaveKey(String host, int port) {
        return host + ":" + port;
    }

    class SlaveCheckRunnable implements Runnable {

        @Override
        public void run() {
            LogUtils.debug(logger, "start to check slave-faker");
            List<String> stoppedSlaves = new ArrayList<String>();
            for (String slaveKey: slaves_.keySet()) {
                RedisSlave slave = slaves_.get(slaveKey);
                try {
                    slave.releaseMutex();
                } catch (Exception e) {
                    LogUtils.error(logger, e, "failed to release mutex, master:{0}", slave.masterInfo());
                }

                if (slave.getReplStatus() == RedisSlave.ReplStatus.REPL_CONNECT &&
                        slave.hasStopped()) {
                   stoppedSlaves.add(slaveKey);
                }
            }

            for (String slaveKey: stoppedSlaves) {
                String[] fields = slaveKey.split(":");
                RedisSlave slave = slaves_.get(slaveKey);
                LogUtils.info(logger, "replication from master has stopped, restart it. " +
                        "hostPort:{0}/{1}", fields[0], fields[1]);
                slave.start();
            }
        }
    }

    public Map<String, RedisSlave> getSlaves() {
        return slaves_;
    }

}