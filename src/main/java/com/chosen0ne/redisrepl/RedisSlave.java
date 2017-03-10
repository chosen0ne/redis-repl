package com.chosen0ne.redisrepl;

import com.chosen0ne.redisrepl.util.*;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.log4j.Logger;

import com.chosen0ne.redisrepl.cmdprocessor.CommandProcessor;
import com.chosen0ne.redisrepl.codec.RedisDecoder;
import com.chosen0ne.redisrepl.codec.RedisEncoder;
import com.chosen0ne.redisrepl.handler.OnlineHandler;
import com.chosen0ne.redisrepl.handler.RDBHandler;
import com.chosen0ne.redisrepl.handler.SyncHandler;
import com.chosen0ne.redisrepl.progress.ReplProgress;
import com.chosen0ne.redisrepl.protocol.RedisArray;
import com.chosen0ne.redisrepl.protocol.RedisMessage;

public class RedisSlave {
    private static Logger logger = Logger.getLogger(RedisSlave.class);

    private static int MASTER_TIMEOUT_MSEC = 15 * 1000;
    private static String MUTEX_PATH_IN_ZK = "warden_mutex_by_machine";

    private String host_;
    private int port_;
    private String hostPort_;
    private Map<String, Object> attrs_;

    private volatile ReplStatus replStatus_;
    private boolean isStopped_;

    private ChannelFuture channelFuture_;
    private ChannelPipeline channelPipeline_;

    // Used to represent state of slave
    private ReplCoreData coreData_ = new ReplCoreData();
    private Counter cmdCounter_ = new Counter();
    private long timeEventLoops_ = 0;

    // Inter-process mutex implemented by zookeeper is used to serialize all
    // the replication from one master, to make sure that no more than one master
    // can do RDB.
    // Due to the restriction of Curator that 'acquire' and 'release' must be
    // called in the same thread. We just mark 'needReleaseMutex_' to true when
    // we release the mutex. And the thread in 'RedisSlaveMgr' which has tried to
    // acquire the mutex will release it asynchronously.
    private InterProcessMutex mutex_;
    private volatile boolean needReleaseMutex_;

    // Avoid to allocate and deallocate buffer to dump each command.
    private ByteArrayOutputStream cmdDumpOs_ = new ByteArrayOutputStream(Conf.getMsgMaxBytes());
    // Store the buffer of online replication when the TCP packet consists of RDB
    // and online data.
    private ByteBuf onlineBuf_;

    private CommandProcessor processor_;

    RedisSlave(
            String host,
            int port,
            CommandProcessor processor,
            Map<String, Object> attrs) {
        host_ = host;
        port_ = port;
        attrs_ = attrs;
        processor_ = processor;

        hostPort_ = host + ":" + port;
        replStatus_ = ReplStatus.REPL_CONNECT;
        isStopped_ = true;

        CuratorFramework cli = ZKClientFactory.getCli(MUTEX_PATH_IN_ZK);
        mutex_ = new InterProcessMutex(cli, "/" + host_);
    }

    public void stop() {
        if (channelFuture_.channel().eventLoop().isShuttingDown()) {
            LogUtils.info(logger, "slave has already been shutting down. wait...");
            return;
        }

        replStatus_ = ReplStatus.REPL_CONNECT;
        isStopped_ = true;
        try {
            channelFuture_.channel().close();
            channelFuture_.channel().eventLoop().shutdownGracefully();
        } catch (Exception e) {
            LogUtils.error(logger, e, "failed to stop the slave, master:{0}", masterInfo());
            return;
        }

        markReleaseMutex();

        LogUtils.info(logger, "stop the slave, master:{0}", masterInfo());
    }

    void start() {
        if (!acquireMutex()) {
            return;
        }

        EventLoopGroup workerGroup = new NioEventLoopGroup(2);
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 2000)
                .handler(new ChannelInitializer<SocketChannel>() {

                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        channelPipeline_ = ch.pipeline();
                        registerSyncHandler();
                    }
                });

        isStopped_ = false;

        if (!connectWithMaster(bootstrap)) {
            LogUtils.error(logger, "failed to connect to master, stop trying to " +
                    "replication, master:{0}", masterInfo());
            stop();
            return;
        }

        // Send ping to make sure master is alive when slave does high-weight
        // blocking operation.
        RedisMessage<?> cmd = RedisMessageUtils.makeStringArray("PING");
        channelFuture_.channel().writeAndFlush(cmd);
        coreData_.lastIOtimestamp_ = System.currentTimeMillis();
        replStatus_ = ReplStatus.REPL_PING_SENT;

        addTimeEvent();
    }

    private boolean connectWithMaster(Bootstrap bootstrap) {
        for (int i = 0; !isStopped_ && i < Conf.getConnectTimes(); i++) {
            channelFuture_ = bootstrap.connect(host_, port_);
            try {
                channelFuture_.sync();
            } catch (InterruptedException e) {
                LogUtils.error(logger, e, "interrupted when connect to master, master:{0}", masterInfo());
            } catch (Exception e) {
                // Connection exception, no need to logging.
            }

            if (channelFuture_.isSuccess()) {
                replStatus_ = ReplStatus.REPL_CONNECTED;
                LogUtils.info(logger, "successful to connect to master, master:{0}", masterInfo());
                return true;
            }

            LogUtils.error(logger, "failed to connect to master, master:{0}", masterInfo());
            try {
                Thread.sleep(Conf.getConnectIntervalSec() * 1000);
            } catch (InterruptedException e) {
                // Ignore!
            }
        }

        return false;
    }

    private void addTimeEvent() {
        channelFuture_.channel().eventLoop().scheduleAtFixedRate(new Runnable() {
            private long lastAckOffset = -1;

            @Override
            public void run() {
                timeEventLoops_++;

                // Send ack to master
                if (!coreData_.masterReplOffset_.equals(lastAckOffset) &&
                        replStatus_ == ReplStatus.REPL_ONLINE) {
                    RedisMessage<?> replConf = RedisMessageUtils.makeStringArray(
                            "REPLCONF",
                            "ACK",
                            coreData_.masterReplOffset_ + "");
                    channelFuture_.channel().writeAndFlush(replConf);
                    lastAckOffset = coreData_.masterReplOffset_.get();

                    // Update replication progress in data store
                    ReplProgress.setProgress(getHostport(), lastAckOffset);
                }

                // Check whether master is timeout?
                boolean needCheckTimeout = (replStatus_ == ReplStatus.REPL_CONNECT ||
                        replStatus_ == ReplStatus.REPL_PING_SENT ||
                        replStatus_ == ReplStatus.REPL_TRANSFER ||
                        replStatus_ == ReplStatus.REPL_ONLINE);
                if (needCheckTimeout && 
                        (System.currentTimeMillis() - coreData_.lastIOtimestamp_ > MASTER_TIMEOUT_MSEC)) {
                    LogUtils.error(logger, "master is timeout, connection will be closed. master:{0}, status:{1}",
                            masterInfo(),
                            replStatus_);
                    stop();
                }

                // Send '\n' to master, to avoid timeout by master when loading RDB
                if (runWithPeriod(5) && replStatus_ == ReplStatus.REPL_LOADING) {
                    sendNewlineToMaster();
                }
            }
        }, 1, Conf.getReplBasicTimerIntervalSec(), TimeUnit.SECONDS);
    }

    public ReplStatus getReplStatus() {
        return replStatus_;
    }

    public void setReplStatus(ReplStatus replStatus) {
        this.replStatus_ = replStatus;
    }

    private void removeHandlers() {
        List<String> names = channelPipeline_.names();
        for (String name: names) {
            if (name.startsWith("DefaultChannelPipeline")) {
                continue;
            }
            channelPipeline_.remove(name);
        }
    }

    public void registerDefaultHandler(String name, ChannelInboundHandler handler) {
        channelPipeline_.addLast("decoder", new RedisDecoder(this))
                .addLast("encoder", new RedisEncoder())
                .addLast(name, handler);
    }

    public void registerSyncHandler() {
        registerDefaultHandler("sync-handler", new SyncHandler(this));
    }

    public void registerOnlineHandler() {
        if (channelPipeline_.get("decoder") == null) {
            // Full replication. RDBHandler => OnlineHandler
            registerDefaultHandler("online-handler", new OnlineHandler(this));
        } else {
            // Partial replication is accepted. SyncHandler => OnlineHandler
            channelPipeline_.replace("sync-handler", "online-handler", new OnlineHandler(this));
        }
    }

    public void registerRDBHandler() {
        removeHandlers();
        channelPipeline_.addLast("rdb-handler", new RDBHandler(this));
    }

    public void removeRDBHandler() {
        channelPipeline_.remove("rdb-handler");
    }

    public ReplCoreData getCoreData() {
        return coreData_;
    }

    public void setCoreData(ReplCoreData coreData) {
        this.coreData_ = coreData;
    }

    public String getHost() {
        return host_;
    }

    public void setHost(String host) {
        this.host_ = host;
    }

    public int getPort() {
        return port_;
    }

    public void setPort(int port) {
        this.port_ = port;
    }

    public String getHostport() {
        return hostPort_;
    }

    public Counter getCmdCounter() {
        return cmdCounter_;
    }

    public boolean hasStopped() {
        return isStopped_;
    }

    private boolean runWithPeriod(int nsecs) {
        if (timeEventLoops_ % nsecs == 0) {
            return true;
        }
        return false;
    }

    private void sendNewlineToMaster() {
        ByteBufAllocator alloc = channelFuture_.channel().alloc();
        ByteBuf buf = alloc.buffer(1);
        buf.writeByte('\n');
        channelFuture_.channel().writeAndFlush(buf);
    }

    // Serialize all the replication from one machine, to make sure that no more
    // than one master can do RDB.
    private boolean acquireMutex() {
        try {
            if (mutex_.acquire(2, TimeUnit.SECONDS)) {
                LogUtils.info(logger, "successfully to acquire mutex, master:{0}", masterInfo());
                return true;
            }
            LogUtils.info(logger, "failed to acquire mutex, master:{0}, others:{1}",
                    masterInfo(), mutex_.getParticipantNodes());
        } catch (Exception e) {
            LogUtils.error(logger, e, "failed to acquire mutex, master:{0}", masterInfo());
        }
        return false;
    }

    public void markReleaseMutex() {
        needReleaseMutex_ = true;
    }

    // TODO: retry when failure
    public void releaseMutex() throws Exception {
        if (needReleaseMutex_ && mutex_.isAcquiredInThisProcess()) {
            mutex_.release();
            LogUtils.info(logger, "successfully to release mutex, master:{0}", masterInfo());
        }

        needReleaseMutex_ = false;
    }

    public String masterInfo() {
        return String.format("%s/%s", host_, port_);
    }

    public ByteArrayOutputStream cmdDumpOs() {
        return cmdDumpOs_;
    }

    public void disableReadEvent() {
        channelPipeline_.channel().config().setAutoRead(false);
    }

    public void enableReadEvent() {
        channelPipeline_.channel().config().setAutoRead(true);
    }

    public void setOnlieBuf(ByteBuf buf) {
        onlineBuf_ = buf;
    }

    public ByteBuf getOnlineBuf() {
        return onlineBuf_;
    }

    public boolean processCommand(RedisArray cmd) {
        return processor_.process(cmd, attrs_);
    }

    public boolean processCommand(byte[] cmdBytes, byte[] keyBytes, List<byte[]> vals) {
        return processor_.process(cmdBytes, keyBytes, vals, attrs_);
    }

    public enum ReplStatus {
        REPL_CONNECT,
        REPL_CONNECTED,
        REPL_PING_SENT,
        REPL_PONG_RECV,
        REPL_TRANSFER,
        REPL_LOADING,
        REPL_ONLINE,
        REPL_OFFLINE,
    }
}
