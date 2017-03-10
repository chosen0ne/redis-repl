package com.chosen0ne.redisrepl.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import org.apache.log4j.Logger;

import com.chosen0ne.redisrepl.util.LogUtils;
import com.chosen0ne.redisrepl.RedisSlave;
import com.chosen0ne.redisrepl.progress.ReplProgress;
import com.chosen0ne.redisrepl.progress.ReplProgress.Progress;
import com.chosen0ne.redisrepl.protocol.Constants;
import com.chosen0ne.redisrepl.protocol.RedisMessage;
import com.chosen0ne.redisrepl.util.RedisMessageUtils;

public class SyncHandler extends ChannelInboundHandlerAdapter {
    private static Logger logger = Logger.getLogger(SyncHandler.class);
    
    private RedisSlave slave_;

    public SyncHandler(RedisSlave slave) {
        slave_ = slave;
    }

    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        RedisMessage<?> cmd = (RedisMessage<?>) msg;
        LogUtils.debug(logger, "recv message: {0}", cmd);

        // current 'replStatus == ReplStatus.REPL_PING_SENT'
        if (slave_.getReplStatus() == RedisSlave.ReplStatus.REPL_PING_SENT && 
                RedisMessageUtils.isPong(cmd)) {
            slave_.setReplStatus(RedisSlave.ReplStatus.REPL_PONG_RECV);

            // Fetch replication progress from data store
            Progress progress = ReplProgress.getProgress(slave_.getHostport());
            if (progress == null) {
                LogUtils.error(logger, "failed to get progress info from redis, stop");
                slave_.stop();
                return;
            }

            slave_.getCoreData().masterRunId_ = progress.runid;
            slave_.getCoreData().masterReplOffset_.set(progress.offset);
            LogUtils.info(logger, "load progress, offset:{0}, master: {1}",
                    progress.offset,
                    slave_.masterInfo());

            // Send 'PSYNC' command to master to request a partial replication.
            RedisMessage<?> psync = RedisMessageUtils.makeStringArray(
                    "PSYNC",
                    progress.runid,
                    String.valueOf(progress.offset + 1));
            ctx.writeAndFlush(psync);

        } else if (RedisMessageUtils.prefixMatch(cmd, Constants.FULL_RESYNC)) {
            LogUtils.info(logger, "MASTER <-> SLAVE sync: Need a full sync with master, master:{0}",
                    slave_.masterInfo());

            byte[] cmdBytes = RedisMessageUtils.fetchCmdName(cmd);
            if (cmdBytes == null) {
                LogUtils.error(logger, "an unexpected response");
                slave_.stop();
                return;
            }

            String[] parts = new String(cmdBytes, Constants.UTF8).split(" ");
            slave_.getCoreData().masterRunId_ = parts[1];
            slave_.getCoreData().masterReplOffset_.set(Long.parseLong(parts[2]));

            LogUtils.info(logger, "MASTER <-> SLAVE sync: Start to receive RDB, master:{0}, runid:{1}" +
                    ", offset:{2}",
                    slave_.masterInfo(),
                    parts[1],
                    parts[2]);

            // Change handler to process RDB receiving.
            slave_.registerRDBHandler();
            slave_.setReplStatus(RedisSlave.ReplStatus.REPL_TRANSFER);
            slave_.getCoreData().rdbSaveBeginMs_ = System.currentTimeMillis();

        } else if (RedisMessageUtils.prefixMatch(cmd, Constants.CONTINUE)) {
            LogUtils.info(logger, "MASTER <-> SLAVE sync: Master accepted a Partial " +
                    "Resynchronization, master:{0}",
                    slave_.masterInfo());

            // When partial replication is accepted, master doesn't need to do RDB.
            // The mutex is released as soon as possible.
            slave_.markReleaseMutex();

            slave_.registerOnlineHandler();
            slave_.setReplStatus(RedisSlave.ReplStatus.REPL_ONLINE);
        }
    }

    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LogUtils.error(logger, cause, "an exception is caught. restart slave, master:{0}",
                slave_.masterInfo());
        slave_.stop();
    }
}
