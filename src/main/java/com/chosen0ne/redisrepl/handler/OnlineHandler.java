package com.chosen0ne.redisrepl.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import org.apache.log4j.Logger;

import com.chosen0ne.redisrepl.util.LogUtils;
import com.chosen0ne.redisrepl.RedisSlave;
import com.chosen0ne.redisrepl.protocol.Constants;
import com.chosen0ne.redisrepl.protocol.RedisArray;
import com.chosen0ne.redisrepl.protocol.RedisMessage;
import com.chosen0ne.redisrepl.util.RedisMessageUtils;

public class OnlineHandler extends ChannelInboundHandlerAdapter {
    private static Logger logger = Logger.getLogger(OnlineHandler.class);

    private RedisSlave slave_;

    public OnlineHandler(RedisSlave slave) {
        slave_ = slave;
    }

    public void channelRead(ChannelHandlerContext ctx, Object buf) {
        RedisMessage<?> msg = (RedisMessage<?>) buf;
        LogUtils.debug(logger, "recv msg from {0}, msg: {1}", slave_.getHostport(), msg);

        if (!RedisMessageUtils.isValidCmd(msg)) {
            LogUtils.error(logger, "recv an unvalid command, msg:{0}", msg);
            return;
        }

        RedisArray cmd = (RedisArray) msg;
        slave_.getCmdCounter().inc();

        if (RedisMessageUtils.isPing(msg)) {
            // Master sends PING periodically to make sure slave can timeout explicitly.
            // 'lastIOtimestamp' has been updated in the first handler.
            LogUtils.debug(logger, "recv ping from master");
        } else {
            // Other commands which update the state of redis will be processed.
            if (!slave_.processCommand(cmd)) {
                LogUtils.error(logger, "failed to process redis command, cmd:{0}", cmd);
            }
        }

        // Update replication progress
        int cmdByteLen = (Integer) msg.getAttr(Constants.MSG_BYTES);
        slave_.getCoreData().masterReplOffset_.addAndGet(cmdByteLen);
    }

    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LogUtils.error(logger, cause, "an exception is caught. restart slave, master:{0}",
                slave_.masterInfo());
        slave_.stop();
    }
}
