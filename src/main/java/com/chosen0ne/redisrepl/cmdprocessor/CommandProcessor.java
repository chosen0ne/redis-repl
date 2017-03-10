package com.chosen0ne.redisrepl.cmdprocessor;

import com.chosen0ne.redisrepl.protocol.RedisArray;
import com.chosen0ne.redisrepl.protocol.RedisMessage;
import com.chosen0ne.redisrepl.protocol.RedisString;
import com.chosen0ne.redisrepl.util.RedisMessageUtils;
import com.chosen0ne.redisrepl.util.LogUtils;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class CommandProcessor {
    private static Logger logger = Logger.getLogger(CommandProcessor.class);

    public boolean process(RedisArray cmd, Map<String, Object> attrs) {
        if (!RedisMessageUtils.isValidCmd(cmd)) {
            LogUtils.error(logger, "an unvalid redis command, msg:{0}", cmd);
            return false;
        }

        byte[] cmdBytes = RedisMessageUtils.fetchCmdName(cmd);
        byte[] keyBytes = RedisMessageUtils.fetchKey(cmd);
        List<byte[]> vals = new ArrayList<byte[]>();
        List<RedisMessage<?>> elements = cmd.value();
        for (int i=2; elements!=null && i<elements.size(); i++) {
            RedisString e = (RedisString) elements.get(i);
            vals.add(e.value());
        }

        return process(cmdBytes, keyBytes, vals, attrs);
    }

    // Command processor that users need to implement.
    // User can process the command based on the 'attrs'. e.g.:
    //  public boolean process(
    //          byte[] cmdBytes,
    //          byte[] keyBytes,
    //          List<byte[]> val,
    //          Map<String, Object> attrs) {
    //      String hostPort = (String) attrs.get("HOST_PORT");
    //      // get DAO base the host port.
    //      RedisDao dao = ...;
    //      // other process
    //  }
    //
    // NOTICE:
    //      Not thread-safe.
    // args:
    //      cmdBytes: command name
    //      keyBytes: key. null if the command hasn't a key
    //      vals: values. null if the command hasn't values
    //      attrs: custom attributes that user passed
    public abstract boolean process(
            byte[] cmdBytes,
            byte[] keyBytes,
            List<byte[]> vals,
            Map<String, Object> attrs);
}
