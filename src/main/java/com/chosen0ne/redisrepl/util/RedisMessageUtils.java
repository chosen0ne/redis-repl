package com.chosen0ne.redisrepl.util;

import com.chosen0ne.redisrepl.protocol.Constants;
import com.chosen0ne.redisrepl.protocol.RedisArray;
import com.chosen0ne.redisrepl.protocol.RedisError;
import com.chosen0ne.redisrepl.protocol.RedisInteger;
import com.chosen0ne.redisrepl.protocol.RedisMessage;
import com.chosen0ne.redisrepl.protocol.RedisString;

public class RedisMessageUtils {
    private static String PING_CMD_NAME = "PING";
    private static String PONG_CMD_NAME = "PONG";

    public static boolean isPing(RedisMessage<?> msg) {
        return cmdEquals(msg, PING_CMD_NAME);
    }

    public static boolean isPong(RedisMessage<?> msg) {
        return cmdEquals(msg, PONG_CMD_NAME);
    }

    public static boolean prefixMatch(RedisMessage<?> msg, String cmdPrefix) {
        byte[] cmdBytes = fetchCmdName(msg);
        if (cmdBytes == null) {
            return false;
        }

        String cmdName = new String(cmdBytes, Constants.UTF8);
        if (cmdName.length() < cmdPrefix.length()) {
            return false;
        }

        String s = cmdName.substring(0, cmdPrefix.length());
        return s.equalsIgnoreCase(cmdPrefix);
    }

    public static RedisMessage<?> makeString(String str) {
        return new RedisString(str.getBytes(Constants.UTF8));
    }

    public static RedisMessage<?> makeInt(int i) {
        return new RedisInteger(i);
    }

    public static RedisMessage<?> makeError(String err) {
        return new RedisError(err.getBytes(Constants.UTF8));
    }

    public static RedisMessage<?> makeStringArray(String... strs) {
        RedisArray array = new RedisArray();
        for (String s: strs) {
            array.add(new RedisString(s.getBytes(Constants.UTF8)));
        }

        return array;
    }

    public static RedisMessage<?> makeArray(RedisMessage<?>... msgs) {
        RedisArray array = new RedisArray();
        for (RedisMessage<?> m: msgs) {
            array.add(m);
        }
        return array;
    }

    public static byte[] fetchKey(RedisMessage<?> msg) {
        if (!(msg instanceof RedisArray)) {
            return null;
        }

        RedisArray array = (RedisArray) msg;
        if (array.value().size() < 2) {
            return null;
        }

        RedisMessage<?> m = array.value().get(1);
        if (!(m instanceof RedisString)) {
            return null;
        }

        return ((RedisString) m).value();
    }

    public static byte[] fetchCmdName(RedisMessage<?> msg) {
        byte[] cmdBytes = null;
        if (msg instanceof RedisString) {
            cmdBytes = ((RedisString) msg).value();
        } else if (msg instanceof RedisArray) {
            RedisArray array = (RedisArray) msg;
            if (array.value().size() < 1) {
                return null;
            }
            RedisMessage<?> m = array.value().get(0);
            if (!(m instanceof RedisString)) {
                return null;
            }
            cmdBytes = ((RedisString) m).value();
        } 

        return cmdBytes;
    }

    public static boolean isValidCmd(RedisMessage<?> msg) {
        if (!(msg instanceof RedisArray)) {
            return false;
        }

        RedisArray array = (RedisArray) msg;
        for (RedisMessage<?> e: array.value()) {
            if (!(e instanceof RedisString)) {
                return false;
            }
        }

        return true;
    }

    private static boolean cmdEquals(RedisMessage<?> msg, String cmd) {
        byte[] cmdBytes = fetchCmdName(msg);
        if (cmdBytes == null) {
            return false;
        }

        String cmdName = new String(cmdBytes, Constants.UTF8);
        return cmdName.equalsIgnoreCase(cmd);
    }
}
