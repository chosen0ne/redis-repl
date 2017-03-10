package com.chosen0ne.redisrepl.codec;

import com.chosen0ne.redisrepl.protocol.Constants;
import com.chosen0ne.redisrepl.protocol.RedisArray;
import com.chosen0ne.redisrepl.protocol.RedisMessage;
import com.chosen0ne.redisrepl.protocol.RedisString;
import com.chosen0ne.redisrepl.util.RedisMessageUtils;
import com.chosen0ne.redisrepl.util.RedisUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class RedisBytesDumper {
    public static byte[] TAG_SEPARATOR = {'\r', '\n'};

    // FORMAT: offset \r\n cluster \r\n hostport \r\n operator \r\n key \r\n command
    public static byte[] dump(
            ByteArrayOutputStream os,
            RedisArray cmd,
            String... tags/*offset, cluster, hostport, operator, key*/) throws IOException {
        os.reset();
        for (String t: tags) {
            os.write(t.getBytes());
            os.write(TAG_SEPARATOR);
        }
        os.write(RedisMessageUtils.fetchCmdName(cmd));
        os.write(TAG_SEPARATOR);

        byte[] keyBytes = RedisMessageUtils.fetchKey(cmd);
        if (keyBytes == null) {
            keyBytes = "".getBytes(Constants.UTF8);
        }
        os.write(keyBytes);
        os.write(TAG_SEPARATOR);

        dump(cmd, os);

        return os.toByteArray();
    }

    private static void dump(RedisArray cmd, ByteArrayOutputStream os) throws IOException {
        String bulkHeader = "*" + (cmd.value().size()) + "\r\n";
        os.write(bulkHeader.getBytes());

        RedisUtils.toBulkStr(os, RedisMessageUtils.fetchCmdName(cmd));

        for (int i=1; i<cmd.value().size(); i++) {
            // Just cast the RedisMessage<?> to RedisString, as we will make sure that 'cmd'
            // is a validated format of redis command.
            RedisString element = (RedisString) cmd.value().get(i);
            RedisUtils.toBulkStr(os, element.value());
        }
    }

    public static void main(String[] args) throws IOException {
        RedisMessage<?> cmd = RedisMessageUtils.makeStringArray("ZADD", "zset", "1", "xxx", "2", "f");
        ByteArrayOutputStream os = new ByteArrayOutputStream(1024);
        byte[] buf = dump(os, (RedisArray)cmd, "msg", "127.0.0.1:6379");

        System.out.println(new String(buf));
    }
}