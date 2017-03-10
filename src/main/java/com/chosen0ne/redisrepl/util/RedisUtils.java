package com.chosen0ne.redisrepl.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class RedisUtils {
    public static void toBulkStr(ByteArrayOutputStream os, byte[] buf) throws IOException {
        String strHeader = "$" + buf.length + "\r\n";
        os.write(strHeader.getBytes());
        os.write(buf);
        os.write("\r\n".getBytes());
    }
}
