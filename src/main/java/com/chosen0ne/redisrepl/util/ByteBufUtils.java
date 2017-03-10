package com.chosen0ne.redisrepl.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class ByteBufUtils {

    // return:
    //      -1 => char sequence is not found
    //         => the first index of the char sequence founded 
    public static int searchBytes(ByteBuf buf, byte... bytes) {
        if (bytes.length == 0 || buf.readableBytes() < bytes.length) {
            return -1;
        }

        for (int i=buf.readerIndex(); i<=buf.writerIndex() - bytes.length; i++) {
            int j = 0;
            for (; j<bytes.length; j++) {
                if (buf.getByte(j + i) != bytes[j]) {
                    break;
                }
            }
            
            if (j == bytes.length) {
                return i;
            }
        }

        return -1;
    }

    public static void main(String[] args) {
        ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
        ByteBuf buf = allocator.buffer(100);
        buf.writeBytes(new byte[] {'1', '2', '3', '4', '5', '6'});
        System.out.println("" + searchBytes(buf, new byte[] {'3', '4'}));
        System.out.println("" + searchBytes(buf, new byte[] {'1', '2'}));
        System.out.println("" + searchBytes(buf, new byte[] {'5', '6'}));
        System.out.println("" + searchBytes(buf, new byte[] {'6', '7'}));
    }
}
