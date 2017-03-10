package com.chosen0ne.redisrepl.codec;

import com.chosen0ne.redisrepl.protocol.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.util.List;

public class RedisEncoder extends MessageToMessageEncoder<RedisMessage<?>> {
    private static int TYPE_LEN = 1;
    private static int EOF_LEN = 2;

    @Override
    protected void encode(
            ChannelHandlerContext ctx,
            RedisMessage<?> msg,
            List<Object> out) throws Exception {

        ByteBufAllocator allocator = ctx.alloc();
        if (msg instanceof RedisError) {
            writeErr((RedisError) msg, allocator, out);
        } else if (msg instanceof RedisString) {
            writeSimpleStr((RedisString) msg, allocator, out);
        } else if (msg instanceof RedisArray) {
            writeArray((RedisArray) msg, allocator, out);
        } else if (msg instanceof RedisInteger) {
            writeInteger((RedisInteger) msg, allocator, out);
        }
    }

    private void writeNewline(ByteBufAllocator allocator, List<Object> out) {
        ByteBuf buffer = allocator.buffer(1);
        buffer.writeByte('\n');

        out.add(buffer);
    }

    private void writeNull(ByteBufAllocator allocator, List<Object> out) {
        ByteBuf buffer = allocator.buffer(TYPE_LEN + 2 + EOF_LEN);
        buffer.writeByte(Constants.DOLLAR);
        ByteBufUtil.writeUtf8(buffer, "-1");
        buffer.writeShort(Constants.EOF_SHORT);

        out.add(buffer);
    }

    private void writeGeneral(
            char type,
            byte[] data,
            ByteBufAllocator allocator,
            List<Object> out) {
        if (data.length == 0) {
            writeNull(allocator, out);
            return;
        }

        ByteBuf buffer = allocator.buffer(TYPE_LEN + data.length + EOF_LEN);
        buffer.writeByte(type);
        buffer.writeBytes(data);
        buffer.writeShort(Constants.EOF_SHORT);

        out.add(buffer);
    }

    private void writeErr(RedisError msg, ByteBufAllocator allocator, List<Object> out) {
        writeGeneral(Constants.MINUS, msg.value(), allocator, out);
    }

    private void writeSimpleStr(RedisString msg, ByteBufAllocator allocator, List<Object> out) {
        writeGeneral(Constants.PLUS, msg.value(), allocator, out);
    }

    private void writeInteger(RedisInteger msg, ByteBufAllocator allocator, List<Object> out) {
        writeGeneral(
                Constants.QUOTE,
                String.valueOf(msg.value()).getBytes(Constants.UTF8),
                allocator,
                out);
    }

    private void writeArray(RedisArray msg, ByteBufAllocator allocator, List<Object> out) {
        if (msg.value().isEmpty()) {
            writeNull(allocator, out);
            return;
        }

        // write array header
        ByteBuf buffer = allocator.buffer(TYPE_LEN + Constants.LONG_MAX_LEN + EOF_LEN);
        buffer.writeByte(Constants.ASTERISK);
        List<RedisMessage<?>> msgs = msg.value();
        ByteBufUtil.writeUtf8(buffer, String.valueOf(msgs.size()));
        buffer.writeShort(Constants.EOF_SHORT);

        out.add(buffer);

        // write each element
        for (RedisMessage<?> m: msgs) {
            byte[] bytes = null;
            char typeByte = Constants.DOLLAR;
            if (m instanceof RedisString) {
                bytes = ((RedisString) m).value();
            } else if (m instanceof RedisError) {
                bytes = ((RedisError) m).value();
                typeByte = Constants.MINUS;
            } else if (m instanceof RedisInteger) {
                int i = ((RedisInteger) m).value();
                bytes = String.valueOf(i).getBytes(Constants.UTF8);
                typeByte = Constants.QUOTE;
            }

            ByteBuf buf = allocator.buffer(TYPE_LEN + Constants.LONG_MAX_LEN + bytes.length
                    + 2 * EOF_LEN);
            buf.writeByte(typeByte);
            if (m instanceof RedisInteger ||
                    m instanceof RedisError) {
                buf.writeBytes(bytes);
                buf.writeShort(Constants.EOF_SHORT);
            } else {
                // header
                ByteBufUtil.writeUtf8(buf, String.valueOf(bytes.length));
                buf.writeShort(Constants.EOF_SHORT);

                // content
                buf.writeBytes(bytes);
                buf.writeShort(Constants.EOF_SHORT);
            }
            out.add(buf);
        }
    }
}