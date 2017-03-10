package com.chosen0ne.redisrepl.codec;

import com.chosen0ne.redisrepl.RedisSlave;
import com.chosen0ne.redisrepl.protocol.*;
import com.chosen0ne.redisrepl.util.LogUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import io.netty.util.AttributeKey;
import org.apache.log4j.Logger;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class RedisDecoder extends ReplayingDecoder<DecoderState> {
    private static Logger logger = Logger.getLogger(RedisDecoder.class);
    private static AttributeKey<MessageBytes> MSG_BYTES_ATTR = AttributeKey.valueOf("BYTES");

    // In 'array mode' if arrayParseState_ != null
    private ArrayParseState arrayParseState_;

    // Store the length of the bulk string will be read next step.
    private int bulkStrLen_;
    // Store the RedisMessage has been parsed.
    private Queue<RedisMessage<?>> outMsgs_ = new LinkedList<RedisMessage<?>>();

    private RedisSlave slave_;

    private LineBuf lineBuf_;

    public RedisDecoder(RedisSlave slave) {
        super(DecoderState.DECODE_START);
        slave_ = slave;
    }

    @Override
    protected void decode(
            ChannelHandlerContext ctx,
            ByteBuf in,
            List<Object> out) throws Exception {

        // Update the io timestamp to avoid timeout
        slave_.getCoreData().lastIOtimestamp_ = System.currentTimeMillis();

        Channel channel = ctx.channel();
        int oldBufLen = in.readableBytes();

        switch (state()) {
        case DECODE_START:
            parseMode(in.readByte());
            break;
        case LINE_MODE:
            parseLine(in);
            break;
        case ERR_MODE:
            parseErr(in);
            break;
        case BULK_STR_MODE:
            parseBulkStr(in);
            break;
        case ARRAY_MODE:
            parseArray(in);
            break;
        case READ_BULK_VAL:
            parseBulkVal(in);
            break;
        default:
            LogUtils.info(logger, "meet unexpected state", state());
            break;
        }

        int readBytes = oldBufLen - in.readableBytes();
        MessageBytes bytes = channel.attr(MSG_BYTES_ATTR).get();
        if (bytes == null) {
            bytes = new MessageBytes();
            channel.attr(MSG_BYTES_ATTR).set(bytes);
        }
        bytes.bytes_ += readBytes;

        if (outMsgs_.isEmpty()) {
            LogUtils.debug(logger, "state: {0}, buf: #{1}#, len:{2}, r:{3}, w:{4}",
                    state(),
                    in.getByte(in.readerIndex()),
                    in.readableBytes(),
                    in.readerIndex(),
                    in.writerIndex());
            return;
        }

        lineBuf_ = null;

        // A new message has been parsed
        RedisMessage<?> msg = outMsgs_.poll();
        msg.setAttr(Constants.MSG_BYTES, bytes.bytes_);
        if (arrayParseState_ == null) {
            out.add(msg);
        } else {
            arrayAdd(msg, out);
        }

        channel.attr(MSG_BYTES_ATTR).set(null);
    }

    private void parseMode(byte firstByte) {
        if (firstByte == Constants.ASTERISK) {
            if (arrayParseState_ != null) {
                LogUtils.error(logger, "failed to parse array. Meet a new array when the old " +
                            "one hasn't finished parsing. Drop the old array, old-array-need:{0} " +
                            ", old-array:{1}",
                            arrayParseState_.elementNum_,
                            arrayParseState_.array_);
                arrayParseState_ = null;
            }
            checkpoint(DecoderState.ARRAY_MODE);
        } else if (firstByte == Constants.PLUS) {
            checkpoint(DecoderState.LINE_MODE);
        } else if (firstByte == Constants.MINUS) {
            checkpoint(DecoderState.ERR_MODE);
        } else if (firstByte == Constants.DOLLAR) {
            checkpoint(DecoderState.BULK_STR_MODE);
        } else {
            LogUtils.error(logger, "unexcept type byte, byte:{0}", firstByte);
        }
    }

    private void parseLine(ByteBuf in) {
        lineBuf_ = new LineBuf(1024);
        readLine(in);

        outMsgs_.offer(new RedisString(lineBuf_.lineBytes()));
        checkpoint(DecoderState.DECODE_START);
    }

    private void parseErr(ByteBuf in) {
        lineBuf_ = new LineBuf(1024);
        readLine(in);

        outMsgs_.offer(new RedisError(lineBuf_.lineBytes()));
        checkpoint(DecoderState.DECODE_START);
    }

    private void parseBulkStr(ByteBuf in) {
        bulkStrLen_ = readInt(in);

        checkpoint(DecoderState.READ_BULK_VAL);
    }

    private void parseArray(ByteBuf in) {
        int num = readInt(in);

        arrayParseState_ = new ArrayParseState();
        arrayParseState_.elementNum_ = num;
        checkpoint(DecoderState.DECODE_START);
    }

    private void parseBulkVal(ByteBuf in) {
        lineBuf_ = new LineBuf(bulkStrLen_);
        readLine(in);

        if (lineBuf_.size_ != bulkStrLen_) {
            LogUtils.error(logger, "failed to parse bulk val, length-need:{0}, content:{1}",
                    bulkStrLen_,
                    new String(lineBuf_.buf_, 0, lineBuf_.size_, Constants.UTF8));
            reset();
            return;
        }

        outMsgs_.offer(new RedisString(lineBuf_.buf_));
        checkpoint(DecoderState.DECODE_START);
    }

    private void readLine(ByteBuf in) {
        byte b, c;
        // find '\r\n'
        while (true) {
            if ((b = in.readByte()) == '\r') {
                if ((c = in.readByte()) == '\n') {
                    // A line has read
                    return;
                }

                lineBuf_.add(b);
                lineBuf_.add(c);
            } else {
                lineBuf_.add(b);
            }
        }
    }

    private int readInt(ByteBuf in) {
        int i = 0;
        byte b;
        while ((b = in.readByte()) != '\r') {
            i = i * 10 + (b - '0');
        }

        if ((b = in.readByte()) != '\n') {
            LogUtils.error(logger, "unexpect formate when read integer, need '\n', read:{0}", b);
        }

        return i;
    }

    private void reset() {
        checkpoint(DecoderState.DECODE_START);
    }

    private void arrayAdd(RedisMessage<?> msg, List<Object> out) {
        arrayParseState_.array_.add(msg);
        arrayParseState_.curElementNum_++;
        arrayParseState_.msgBytes_ += (Integer)msg.getAttr(Constants.MSG_BYTES);
        if (arrayParseState_.curElementNum_ == arrayParseState_.elementNum_) {
            arrayParseState_.array_.setAttr(Constants.MSG_BYTES, arrayParseState_.msgBytes_);
            out.add(arrayParseState_.array_);

            // Clear 'array mode'
            arrayParseState_ = null;
        }
    }

    class ArrayParseState {
        int elementNum_;
        int curElementNum_;
        int msgBytes_;
        RedisArray array_ = new RedisArray();
    }

    class MessageBytes {
        int bytes_;
    }

    class LineBuf {
        public byte[] buf_;
        public int size_;
        private int capacity_;

        public LineBuf(int capacity) {
            capacity_ = capacity;
            buf_ = new byte[capacity_];
        }

        private void expand() {
            byte[] newBuf = new byte[capacity_ * 2];
            System.arraycopy(buf_, 0, newBuf, 0, capacity_);
            capacity_ *= 2;
            buf_ = newBuf;
        }

        public void add(byte b) {
            if (size_ >= capacity_) {
                expand();
            }
            buf_[size_++] = b;
        }

        public byte[] lineBytes() {
            byte[] b = new byte[size_];
            System.arraycopy(buf_, 0, b, 0, size_);

            return b;
        }
    }
}