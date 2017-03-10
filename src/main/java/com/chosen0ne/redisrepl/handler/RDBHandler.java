package com.chosen0ne.redisrepl.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ByteProcessor;
import io.netty.util.ReferenceCountUtil;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.List;

import net.whitbeck.rdbparser.DbSelect;
import net.whitbeck.rdbparser.Entry;
import net.whitbeck.rdbparser.EntryType;
import net.whitbeck.rdbparser.KeyValuePair;
import net.whitbeck.rdbparser.RdbParser;
import net.whitbeck.rdbparser.ValueType;

import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.chosen0ne.redisrepl.util.LogUtils;
import com.chosen0ne.redisrepl.RedisSlave;
import com.chosen0ne.redisrepl.codec.RedisBytesDumper;
import com.chosen0ne.redisrepl.progress.ReplProgress;
import com.chosen0ne.redisrepl.progress.ReplProgress.Progress;
import com.chosen0ne.redisrepl.protocol.Constants;
import com.chosen0ne.redisrepl.protocol.RedisArray;
import com.chosen0ne.redisrepl.util.Conf;
import com.chosen0ne.redisrepl.util.RedisMessageUtils;
import com.chosen0ne.redisrepl.util.RedisUtils;

public class RDBHandler extends ChannelInboundHandlerAdapter {
    private static Logger logger = Logger.getLogger(RDBHandler.class);
    private static int WRITE_BLOCK_BYTES = 16 * 1024;

    private RedisSlave slave_;
    private boolean hasReadRDBSize_;
    private long rdbSize_;
    private long transferedBytes_;
    private long recvRdbBegionMs_;

    private String rdbFileName_;
    private FileChannel fileChannel_;

    private int valDumpIdx;
    private ByteArrayOutputStream valOs = new ByteArrayOutputStream(Conf.getMsgMaxBytes());

    public RDBHandler(RedisSlave slave) {
        slave_ = slave;
    }

    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf in = (ByteBuf) msg;
        slave_.getCoreData().lastIOtimestamp_ = System.currentTimeMillis();

        try {
            // Read RDB size. Master send '$1024\r\n'
            if (!hasReadRDBSize_) {
                rdbSize_ = readRDBSize(in);
                if (rdbSize_ > 0) {
                    hasReadRDBSize_ = true;
                    if (!createRDBFile()) {
                        replAbort();
                        return;
                    }

                    recvRdbBegionMs_ = System.currentTimeMillis();
                    LogUtils.info(logger, "MASTER <-> SLAVE sync: Need to read {0} bytes of RDB,"
                            + " fname:{1}, rdb dump by master elapsed:{2}ms, master:{3}",
                            rdbSize_,
                            rdbFileName_,
                            recvRdbBegionMs_ - slave_.getCoreData().rdbSaveBeginMs_,
                            slave_.masterInfo());
                } else if (rdbSize_ == -1) {
                    // fatal error
                    replAbort();
                    return;
                }
            }

            // Read RDB data and copy to file
            while (in.isReadable()) {
                // Set the block size to write to file.
                long left = rdbSize_ - transferedBytes_;
                long blkBytes = in.readableBytes() > WRITE_BLOCK_BYTES ? WRITE_BLOCK_BYTES
                        : in.readableBytes();
                blkBytes = blkBytes > left ? left : blkBytes;

                int wBytes = 0;
                try {
                    wBytes = in.readBytes(fileChannel_, transferedBytes_, (int)blkBytes);
                } catch (IOException e) {
                    LogUtils.error(logger, e, "failed to write data to file, fname:{0}, master:{1}",
                            rdbFileName_,
                            slave_.masterInfo());

                    // TODO: Truncate the file to already written bytes
                }
                transferedBytes_ += wBytes;

                if (transferedBytes_ == rdbSize_) {
                    // We already read the whole RDB file.
                    long rdbElapsedMs = System.currentTimeMillis() - recvRdbBegionMs_;
                    LogUtils.info(logger, "MASTER <-> SLAVE sync: Finish reading RDB {0}bytes in {1}ms, "
                            + "start to load RDB. master:{2}",
                            rdbSize_,
                            rdbElapsedMs,
                            slave_.masterInfo());

                    // To make the replication as fast as possible, release the repl-lock
                    // after RDB file transfered.
                    slave_.markReleaseMutex();

                    // Disable 'read event' of netty, or the incremental data during RDB
                    // loading will be lost. We need to enabel 'read event' after RDB loaded.
                    slave_.disableReadEvent();

                    // The packet consists of the rdb's last part and online data's start part.
                    // The rest of the buffer need to be store for processing later.
                    if (in.isReadable()) {
                        LogUtils.info(logger, "the packet consists of RDB and online data. online: {0}bytes",
                                in.readableBytes());
                        slave_.setOnlieBuf(in.duplicate().retain());
                    }

                    rdbLoad(ctx);

                    return;
                }
            }
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }

    private void rdbLoad(final ChannelHandlerContext ctx) {
        slave_.setReplStatus(RedisSlave.ReplStatus.REPL_LOADING);

        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                Thread.currentThread().setName("rdb-loader");

                long closeElapsed = 0;
                try {
                    long s = System.currentTimeMillis();
                    fileChannel_.close();
                    closeElapsed = System.currentTimeMillis() - s;
                } catch (IOException e) {
                    LogUtils.error(logger, e, "failed to close rdb, stop replication. fname:{0}" +
                            ", master:{1}",
                            rdbFileName_,
                            slave_.masterInfo());
                    replAbort();
                    return;
                }

                long s = System.currentTimeMillis();
                try {
                    parseRDB();
                } catch (Exception e) {
                    LogUtils.error(logger, e, "failed to parse rdb, stop repl. master:{0}",
                            slave_.masterInfo());
                    slave_.stop();
                    return;
                }
                long e = System.currentTimeMillis() - s;

                // Finish RDB parsing, start online replication
                LogUtils.info(logger, "finish parsing RDB, start online replication." +
                        " RDB size:{0}bytes, close:{1}ms, parse:{2}s. master:{3}",
                        rdbSize_,
                        closeElapsed,
                        e/1000,
                        slave_.masterInfo());

                removeFile(rdbFileName_);

                // Update repl progress after load rdb successfully
                Progress progress = new Progress(
                        slave_.getCoreData().masterRunId_,
                        slave_.getCoreData().masterReplOffset_.get());
                ReplProgress.setProgress(slave_.getHostport(), progress);

                slave_.setReplStatus(RedisSlave.ReplStatus.REPL_ONLINE);
                slave_.registerOnlineHandler();
                slave_.enableReadEvent();
                // Process the left buffer in the last RDB packet
                if (slave_.getOnlineBuf() != null && slave_.getOnlineBuf().isReadable()) {
                    LogUtils.info(logger, "process online buffer in last RDB packet, len:{0}",
                            slave_.getOnlineBuf().readableBytes());
                    ctx.fireChannelRead(slave_.getOnlineBuf());
                }

                slave_.removeRDBHandler();

                // NOTICE: To avoid timeout by slave after a long time for loading rdb.
                slave_.getCoreData().lastIOtimestamp_ = System.currentTimeMillis();
            }
        });

        t.start();
    }

    /*
     * return value:
     *      -1: fatal error which needs to stop replication
     *      -2: not fatal
     */
    private long readRDBSize(ByteBuf buf) {
        String line = readLine(buf);
        if (line == null) {
            return -2;
        }

        if (line.charAt(0) == Constants.MINUS) {
            // Master response an error
            LogUtils.error(logger, "Master aborted replication with an error: {0}, master:{1}",
                    line,
                    slave_.masterInfo());
            return -1;
        } else if (line.length() == 0) {
            // Just a newline works as a PING
            return -2;
        } else if (line.charAt(0) != Constants.DOLLAR) {
            LogUtils.error(logger, "Bad protocol from MASTER, the first byte is not '$'" +
                    " (we received '{0}'), are you sure the host and port are right? master:{1}",
                    line,
                    slave_.masterInfo());
            return -1;
        }

        long rdbSize = -1;
        try {
            rdbSize = Long.parseLong(line.substring(1));
        } catch (Exception e) {
            LogUtils.error(logger, e, "failed to parse int when read RDB len, line:{0}, master:{1}",
                    line,
                    slave_.masterInfo());
        }

        return rdbSize;
    }

    private int findEndOfLine(ByteBuf buffer) {
        int i = buffer.forEachByte(ByteProcessor.FIND_LF);
        if (i > 0 && buffer.getByte(i - 1) == '\r') {
            i--;
        }
        return i;
    }

    private boolean createRDBFile() {
        rdbFileName_ = String.format("temp-%s-%d.rdb",
                slave_.getHostport(),
                System.currentTimeMillis() % 1000000);

        for (int i = 0; i < 3; i++) {
            try {
                RandomAccessFile f = new RandomAccessFile(rdbFileName_, "rw");
                fileChannel_ = f.getChannel();
                return true;
            } catch (FileNotFoundException e) {
                LogUtils.error(logger, e, "failed to create file, fname:{0}, master:{1}",
                        rdbFileName_,
                        slave_.masterInfo());
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // Ignore!
            }
        }

        LogUtils.error(logger, "failed to create rdb file, fname:{0}, master:{1}",
                rdbFileName_,
                slave_.masterInfo());
        return false;
    }

    private void parseRDB() {
        try {
            // Flush redis
            RedisArray flushCmd = (RedisArray) RedisMessageUtils.makeStringArray("FLUSHDB");
            slave_.processCommand(flushCmd);

            long s = System.currentTimeMillis();
            RdbParser parser = new RdbParser(rdbFileName_);
            long openMs = System.currentTimeMillis() - s;

            Entry e;
            long parseMs = 0;
            long processMs = 0;
            int cmdCount = 0;
            while (true) {
                s = System.currentTimeMillis();
                e = parser.readNext();
                parseMs += System.currentTimeMillis() - s;
                if (e == null) {
                    break;
                }

                EntryType type = e.getType();

                if (type == EntryType.DB_SELECT) {
                    LogUtils.info(logger, "processing DB: {0}", ((DbSelect)e).getId());
                } else if (type == EntryType.EOF) {
                    LogUtils.info(logger, "finish parsing RDB, file:{0}, open:{1}ms, " +
                            "parse:{2}ms, process:{3}ms, cmd:{4}, master:{5}",
                            rdbFileName_,
                            openMs,
                            parseMs,
                            processMs,
                            cmdCount,
                            slave_.masterInfo());
                } else if (type == EntryType.KEY_VALUE_PAIR) {
                    KeyValuePair kvp = (KeyValuePair) e;

                    String cmdName = fetchCommandName(kvp.getValueType());
                    s = System.currentTimeMillis();
                    if (!slave_.processCommand(
                            cmdName.getBytes(Constants.UTF8),
                            kvp.getKey(),
                            kvp.getValues())) {
                        LogUtils.error(logger, "faield to process command in RDB, cmd:{0}, key:{1}, " +
                                "master:{2}",
                                cmdName,
                                new String(kvp.getKey(), Constants.UTF8),
                                slave_.masterInfo());
                    }

                    processMs += System.currentTimeMillis() - s;
                    cmdCount++;
                    slave_.getCmdCounter().inc();
                }
            }
        } catch (Exception e) {
            LogUtils.error(logger, e, "failed to parse rdb, file:{0}, master:{1}",
                    rdbFileName_,
                    slave_.masterInfo());
        }
    }

    // We can convert the command parsed from RDB to 'Command' Object and call
    // 'RedisBytesDumper.dump()' to avoid the repetition of command dumpping.
    // However, parsed command is in byte[] format. If we does as above, the
    // overhead of byte[] => String and String => byte[] will be included.
    // So, we just dump the command here to improve performation.
    private byte[] kvToCommandBytes(KeyValuePair kvp) throws IOException {
        List<byte[]> values = kvp.getValues();
        if (valDumpIdx >= values.size()) {
            return null;
        }

        // TODO: use ByteBuffer to avoid allocation and deallocation for each command
        ByteArrayOutputStream dumpOs = slave_.cmdDumpOs();
        dumpOs.reset();
        valOs.reset();

        // Offset \r\n Cluster \r\n Hostport \r\n
        String tag = Joiner.on("\r\n").join("-1", slave_.getHostport());
        dumpOs.write(tag.getBytes());
        // add 'Command Name\r\n'
        ValueType valueType = kvp.getValueType();
        String cmdName = fetchCommandName(valueType);
        dumpOs.write(RedisBytesDumper.TAG_SEPARATOR);
        dumpOs.write(cmdName.getBytes());
        dumpOs.write(RedisBytesDumper.TAG_SEPARATOR);
        // add 'Key\r\n'
        dumpOs.write(kvp.getKey());
        dumpOs.write(RedisBytesDumper.TAG_SEPARATOR);

        // Dump values.
        int numVals = 0;
        int valBytes = 0;
        // tag.length + command length + key length + element length + key length
        int maxBytesExceptVals = tag.length() + 100 + kvp.getKey().length + 13
                + (kvp.getKey().length + 3);
        int maxBytesVals = Conf.getMsgMaxBytes() - (int)(maxBytesExceptVals * 1.1);

        // handle values
        if (valueType == ValueType.SORTED_SET ||
                valueType == ValueType.SORTED_SET_AS_ZIPLIST) {
            // Consistency in program is pretty important.
            // The order of ZSET in redis command and RDB is different,
            // we need to process it specially.
            //      redis command: ZADD KEY score1 sub-key1 score2 sub-key2
            //      in rdb       : KEY sub-key1 score1 sub-key2 score2
            while (valDumpIdx < values.size()) {
                byte[] subKey = values.get(valDumpIdx);
                byte[] score = values.get(valDumpIdx + 1);
                int bytes = subKey.length + 13 + score.length + 13;
                if (valBytes + bytes > maxBytesVals) {
                    break;
                }
                RedisUtils.toBulkStr(valOs, score);
                RedisUtils.toBulkStr(valOs, subKey);
                numVals += 2;
                valDumpIdx += 2;
                valBytes += bytes;
            }
        } else {
            while (valDumpIdx < values.size()) {
                byte[] val = values.get(valDumpIdx);
                if (valBytes + val.length + 13 > maxBytesVals) {
                    break;
                }
                RedisUtils.toBulkStr(valOs, val);
                valDumpIdx++;
                numVals++;
                valBytes += val.length + 13;
            }
        }

        // Value is too large to be put into a message. You need to increase the config of
        // 'message.max.bytes'
        if (numVals == 0 && valDumpIdx < values.size()) {
            LogUtils.error(logger, "Value is too large, need to increase 'message.max.bytes'. " +
                    "master:{0}, key:{1}, val-len:{2}",
                    slave_.masterInfo(),
                    new String(kvp.getKey()),
                    values.get(valDumpIdx).length);
            return null;
        }

        // 2 => Command + Key
        String bulkHeader = "*" + (numVals + 2) + "\r\n";
        dumpOs.write(bulkHeader.getBytes());

        RedisUtils.toBulkStr(dumpOs, cmdName.getBytes());
        RedisUtils.toBulkStr(dumpOs, kvp.getKey());
        valOs.writeTo(dumpOs);

        // Output the expire command after the last sub-command of the Key-Value pair.
        if (valDumpIdx == values.size() && kvp.hasExpiry()) {
            long expire = kvp.getExpiryMillis();
            RedisArray expireCmd = (RedisArray) RedisMessageUtils.makeStringArray(
                    "PEXPIRE",
                    new String(kvp.getKey(), Constants.UTF8),
                    String.valueOf(expire));
            byte[] buf = RedisBytesDumper.dump(
                    slave_.cmdDumpOs(),
                    expireCmd,
                    "-1",
                    slave_.getHostport());
            dumpOs.write(buf);
        }

        return dumpOs.toByteArray();
    }

    private String fetchCommandName(ValueType valueType) {
        String cmd = null;
        if (valueType == ValueType.HASH ||
                valueType == ValueType.HASHMAP_AS_ZIPLIST ||
                valueType == ValueType.ZIPMAP) {
            cmd = "HMSET";
        } else if (valueType == ValueType.LIST ||
                valueType == ValueType.QUICKLIST ||
                valueType == ValueType.ZIPLIST) {
            cmd = "RPUSH";
        } else if (valueType == ValueType.SET ||
                valueType == ValueType.INTSET) {
            cmd = "SADD";
        } else if (valueType == ValueType.SORTED_SET ||
                valueType == ValueType.SORTED_SET_AS_ZIPLIST) {
            cmd = "ZADD";
        } else if (valueType == ValueType.VALUE) {
            cmd = "SET";
        }

        return cmd;
    }

    private String readLine(ByteBuf in) {
        int eol = findEndOfLine(in);
        if (eol < 0) {
            return null;
        }

        int len = eol - in.readerIndex();
        if (len == 0) {
            // only a '\n' or '\r\n' which is just a PING
            int skipBytes = in.getByte(eol + 1) == '\r' ? 2 : 1;
            in.skipBytes(skipBytes);
            return null;
        }

        ByteBuf lineBuf = in.readSlice(len);
        // skip '\r', '\n'
        in.skipBytes(2);

        String line = lineBuf.toString(Charset.defaultCharset());

        return line;
    }

    private void replAbort() {
        slave_.stop();
        removeFile(rdbFileName_);
    }

    private void removeFile(String fname) {
        if (fname == null) {
            return;
        }

        File file = new File(fname);
        if (file.isFile() && file.exists()) {
            file.delete();
        }
    }

    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LogUtils.error(logger, cause, "an exception is caught. restart slave, master:{0}",
                slave_.masterInfo());
        slave_.stop();
    }
}