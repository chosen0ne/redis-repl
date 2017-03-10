package com.chosen0ne.redisrepl.protocol;

public class RedisError extends AbstractRedisMessage<byte[]> {
    private byte[] err_;

    public RedisError(byte[] err) {
        err_ = err;
    }

    @Override
    public String type() {
        return "error";
    }

    @Override
    public byte[] value() {
        return err_;
    }

    @Override
    public String toString() {
        return "RedisMessage@" + type() + "[" + new String(value(), Constants.UTF8)
                + "]@attrs[" + getAttrs() + "]";
    }
}
