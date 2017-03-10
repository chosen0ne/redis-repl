package com.chosen0ne.redisrepl.protocol;

public class RedisString extends AbstractRedisMessage<byte[]> {
    private byte[] string_;

    public RedisString(byte[] string) {
        string_ = string;
    }

    @Override
    public String type() {
        return "string";
    }

    @Override
    public byte[] value() {
        return string_;
    }

    @Override
    public String toString() {
        return "RedisMessage@" + type() + "[" + new String(value(), Constants.UTF8) +
                "]@attrs[" + getAttrs() + "]";
    }
}
