package com.chosen0ne.redisrepl.protocol;

public class RedisInteger extends AbstractRedisMessage<Integer>{
    private int val_;

    public RedisInteger(int val) {
        val_ = val;
    }

    @Override
    public String type() {
        return "integer";
    }

    @Override
    public Integer value() {
        return val_;
    }

    public String toString() {
        return "RedisMessage@" + type() + "[" + val_ + "]@attrs[" + getAttrs() + "]";
    }
}
