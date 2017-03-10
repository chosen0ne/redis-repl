package com.chosen0ne.redisrepl.protocol;

import java.util.ArrayList;
import java.util.List;

public class RedisArray extends AbstractRedisMessage<List<RedisMessage<?>>>{
    private List<RedisMessage<?>> array_;

    public RedisArray() {
        array_ = new ArrayList<RedisMessage<?>>();
    }

    @Override
    public String type() {
        return "array";
    }

    @Override
    public List<RedisMessage<?>> value() {
        return array_;
    }

    public void add(RedisMessage<?> element) {
        array_.add(element);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(100);
        sb.append("RedisMessage@").append(type()).append('{');
        boolean isFirst = true;
        for (RedisMessage<?> msg: array_) {
            if (isFirst) {
                isFirst = false;
            } else {
                sb.append(',');
            }
            sb.append(msg.toString());
        }
        sb.append("}@attrs[").append(getAttrs().toString()).append(']');
        return sb.toString();
    }
}