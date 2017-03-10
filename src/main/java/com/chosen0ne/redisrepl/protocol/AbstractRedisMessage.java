package com.chosen0ne.redisrepl.protocol;

import java.util.HashMap;
import java.util.Map;

public abstract class AbstractRedisMessage<E> implements RedisMessage<E>{
    private Map<String, Object> attr_ = new HashMap<String, Object>();

    @Override
    public void setAttr(String key, Object val) {
        attr_.put(key, val);
    }

    @Override
    public Object getAttr(String key) {
        return attr_.get(key);
    }

    public Map<String, Object> getAttrs() {
        return attr_;
    }
}
