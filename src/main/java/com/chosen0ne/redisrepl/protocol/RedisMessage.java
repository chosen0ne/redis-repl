package com.chosen0ne.redisrepl.protocol;

import java.util.Map;

public interface RedisMessage<E> {
    public String type();
    public E value();
    public void setAttr(String key, Object val);
    public Object getAttr(String key);
    public Map<String, Object> getAttrs();
}