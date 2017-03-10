package com.chosen0ne.redisrepl.util;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by louzhenlin on 2017/3/8.
 */
public class Counter {
    private AtomicLong val_ = new AtomicLong();
    private AtomicLong prevVal_ = new AtomicLong();

    public long inc() {
        return val_.incrementAndGet();
    }

    public long getIncrement() {
        return val_.addAndGet(0 - prevVal_.longValue());
    }

    public long incrementAndReset() {
        long increment = getIncrement();
        prevVal_.set(val_.longValue());
        val_.set(0);

        return increment;
    }
}
