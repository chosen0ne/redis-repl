package com.chosen0ne.redisrepl;

import java.util.concurrent.atomic.AtomicLong;

public class ReplCoreData {
    public AtomicLong masterReplOffset_ = new AtomicLong();
    public String masterRunId_;
    public long lastIOtimestamp_;
    public long rdbSaveBeginMs_;
}
