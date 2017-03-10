package com.chosen0ne.redisrepl.progress;

import java.util.Map;

public interface IProgressStore {
    void set(String hostport, Map<String, String> progress);
    Map<String, String> get(String hostport);
}
