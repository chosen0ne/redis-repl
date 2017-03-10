package com.chosen0ne.redisrepl.progress;

import java.util.HashMap;
import java.util.Map;

import com.chosen0ne.redisrepl.util.LogUtils;
import org.apache.log4j.Logger;

public class ReplProgress {
    private static Logger logger = Logger.getLogger(ReplProgress.class);

    private static final String RUNID = "runid";
    private static final String OFFSET = "offset";

    private static IProgressStore progressStore_;

    public static Progress getProgress(String hostPort) {
        if (progressStore_ == null) {
            LogUtils.error(logger, "progress store is null");
            return null;
        }

        Map<String, String> progressMap = progressStore_.get(hostPort);
        Progress progress = new Progress(progressMap);
        
        return progress;
    }

    public static void setProgress(String hostPort, long offset) {
        if (progressStore_ == null) {
            LogUtils.error(logger, "progress store is null");
            return;
        }
        Map<String, String> map = new HashMap<String, String>();
        map.put(OFFSET, offset + "");
        progressStore_.set(hostPort, map);
    }

    public static void setProgress(String hostPort, Progress progress) {
        if (progressStore_ == null) {
            LogUtils.error(logger, "progress store is null");
            return;
        }
        progressStore_.set(hostPort, progress.toMap());
    }

    public static void setProgressStore(IProgressStore progressStore) {
        progressStore_ = progressStore;
    }

    public static class Progress {
        public String runid;
        public long offset;

        public Progress(String runid_, long offset_) {
            runid = runid_;
            offset = offset_;
        }

        public Progress(Map<String, String> m) {
            runid = "?";
            offset = -1;
            if (m == null) {
                return;
            }

            if (m.containsKey(RUNID)) {
                runid = m.get(RUNID);
            }
            if (m.containsKey(OFFSET)) {
                try {
                    offset = Long.parseLong(m.get("offset"));
                } catch (Exception e) {
                    offset = -1;
                }
            }
        }

        public Map<String, String> toMap() {
            Map<String, String> m = new HashMap<String, String>();
            m.put(RUNID, runid);
            m.put(OFFSET, offset + "");
            return m;
        }
    }
}
