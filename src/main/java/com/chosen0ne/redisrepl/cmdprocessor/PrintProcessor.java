package com.chosen0ne.redisrepl.cmdprocessor;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.chosen0ne.redisrepl.util.LogUtils;

public class PrintProcessor extends CommandProcessor {
    private static Logger logger = Logger.getLogger(PrintProcessor.class);

    @Override
    public boolean process(
            byte[] cmdBytes,
            byte[] keyBytes,
            List<byte[]> vals,
            Map<String, Object> attrs) {

        StringBuilder sb = new StringBuilder();
        sb.append(new String(cmdBytes));
        if (keyBytes != null) {
            sb.append(' ').append(new String(keyBytes));
        }
        for (byte[] v: vals) {
            sb.append(' ').append(new String(v));
        }
        LogUtils.info(logger, "receive command: {0}", sb.toString());
        return true;
    }

}
