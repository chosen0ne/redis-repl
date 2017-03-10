package com.chosen0ne.redisrepl.util;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.regex.Matcher;

public class LogUtils {

    private static String format(String fmt, Object... params) {
        if (fmt == null) {
            return null;
        }
        if (params == null || params.length == 0) {
            return fmt;
        }

        for (int i=0; i<params.length; i++) {
            String target = "null";
            if (params[i] != null) {
                target = Matcher.quoteReplacement(params[i].toString());
            }
            fmt = fmt.replaceAll("\\{" + i + "\\}", target);
        }

        return fmt;
    }

    private static boolean lvLessThan(Logger logger, Level levelBase) {
        Level level = logger.getLevel();
        if (level == null) {
            return false;
        }
        return level.toInt() < levelBase.toInt();
    }

    public static void debug(Logger logger, String fmt, Object... params) {
        if (lvLessThan(logger, Level.DEBUG)) {
            return;
        }
        logger.debug(format(fmt, params));
    }

    public static void info(Logger logger, String fmt, Object... params) {
        if (lvLessThan(logger, Level.INFO)) {
            return;
        }
        logger.info(format(fmt, params));
    }

    public static void warn(Logger logger, String fmt, Object... params) {
        if (lvLessThan(logger, Level.WARN)) {
            return;
        }
        logger.warn(format(fmt, params));
    }

    public static void error(Logger logger, String fmt, Object... params) {
        if (lvLessThan(logger, Level.ERROR)) {
            return;
        }
        logger.error(format(fmt, params));
    }

    public static void error(Logger logger, Throwable cause, String fmt, Object... params) {
        if (lvLessThan(logger, Level.FATAL)) {
            return;
        }

        String msg = format(fmt, params);
        if (cause == null) {
            logger.error(msg);
        } else {
            logger.error(msg, cause);
        }
    }

    public static void fatal(Logger logger, String fmt, Object... params) {
        if (logger.getLevel().toInt() < Level.FATAL_INT) {
            return;
        }
        logger.fatal(format(fmt, params));
    }
}
