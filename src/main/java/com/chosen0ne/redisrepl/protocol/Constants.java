package com.chosen0ne.redisrepl.protocol;

import io.netty.util.internal.PlatformDependent;

import java.nio.charset.Charset;

public class Constants {
    public static char ASTERISK = '*';
    public static char DOLLAR = '$';
    public static char PLUS = '+';
    public static char MINUS = '-';
    public static char QUOTE = ':';

    public static String EOF = "\r\n";
    public static String MSG_BYTES = "MSG_BYTES";
    public static String FULL_RESYNC = "FULLRESYNC";
    public static String CONTINUE = "CONTINUE";

    public static short EOF_SHORT = makeShort('\r', '\n');
    public static int LONG_MAX_LEN = 20;

    public static Charset UTF8 = Charset.forName("UTF-8");

    static short makeShort(char first, char second) {
        return PlatformDependent.BIG_ENDIAN_NATIVE_ORDER ?
                (short) ((second << 8) | first) : (short) ((first << 8) | second);
    }
}
