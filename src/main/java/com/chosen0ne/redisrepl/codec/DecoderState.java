package com.chosen0ne.redisrepl.codec;

public enum DecoderState {
    DECODE_START, LINE_MODE, ERR_MODE, BULK_STR_MODE,
    ARRAY_MODE, READ_BULK_VAL
}
