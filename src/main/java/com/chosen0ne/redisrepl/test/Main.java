package com.chosen0ne.redisrepl.test;

import com.chosen0ne.redisrepl.RedisSlaveMgr;
import com.chosen0ne.redisrepl.cmdprocessor.PrintProcessor;
import com.chosen0ne.redisrepl.progress.RedisProgressStore;

public class Main {

    public static void main(String[] args) {
        String confPath = "/Users/louzhenlin/IdeaProjects/redis-repl/src/main/resources/config.properties";

        try {
            RedisSlaveMgr.getInstance().conf(confPath)
                .processor(new PrintProcessor())
                .progressStore(new RedisProgressStore("127.0.0.1", 6380))
                .start();
        } catch (Exception e) {
            e.printStackTrace();
        }

        RedisSlaveMgr.getInstance().addMaster("127.0.0.1", 6379, null);
    }
}
