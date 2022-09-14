package com.youfan.flinkApp;

import redis.clients.jedis.Jedis;

public class RedisTest {
    public static void main(String[] args) {
        Jedis jedis = new Jedis("192.168.8.71", 6379);
        String ping = jedis.ping();
        System.out.println(ping);
        jedis.close();
    }
}
