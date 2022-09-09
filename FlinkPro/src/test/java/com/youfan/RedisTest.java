package com.youfan;

import redis.clients.jedis.Jedis;

public class RedisTest {
    public static void main(String[] args) {


        Jedis jedis = new Jedis("121.41.82.106", 6379);

        //密码
        jedis.auth("hPxyR42klsyi");
        //jedis 使用数据库1
//        jedis.select(1);


        //String
        String aa = jedis.set("aa", "11");
        jedis.expire("aa",10*60);
        System.out.println(jedis.get("aa"));
        jedis.expire("a",10);
        jedis.resetState();


//        jedis.expire()  --设置过期时间
//        jedis.ttl()  --查看过期时间
//        set
//        Long bb = jedis.sadd("bb", "22", "33");
//        System.out.println(jedis.smembers("bb"));
//
//        //hash(map) 1
//        Long hset = jedis.hset("cc", "c", "44");
//        System.out.println(jedis.hget("cc", "c"));
//        //hash 2
//        Long hset1 = jedis.hset("cc", "d", "55");
//        System.out.println(jedis.hgetAll("cc"));

        jedis.close();

//        JedisPool jedisPool = new JedisPool("121.41.82.106", 6379);
//        Jedis resource = jedisPool.getResource();
//        String ping = resource.ping();
//        System.out.println("ping = " + ping);
//
//        jedisPool.close();
    }
}
