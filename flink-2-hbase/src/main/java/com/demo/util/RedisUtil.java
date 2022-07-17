package com.demo.util;

import org.apache.flink.streaming.connectors.redis.RedisSink;
import redis.clients.jedis.Jedis;

public class RedisUtil {
    private static Jedis jedis;
    public static Jedis connectRedis(String host){
        jedis=new Jedis(host);
        return jedis;
    }

    public static void main(String[] args) {
        Jedis jedis= RedisUtil.connectRedis("172.18.65.186");
        Jedis jedis1=jedis;
        System.out.println(jedis==jedis1);
//        System.out.println(jedis.ping());
//        jedis.sadd("20220502","101");
//        System.out.println(jedis.smembers("20220502"));
    }
}
