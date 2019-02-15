package com.phone.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @ClassName RedisUtil
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description redis的工具类
 **/
public class RedisUtil {

    private static JedisPool jedisPool = null;
    //初始化连接
    static {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(100);
        jedisPoolConfig.setMaxIdle(5);
        jedisPoolConfig.setMaxWaitMillis(100*1000);
        jedisPoolConfig.setTestOnReturn(true);
        jedisPool = new JedisPool(jedisPoolConfig,"192.168.216.6",6379,5000);
    }

    /**
     * 获取jedis的连接池
     * @return
     */
    public static JedisPool getJedisPool(){
        return jedisPool;
    }

    /**
     * 获取jedis对象
     * @return
     */
    public static Jedis getJedis(){
        return jedisPool.getResource();
    }

    /**
     * 关闭jedis
     * @param jedis
     */
    public static void closeJedis(Jedis jedis){
        if(jedis != null){
            jedis.close();
        }
    }

    public static void main(String[] args) {
       /* Jedis jedis = RedisUtil.getJedis();
        jedis.select(1);
        jedis.set("20190211_123","123");
        jedis.set("20190211_345","345");
        jedis.set("20190211_567","567");
        jedis.set("20190212_567","789");
        System.out.println(jedis.exists("20190211_123"));
        Set<String> keys = jedis.keys("20190211_*");
        for (String key:keys) {
//            System.out.println(key);
            jedis.del(key);
        }*/

        System.out.println(MemberUtil.isNewMemberByRedis("567","20190212"));
        System.out.println(MemberUtil.isNewMemberByRedis("568","20190212"));
        System.out.println(MemberUtil.isNewMemberByRedis("569","20190212"));
        System.out.println(MemberUtil.isNewMemberByRedis("566","20190213"));


        MemberUtil.deleteAllDataByRedis("20190212");
    }
}