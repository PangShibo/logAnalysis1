package com.phone.util;

import org.apache.commons.lang.StringUtils;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * @ClassName MemberUtil
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description //TODO $
 **/
public class MemberUtil {
    //先定义缓存 memberId:true
    static Map<String,Boolean> cache = new LinkedHashMap<String,Boolean>(){
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
            return this.size() > 5000;
        }
    };


    /**
     *
     * @param memberId
     * @return  true是新增会员，false为老会员
     *
     * 1、新增的会员或者是从数据库中查询出来的会员存储到缓存中
     * 2、选择mysql或者redis中的一种来做持久化
     *
     */
    public static boolean isNewMember(String memberId){

        return false;
    }

    //3、边写一个方法删除某一天的所有新增会员
    public static void deleteAllData(String day){

    }


    private static Jedis jedis = null;

    static {
        jedis = RedisUtil.getJedis();
    }

    /**
     * 使用redis查询是否为一个新会员
     * @param memberId
     * @param dt  YYYYMMdd
     * @return 返回true则是一个新会员，false是一个老会员
     * 思路：
     * 1、先查询缓存中是否存在，存在直接返回，不存在则查询redis的库
     * 2、redis中存储为字符存储即可，存储格式为日期拼接memberId ：20190211_123567(拼接日期是为咯删除某一天所有数据)
     */
    public static boolean isNewMemberByRedis(String memberId,String dt){
        if(StringUtils.isEmpty(memberId)){
            return false;
        }
        //查看缓存中是否有
        if(cache.containsKey(memberId)){
            return cache.get(memberId);
        }
        //代码走到这儿，代表缓存中没有，则查询redis库
        jedis.select(1);
        Set<String> keys = jedis.keys("*_"+memberId);
        if(keys.size() > 0){
            //有则代表是老会员，返回false
            cache.put(memberId,false);//放到缓存
            return false;
        } else {
            //没有则代表是新增会员，则先添加到redis中去
            jedis.set(dt+"_"+memberId,memberId);
            cache.put(memberId,false);
            return true;
        }
    }

    /**
     * 使用redis删除某一天的数据
     * @param day
     */
    public static void deleteAllDataByRedis(String day){
        if(StringUtils.isEmpty(day)){
            return;
        }
        jedis.select(1);
        //day不为空则删除所有以day开始的数据
        Set<String> keys = jedis.keys(day+"_*");
        for (String key:keys) {
            jedis.del(key);
        }
    }
}