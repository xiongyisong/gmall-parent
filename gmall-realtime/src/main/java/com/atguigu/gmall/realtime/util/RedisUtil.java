package com.atguigu.gmall.realtime.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConstant;
import com.atguigu.gmall.realtime.function.DimMapFunction;
import io.debezium.engine.format.Json;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @title: RedisUtil
 * @Author joey
 * @Date: 2023/8/16 20:45
 * @Version 1.0
 * @Note:
 */
public class RedisUtil {

    private static final JedisPool pool;

    static {

        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(300);  // 最多提供 300 个客户端
        config.setMaxIdle(20);  // 最大空闲
        config.setMinIdle(10); // 最小空闲
        config.setMaxWaitMillis(10000); //当连接池没有空闲连接的时候, 最大等待时间
        config.setTestOnCreate(true);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);

        pool = new JedisPool(config, "hadoop162", 6379);
    }

    public static Jedis getRedisClient() {
        Jedis jedis = pool.getResource();
        jedis.select(4);

        return jedis;
    }

    public static void closeRedisClient(Jedis redisClient) {
        if (redisClient != null) {
            // 1. 如果客户端是通过 new jedis 出来的,则是关闭
            // 2. 如果客户端是通过连接池获取的,则是归还
            redisClient.close();
        }
    }


    /**
     * 根据传入的条件从 redis 读取维度
     *
     * @param redisClient redis 的客户端
     * @param tableName   表名
     * @param id          id 的值
     * @param tClass  泛型
     * @return 把读到维度值封装到 T 类型的对象中. 如果返回值是 null,表示没有查到对应的维度信息
     */
    public static  <T> T  getOneRow(Jedis redisClient,
                                    String tableName,
                                    String id,
                                    Class<T> tClass) {
        String key = getKey(tableName, id);
        String jsonStr = redisClient.get(key);
        if (jsonStr !=null) {
            return JSON.parseObject(jsonStr,tClass);
        }

        return null;
    }

    private static String getKey(String tableName, String id) {
        return tableName + ":" + id;
    }



    /**
     * 把指定的维度数据写入到 redis 中
     *
     * @param redisClient
     * @param tableName
     * @param id
     * @param dim
     * @param <T>
     */
    public static <T> void writeOneRow(Jedis redisClient,
                                   String tableName,
                                   String id,
                                   T dim) {
        String key = getKey(tableName, id);

        redisClient.setex(key, GmallConstant.TWO_DAY_SECONDS,JSON.toJSONString(dim));

    }
}
