package com.atguigu.gmall.realtime.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.util.HbaseUtil;
import com.atguigu.gmall.realtime.util.RedisUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;
/**
 * @title: DimMapFunction
 * @Author joey
 * @Date: 2023/8/15 23:43
 * @Version 1.0
 * @Note:
 */
public abstract class DimMapFunction <T> extends RichMapFunction<T,T> implements  DimFunction<T>{

    private Connection hbaseConn;
    private Jedis redisClient;
    @Override
    public void open(Configuration parameters) throws Exception {
        // 1. 获取 hbase 连接
        hbaseConn = HbaseUtil.getHbaseConnection();


        redisClient = RedisUtil.getRedisClient();
    }

    @Override
    public void close() throws Exception {
        // 关闭 hbase 连接
        HbaseUtil.closeHbaseConnection(hbaseConn);
         RedisUtil.closeRedisClient(redisClient);

    }

    @Override
    public T map(T bean) throws Exception {
        // 1. 先从缓存读取维度
        JSONObject dim = RedisUtil.getOneRow(redisClient, getTableName(), getId(bean), JSONObject.class);
        if (dim == null) {
            System.out.println(getTableName() + "  " + getId(bean) + " 走的 hbase");
            // 2. 没有读到, 去 hbase 中读取
            dim = HbaseUtil.getOneRow(hbaseConn, "gmall", getTableName(), getId(bean), JSONObject.class);
            // 3. 把读到的维度存入到 redis 中
            RedisUtil.writeOneRow(redisClient, getTableName(), getId(bean), dim);
        }else{
            System.out.println(getTableName() + "  " + getId(bean) + " 走的 redis");
        }

        // 5. 补充维度
        addDim(bean, dim);
        return bean;




    }
}
