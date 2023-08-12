package com.atguigu.gmall.realtime.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;

/**
 * @title: DorisMapFunction
 * @Author joey
 * @Date: 2023/8/11 23:42
 * @Version 1.0
 * @Note:
 */

/**
 * 实现了 Flink 的 MapFunction 接口的自定义函数 DorisMapFunction。
 * 该函数将输入的泛型对象 T 转换为 JSON 字符串
 * @param <T>
 */
public class DorisMapFunction<T> implements MapFunction<T,String> {
    @Override
    public String map(T value) throws Exception {
        SerializeConfig config = new SerializeConfig();
        config.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);
        return JSON.toJSONString(value,config);
    }
}
