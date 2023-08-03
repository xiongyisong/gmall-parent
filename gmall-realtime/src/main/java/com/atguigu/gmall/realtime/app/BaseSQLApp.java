package com.atguigu.gmall.realtime.app;

import com.atguigu.gmall.realtime.common.GmallConstant;
import com.atguigu.gmall.realtime.util.SQLUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

/**
 * @title: BaseSQLApp
 * @Author joey
 * @Date: 2023/8/3 10:31
 * @Version 1.0
 * @Note:
 */
public abstract class BaseSQLApp {
    public static void readOdsDb(StreamTableEnvironment tEnv, String groupId) {
        tEnv.executeSql("create table ods_db (" +
                "`database` string," +
                "`table` string," +
                "`type` string," +
                "`data` map<string, string>," +
                "`old` map<string, string>," +
                "`ts` bigint " +
                ")"+ SQLUtil.getKafkaSourceSQL(groupId, GmallConstant.TOPIC_ODS_DB));
    }

    public void handel(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
    }

    public void start(int port, int p, String ckAndJobName) {
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        // 给job 设置名字
        conf.setString("pipeline.name", ckAndJobName);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        env.setParallelism(p);

        // 1.设置状态后端：1、hashmap(默认) 2.rocksdb
        env.setStateBackend(new HashMapStateBackend());
        // 2.开启 checkpoint
        // 生产环境中 一般是分钟级别
        env.enableCheckpointing(3000);
        // 3.设置状态的一致性级别
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 4.设置就 checkpoint 存储目录
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop162:8020/gmall/ck/" + ckAndJobName);
        // 5. 设置 checkpoint 的并发数
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 6. 设置两个 checkpoint 之间的最小间隔. 如果这设置了, 则可以忽略setMaxConcurrentCheckpoints
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 7. 设置 checkpoint 的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        // 8. 当 job 被取消的时候, 存储从 checkpoint 的数据是否要删除
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);

        // 9. 开启非对齐检查点
        // env.getCheckpointConfig().enableUnalignedCheckpoints();
        // env.getCheckpointConfig().setForceUnalignedCheckpoints(true);

        // 创建表环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        handel(env, tEnv);

    }


}
