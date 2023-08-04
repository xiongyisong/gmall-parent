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

    /**
     *  读取kafka中数据
     *  创建一个临时表ods_db
     * @param tEnv 表环境
     * @param groupId
     */
    public static void readOdsDb(StreamTableEnvironment tEnv, String groupId) {
        tEnv.executeSql("create table ods_db (" +
                "`database` string," +
                "`table` string," +
                "`type` string," +
                "`data` map<string, string>," +
                "`old` map<string, string>," +
                "`ts` bigint," +
                "`pt` as proctime()," +
                "et as to_timestamp_ltz(ts, 0),"+
                "watermark for et as et - interval '3' second" +
                ")"+ SQLUtil.getKafkaSourceSQL(groupId, GmallConstant.TOPIC_ODS_DB));
    }

    /**
     * flink sql中的 hbase sql
     * 根据table对象 创建一个hbase临时表 用于数据存储
     * @param tEnv  表对象
     */
    protected static void readBaseDIC(StreamTableEnvironment tEnv) {
        tEnv.executeSql("create table base_dic (" +
                " dic_code string," +
                " info row<dic_name string> " +
                ") with (" +
                " 'connector' = 'hbase-2.2'," +
                " 'table-name' = 'gmall:dim_base_dic'," +
                " 'lookup.cache' = 'PARTIAL'," +
                " 'lookup.async' = 'true'," +
                " 'lookup.partial-cache.expire-after-write' = '1 day'," +
                // " 'lookup.partial-cache.expire-after-access' = '20 second'," + // ttl更新: 每访问一次这个值, 就跟新一下 ttl
                " 'lookup.partial-cache.max-rows' = '10'," + // 缓存的最大行数
                " 'zookeeper.quorum' = 'hadoop162,hadoop163,hadoop164:2181'" +
                ")");
    }

    public void handel(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
    }




}
