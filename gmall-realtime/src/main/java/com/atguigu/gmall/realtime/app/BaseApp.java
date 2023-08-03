package com.atguigu.gmall.realtime.app;

import com.atguigu.gmall.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

/**
 * @title: BaseApp
 * @Author joey
 * @Date: 2023/7/30 22:47
 * @Version 1.0
 * @Note:
 */
public abstract class BaseApp {


    public abstract void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream);

    public void  start(int port, int p, String ckAndGroupIdAndJobName, String topic){

        System.setProperty("HADOOP_USER_NAME","atguigu");

        Configuration conf = new Configuration();

        conf.setInteger("rest.port",port);
        conf.setString("pipeline.name", ckAndGroupIdAndJobName);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        env.setParallelism(p);


        //1.设置状态后端、
        env.setStateBackend(new HashMapStateBackend());
        //2.开启checkpoint
        env.enableCheckpointing(3000);
        //3.设置状态的一致性级别
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //4.设置 checkpoint存储的目录
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop162:8020/gmall/ck/"+ckAndGroupIdAndJobName);
        //5.设置 checkpoint 的并发数
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //6.设置两个checkpoint之间的最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        //7.设置checkpoint超时时间
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        //8.当取消job时候,存储checkpoint的数据是否删除
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);
        //9.开启非对齐检查点

        // env.getCheckpointConfig().enableUnalignedCheckpoints();

        // env.getCheckpointConfig().setForceUnalignedCheckpoints(true);

        KafkaSource<String> source = KafkaUtil.getKafkaSource(ckAndGroupIdAndJobName,topic);
        DataStreamSource<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source");



        handle(env,stream);

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }




    }
}
