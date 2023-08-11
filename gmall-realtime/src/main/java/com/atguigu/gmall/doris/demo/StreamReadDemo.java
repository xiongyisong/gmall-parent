package com.atguigu.gmall.doris.demo;

import com.atguigu.gmall.realtime.app.BaseApp;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.deserialization.SimpleListDeserializationSchema;
import org.apache.doris.flink.source.DorisSource;
import org.apache.doris.flink.source.DorisSourceBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

/**
 * @title: StreamReadDemo
 * @Author joey
 * @Date: 2023/8/11 11:35
 * @Version 1.0
 * @Note:
 */
public class StreamReadDemo extends BaseApp {
    public static void main(String[] args) {
        new StreamReadDemo().start(20000,2,"StreamReadDemo","ods_log");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        DorisOptions.Builder builder = DorisOptions.builder()
                .setFenodes("hadoop162:7030")
                .setTableIdentifier("test.table1")
                .setUsername("root")
                .setPassword("aaaaaa");
        DorisSource<List<?>> dorisSource = DorisSourceBuilder.<List<?>>builder()
                .setDorisOptions(builder.build())
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDeserializer(new SimpleListDeserializationSchema())
                .build();

        env.fromSource(dorisSource, WatermarkStrategy.noWatermarks(),"doris source").print();


    }
}
