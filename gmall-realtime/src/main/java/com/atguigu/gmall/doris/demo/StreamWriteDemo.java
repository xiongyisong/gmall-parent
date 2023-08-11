package com.atguigu.gmall.doris.demo;

import com.atguigu.gmall.realtime.app.BaseApp;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @title: StreamWriteDemo
 * @Author joey
 * @Date: 2023/8/11 11:47
 * @Version 1.0
 * @Note:
 */
public class StreamWriteDemo extends BaseApp {

    public static void main(String[] args) {
        new StreamWriteDemo().start(20000,2,"StreamReadDemo","ods_log");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 文本: json csv  orc parquet
        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true"); // 每行一条 json 数据

        DorisSink<String> dorisSink = DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisOptions(DorisOptions.builder()
                        .setFenodes("hadoop162:7030")
                        .setTableIdentifier("test.table1")
                        .setUsername("root")
                        .setPassword("aaaaaa")
                        .build())
                .setDorisExecutionOptions(DorisExecutionOptions.builder()
                        .setBufferSize(100)
                        .setCheckInterval(1000)
                        .setBufferCount(10)
                        .setMaxRetries(3)
                        .setStreamLoadProp(props)
                        // .setLabelPrefix("doris") // 要求全局唯一
                        .disable2PC() // 禁止两阶段提交
                        .build())
                .setSerializer(new SimpleStringSerializer())
                .build();

        stream.sinkTo(dorisSink);
    }
}
