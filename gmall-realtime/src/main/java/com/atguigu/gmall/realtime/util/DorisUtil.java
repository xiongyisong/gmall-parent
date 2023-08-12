package com.atguigu.gmall.realtime.util;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;

import java.util.Properties;

/**
 * @title: DorisUtil
 * @Author joey
 * @Date: 2023/8/11 23:45
 * @Version 1.0
 * @Note:
 */
public class DorisUtil {

    public static DorisSink<String> getDorisSink(String table) {
        // 文本: json csv  orc parquet
        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true"); // 每行一条 json 数据

        return DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisOptions(DorisOptions.builder()
                        .setFenodes("hadoop162:7030")
                        .setTableIdentifier(table)
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
    }

}
