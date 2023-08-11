package com.atguigu.gmall.doris.demo;

import com.atguigu.gmall.realtime.app.BaseApp;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.RowDataSerializer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import java.util.Properties;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;


/**
 * @title: StreamWriteDemo_2
 * @Author joey
 * @Date: 2023/8/11 11:54
 * @Version 1.0
 * @Note:
 */
public class StreamWriteDemo_2 extends BaseApp {

    public static void main(String[] args) {
        new StreamWriteDemo_2().start(20000, 2, "StreamReadDemo", "ods_log");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
// 文本: json csv  orc parquet
        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true"); // 每行一条 json 数据


        String[] fields = {"siteid", "citycode", "username", "pv"};

        DataType[] fieldTypes = {
                DataTypes.INT(),
                DataTypes.SMALLINT(),
                DataTypes.STRING(),
                DataTypes.BIGINT()
        };
        DorisSink<RowData> dorisSink = DorisSink.<RowData>builder()
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
                .setSerializer(RowDataSerializer.builder()
                        .setFieldNames(fields)
                        .setFieldType(fieldTypes)
                        .setType("json")
                        .build())
                .build();

        stream
                .map(new MapFunction<String, RowData>() {
                    @Override
                    public RowData map(String value) throws Exception {
                        JSONObject obj = JSON.parseObject(value);

                        GenericRowData rd = new GenericRowData(4);
                        rd.setField(0, obj.getIntValue("siteid"));
                        rd.setField(1, obj.getShortValue("citycode"));
                        rd.setField(2, StringData.fromString(obj.getString("username")));
                        rd.setField(3, obj.getLongValue("pv"));
                        return rd;
                    }
                })
                .sinkTo(dorisSink);
    }
}
