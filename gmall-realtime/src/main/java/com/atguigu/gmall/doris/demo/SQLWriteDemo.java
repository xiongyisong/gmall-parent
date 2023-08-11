package com.atguigu.gmall.doris.demo;

import com.atguigu.gmall.realtime.app.BaseSQLApp;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @title: SQLWriteDemo
 * @Author joey
 * @Date: 2023/8/11 11:30
 * @Version 1.0
 * @Note:
 */
public class SQLWriteDemo extends BaseSQLApp {
    public static void main(String[] args) {
        new SQLWriteDemo().start(20000,2,"StreamReadDemo");
    }


    @Override
    public void handel(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        tEnv.executeSql("CREATE TABLE flink_doris (  " +
                "    siteid INT,  " +
                "    citycode INT,  " +
                "    username STRING,  " +
                "    pv BIGINT  " +
                ")WITH (" +
                "  'connector' = 'doris', " +
                "  'fenodes' = 'hadoop162:7030', " +
                "  'table.identifier' = 'test.table1', " +
                "  'username' = 'root', " +
                "  'password' = 'aaaaaa', " +
                "  'sink.properties.format' = 'json', " +
                "  'sink.buffer-count' = '4', " +
                "  'sink.buffer-size' = '4086'," +
                "  'sink.enable-2pc' = 'false', " + // 测试阶段可以关闭两阶段提交,方便测试
                "  'sink.properties.read_json_by_line' = 'true' " +
                ")  ");
        
        tEnv.executeSql("insert into flink_doris values(600, 1, 'abc', 3)");
    }
}
