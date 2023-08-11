package com.atguigu.gmall.doris.demo;

import com.atguigu.gmall.realtime.app.BaseSQLApp;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @title: SQLReadDemo
 * @Author joey
 * @Date: 2023/8/11 10:59
 * @Version 1.0
 * @Note:
 */
public class SQLReadDemo extends BaseSQLApp {
    public static void main(String[] args) {
        new SQLReadDemo().start(2000,2,"StreamReadDemo");
    }

    @Override
    public void handel(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        tEnv.executeSql("CREATE TABLE flink_doris (  " +
                "    siteid INT,  " +
                "    citycode SMALLINT,  " +
                "    username STRING,  " +
                "    pv BIGINT  " +
                "    )   " +
                "    WITH (  " +
                "      'connector' = 'doris',  " +
                "      'fenodes' = 'hadoop162:7030',  " +
                "      'table.identifier' = 'test.table1',  " +
                "      'username' = 'root',  " +
                "      'password' = 'aaaaaa'  " +
                ")  ");
        tEnv.sqlQuery("select * from flink_doris").execute().print();
    }
}
