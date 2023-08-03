package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.app.BaseSQLApp;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @title: Dwd_02_DwdInteractionCommentInfo
 * @Author joey
 * @Date: 2023/8/3 10:42
 * @Version 1.0
 * @Note:
 */
public class Dwd_02_DwdInteractionCommentInfo extends BaseSQLApp {

    public static void main(String[] args) {
        new Dwd_02_DwdInteractionCommentInfo().start(30002,2,
                        "Dwd_02_DwdInteractionCommentInfo"
                );
    }


    @Override
    public void handel(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        //1.读取 ods_db
        BaseSQLApp.readOdsDb(tEnv,"Dwd_02_DwdInteractionCommentInfo");
        tEnv.sqlQuery("select * from ods_db").execute().print();


    }


}
