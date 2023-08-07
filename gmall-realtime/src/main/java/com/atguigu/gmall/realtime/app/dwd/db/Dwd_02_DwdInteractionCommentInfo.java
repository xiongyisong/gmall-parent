package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.app.BaseSQLApp;
import com.atguigu.gmall.realtime.common.GmallConstant;
import com.atguigu.gmall.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
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
        new Dwd_02_DwdInteractionCommentInfo().start(30003,1,
                        "Dwd_02_DwdInteractionCommentInfo"
                );
    }


    @Override
    public void handel(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        //1.读取 ods_db
        readOdsDb(tEnv,"Dwd_02_DwdInteractionCommentInfo");

        // tEnv.sqlQuery("select * from ods_db").execute().print();

        //2.从 ods_db 中过滤出 comment_info 表的数据
        Table commentInfo = tEnv.sqlQuery("" +
                "select " +
                "data['id'] id," +
                "data['user_id'] user_id," +
                "data['sku_id'] sku_id," +
                "data['appraise'] appraise," +
                "data['comment_txt'] comment_txt," +
                "ts," +
                "pt " +
                "from ods_db " +
                "where `database`='gmall2023' " +
                "and `table`='comment_info' " +
                "and `type`='insert'");

        //临时表创建
        tEnv.createTemporaryView("comment_info",commentInfo);

        //tEnv.sqlQuery("select * from ods_db").execute().print();

        //3.读取字典表 （hbase 连接器）
        readBaseDic(tEnv);


        //4.lookup join: 维度退化
        Table result = tEnv.sqlQuery(
                "select " +
                "ci.id," +
                "ci.user_id," +
                "ci.sku_id," +
                "ci.appraise," +
                "dic.info.dic_name appraise_name," +
                "comment_txt," +
                "ts " +
                "from comment_info ci " +
                "join base_dic for system_time as of ci.pt as dic " +
                "on ci.appraise=dic.dic_code ");

        result.execute().print();
        //5.写出到dwd层(kafka中)
        tEnv.executeSql("create table dwd_interaction_comment_info(" +
                "id string," +
                "user_id string," +
                "sku_id string," +
                "appraise string," +
                "appraise_name string," +
                "comment_txt string," +
                "ts bigint " +
                ")"+SQLUtil.getKafkaSinkSQL(GmallConstant.TOPIC_DWD_INTERACTION_COMMENT_INFO));

        // tEnv.sqlQuery("select * from dwd_interaction_comment_info").execute().print();


        result.executeInsert("dwd_interaction_comment_info");



    }


}
