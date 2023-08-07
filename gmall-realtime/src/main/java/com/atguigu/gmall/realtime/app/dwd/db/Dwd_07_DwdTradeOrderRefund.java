package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.app.BaseSQLApp;
import com.atguigu.gmall.realtime.common.GmallConstant;
import com.atguigu.gmall.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @title: Dwd_07_DwdTradeOrderRefund
 * @Author joey
 * @Date: 2023/8/7 16:45
 * @Version 1.0
 * @Note:
 */
public class Dwd_07_DwdTradeOrderRefund extends BaseSQLApp {
    public static void main(String[] args) {
        new Dwd_07_DwdTradeOrderRefund().start(30007,2,"Dwd_07_DwdTradeOrderRefund");
    }


    @Override
    public void handel(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 1. 读取 ods_db
        readOdsDb(tEnv, "Dwd_07_DwdTradeOrderRefund");
        // 2. 过滤退单表数据

        Table orderRefundInfo = tEnv.sqlQuery(
                "select " +
                        "data['id'] id," +
                        "data['user_id'] user_id," +
                        "data['order_id'] order_id," +
                        "data['sku_id'] sku_id," +
                        "data['refund_type'] refund_type," +
                        "data['refund_num'] refund_num," +
                        "data['refund_amount'] refund_amount," +
                        "data['refund_reason_type'] refund_reason_type," +
                        "data['refund_reason_txt'] refund_reason_txt," +
                        "data['operate_time'] operate_time," +
                        "pt, " +
                        "ts " +
                        "from ods_db " +
                        "where `database`='gmall2023' " +
                        "and `table`='order_refund_info' " +
                        "and `type`='update' " +
                        "and `old`['refund_status'] is not null " +
                        "and `data`['refund_status']='0702' ");
        tEnv.createTemporaryView("order_refund_info", orderRefundInfo);



        // 3. 过滤 订单表中的退单信息
        // Table orderInfo = tEnv.sqlQuery("select" +
        //         "data['id'] id," +
        //         "data['province_id'] province_id, " +
        //         "`old`" +
        //         "from ods_db " +
        //         "where `database`='gmall2023'" +
        //         "and `table`='order_info'" +
        //         "and `type`='update' " +
        //         "and `old`['order_status'] is not null " +
        //         "and `data`['order_status']='1005' ");
        //
        // tEnv.createTemporaryView("order_info", orderInfo);

        Table orderInfo = tEnv.sqlQuery("select " +
                "data['id'] id, " +
                "data['province_id'] province_id, " +
                "`old` " +
                "from ods_db " +
                "where `database`='gmall2023' " +
                "and `table`='order_info' " +
                "and `type`='update' " +
                "and `old`['order_status'] is not null " +
                "and `data`['order_status']='1005' ");

        tEnv.createTemporaryView("order_info", orderInfo);


        // 4. 读取字典表
        readBaseDic(tEnv);
        // 5. 3 张表 join
        Table result = tEnv.sqlQuery(
                "select " +
                        "ri.id," +
                        "ri.user_id," +
                        "ri.order_id," +
                        "ri.sku_id," +
                        "oi.province_id," +
                        "date_format(ri.operate_time,'yyyy-MM-dd') date_id," +
                        "ri.operate_time," +
                        "ri.refund_type," +
                        "dic1.info.dic_name refund_type_name," +
                        "ri.refund_reason_type," +
                        "dic2.info.dic_name refund_reason_type_name," +
                        "ri.refund_reason_txt," +
                        "ri.refund_num," +
                        "ri.refund_amount," +
                        "ri.ts " +
                        "from order_refund_info ri " +
                        "join order_info oi " +
                        "on ri.order_id=oi.id " +
                        "join base_dic for system_time as of ri.pt as dic1 " +
                        "on ri.refund_type=dic1.dic_code " +
                        "join base_dic for system_time as of ri.pt as dic2 " +
                        "on ri.refund_reason_type=dic2.dic_code ");

        // 6.写出到 kafka
        tEnv.executeSql("create table dwd_trade_order_refund(" +
                "id string," +
                "user_id string," +
                "order_id string," +
                "sku_id string," +
                "province_id string," +
                "date_id string," +
                "create_time string," +
                "refund_type_code string," +
                "refund_type_name string," +
                "refund_reason_type_code string," +
                "refund_reason_type_name string," +
                "refund_reason_txt string," +
                "refund_num string," +
                "refund_amount string," +
                "ts bigint)" + SQLUtil.getKafkaSinkSQL(GmallConstant.TOPIC_DWD_TRADE_ORDER_REFUND));
        result.executeInsert("dwd_trade_order_refund");



    }
}

/*
 交易域退单事务事实表

 业务过程: 用户发起退单操作

 粒度: 商品

 维度: 用户 时间 商品..

 度量: 商品数量 退单金额
---------

数据源:
    order_refund_info   update  refund_status =>0702(商家审核通过)

    order_info          update  order_status  =>1005(退款中)

    字典表: 退化refund_reason_type

 */