package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.app.BaseSQLApp;
import com.atguigu.gmall.realtime.common.GmallConstant;
import com.atguigu.gmall.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @title: Dwd_05_DwdTradeOrderCancelDetail
 * @Author joey
 * @Date: 2023/8/7 14:47
 * @Version 1.0
 * @Note:
 */
public class Dwd_05_DwdTradeOrderCancelDetail  extends BaseSQLApp {
    public static void main(String[] args) {
        new Dwd_05_DwdTradeOrderCancelDetail().start(30006,2,"Dwd_05_DwdTradeOrderCancelDetail");
    }

    @Override
    public void handel(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(30 * 60 + 5));

        // 1. 读取 ods_db
        readOdsDb(tEnv,"Dwd_05_DwdTradeOrderCancelDetail");
        // 2. 过滤订单取消数据 order_info:   update   order_status 1001=>1003
        Table orderCancel = tEnv.sqlQuery("select " +
                " `data`['id'] id, " +
                " `data`['operate_time'] cancel_time,  " +
                " `ts`  " +
                "from ods_db " +
                "where `database`='gmall2023' " +
                "and `table`='order_info' " +
                "and `type`='update' " +
                "and `old`['order_status']='1001' " +
                "and `data`['order_status']='1003' ");

        tEnv.createTemporaryView("order_cancel",orderCancel);

        // 3. 读取 dwd 层的下单明细表
        tEnv.executeSql("create table dwd_trade_order_detail( " +
                "id string, " +
                "order_id string, " +
                "user_id string, " +
                "sku_id string, " +
                "sku_name string, " +
                "province_id string, " +
                "activity_id string, " +
                "activity_rule_id string, " +
                "coupon_id string, " +
                "date_id string, " +
                "create_time string, " +
                "sku_num string, " +
                "split_original_amount string, " +
                "split_activity_amount string, " +
                "split_coupon_amount string, " +
                "split_total_amount string, " +
                "ts bigint " +
                ")"+SQLUtil.getKafkaSourceSQL("Dwd_05_DwdTradeOrderCancelDetail_1", GmallConstant.TOPIC_DWD_TRADE_ORDER_DETAIL));





        // 4. 取消数据和 dwd 层下单明细数据进行 join

        Table result = tEnv.sqlQuery("select " +
                "od.id," +
                "od.order_id," +
                "od.user_id," +
                "od.sku_id," +
                "od.sku_name," +
                "od.province_id," +
                "od.activity_id," +
                "od.activity_rule_id," +
                "od.coupon_id," +
                "date_format(oc.cancel_time, 'yyyy-MM-dd') operate_date_id," +
                "oc.cancel_time," +
                "od.sku_num," +
                "od.split_original_amount," +
                "od.split_activity_amount," +
                "od.split_coupon_amount," +
                "od.split_total_amount," +
                "oc.ts " +
                "from dwd_trade_order_detail od " +
                "join order_cancel oc " +
                "on od.order_id=oc.id");




        // 5. 写出到 dwd 层(kafka)
        tEnv.executeSql("create table dwd_trade_cancel_detail(" +
                "id string," +
                "order_id string," +
                "user_id string," +
                "sku_id string," +
                "sku_name string," +
                "province_id string," +
                "activity_id string," +
                "activity_rule_id string," +
                "coupon_id string," +
                "date_id string," +
                "cancel_time string," +
                "sku_num string," +
                "split_original_amount string," +
                "split_activity_amount string," +
                "split_coupon_amount string," +
                "split_total_amount string," +
                "ts bigint " +
                ")" + SQLUtil.getKafkaSinkSQL(GmallConstant.TOPIC_DWD_TRADE_CANCEL_DETAIL));
        result.executeInsert("dwd_trade_cancel_detail");


    }
}


/*
订单取消

业务过程: 取消订单 主动 被动
粒度: 商品
维度: 和下单一样
度量: 和下单一样
-----
ttl: 30min+5s
order_info:   update   order_status 1001=>1003
    join
order_detail: insert
    left join
order_detail_activity: insert
    left join
order_detail_coupon: insert

-----------

优化:
   order_info:   update   order_status 1001=>1003
    join
   dwd_下单明细表


*/