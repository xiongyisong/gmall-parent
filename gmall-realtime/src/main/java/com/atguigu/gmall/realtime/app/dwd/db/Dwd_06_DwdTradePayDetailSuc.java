package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.app.BaseSQLApp;
import com.atguigu.gmall.realtime.common.GmallConstant;
import com.atguigu.gmall.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 交易域支付成功事务事实表
 * @title: Dwd_06_DwdTradePayDetailSuc
 * @Author joey
 * @Date: 2023/8/7 16:16
 * @Version 1.0
 * @Note:
 */
public class Dwd_06_DwdTradePayDetailSuc extends BaseSQLApp {
    public static void main(String[] args) {
        new Dwd_06_DwdTradePayDetailSuc().start(30006,2,"Dwd_06_DwdTradePayDetailSuc");
    }

    @Override
    public void handel(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {

        // 1. 读取 ods_db
        readOdsDb(tEnv,"Dwd_06_DwdTradePayDetailSuc");

        // 2. 过滤支付成功的数据 payment_info update   payment_status 1601=>1602
        Table paymentInfo = tEnv.sqlQuery("select " +
                "data['user_id'] user_id," +
                "data['order_id'] order_id," +
                "data['payment_type'] payment_type," +
                "data['callback_time'] callback_time," +
                "pt," +
                "et," +
                "ts " +
                "from ods_db " +
                "where `database`='gmall2023' " +
                "and `table`='payment_info' " +
                "and `type`='update' " +
                "and `old`['payment_status']='1601' " +
                "and `data`['payment_status']='1602' ");
        tEnv.createTemporaryView("payment_info", paymentInfo);

        // 3. 读取dwd 下单明细表
        tEnv.executeSql(
                "create table dwd_trade_order_detail( " +
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
                        "ts bigint, " +
                        "et as to_timestamp_ltz(ts, 0)," +
                        "watermark for et as et - interval '3' second " +
                        ")" + SQLUtil.getKafkaSourceSQL("Dwd_05_DwdTradeOrderCancelDetail_1", GmallConstant.TOPIC_DWD_TRADE_ORDER_DETAIL));
        // 4. 读取字典表
        BaseSQLApp.readBaseDic(tEnv);


        // 5. join: interval join    lookup join: 退化支付类型
        Table result = tEnv.sqlQuery(
                "select " +
                        "od.id order_detail_id," +
                        "od.order_id," +
                        "od.user_id," +
                        "od.sku_id," +
                        "od.sku_name," +
                        "od.province_id," +
                        "od.activity_id," +
                        "od.activity_rule_id," +
                        "od.coupon_id," +
                        "pi.payment_type payment_type_code," +
                        "dic.info.dic_name payment_type_name," +
                        "pi.callback_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount split_payment_amount," +
                        "pi.ts " +
                        "from payment_info pi " +
                        "join dwd_trade_order_detail od " +
                        "on pi.order_id=od.order_id " +
                        "   and od.et>=pi.et - interval '30' minute " +
                        "   and od.et<=pi.et + interval '5' second " +
                        "join base_dic for system_time as of pi.pt as dic " +
                        "on pi.payment_type=dic.dic_code ");

        // 6. 写出
        tEnv.executeSql("create table dwd_trade_pay_detail_suc(" +
                "order_detail_id string," +
                "order_id string," +
                "user_id string," +
                "sku_id string," +
                "sku_name string," +
                "province_id string," +
                "activity_id string," +
                "activity_rule_id string," +
                "coupon_id string," +
                "payment_type_code string," +
                "payment_type_name string," +
                "callback_time string," +
                "sku_num string," +
                "split_original_amount string," +
                "split_activity_amount string," +
                "split_coupon_amount string," +
                "split_payment_amount string," +
                "ts bigint " +
                ")" + SQLUtil.getKafkaSinkSQL(GmallConstant.TOPIC_DWD_TRADE_PAY_DETAIL_SUC));

        result.executeInsert("dwd_trade_pay_detail_suc");

    }
}

/*
交易域支付成功事务事实表

select
..
from a
join b on a.id=b.id
where a.et>=b.et - 下界 and a.et<= b.et+上界
------
interval join 目前只支持事件时间
8:00 下单
8:20 支付成功

//order_info   update  order_status: 1001=>1002
payment_info   update   payment_status 1601=>1602
    join
dwd_trade_order_detail
on oi.id=od.order_id
where od.et>=oi.et - 30分钟 and od.et<=oi.et+5s


 */