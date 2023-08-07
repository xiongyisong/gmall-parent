package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.app.BaseSQLApp;
import com.atguigu.gmall.realtime.common.GmallConstant;
import com.atguigu.gmall.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @title: Dwd_08_DwdTradeRefundPaySuc
 * @Author joey
 * @Date: 2023/8/7 18:35
 * @Version 1.0
 * @Note:
 */
public class Dwd_08_DwdTradeRefundPaySuc extends BaseSQLApp {
    public static void main(String[] args) {
        new Dwd_08_DwdTradeRefundPaySuc().start(30008,2,"Dwd_08_DwdTradeRefundPaySuc");
    }

    @Override
    public void handel(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {

        // 1. 读取 ods_db
        readOdsDb(tEnv,"Dwd_08_DwdTradeRefundPaySuc");

        // 2. 过滤退款成功表
        Table refundPayment = tEnv.sqlQuery(
                "select " +
                        "data['id'] id, " +
                        "data['order_id'] order_id ," +
                        "data['sku_id'] sku_id ," +
                        "data['payment_type'] payment_type ," +
                        "data['callback_time'] callback_time ," +
                        "data['total_amount'] total_amount ," +
                        "pt," +
                        "ts " +
                        "from ods_db " +
                        "where `database`='gmall2023' " +
                        "and `table`='refund_payment' " +
                        "and `type`='update' " +
                        "and `old`['refund_status'] is not null " +
                        "and `data`['refund_status'] ='1602' "
        );

        tEnv.createTemporaryView("refund_payment",refundPayment);

        // 3. 过滤退单表中退款成功相关的退单信息
        Table orderRefundInfo = tEnv.sqlQuery(
                "select " +
                        "data['order_id'] order_id," +
                        "data['sku_id'] sku_id," +
                        "data['refund_num'] refund_num," +
                        "`old` " +
                        "from ods_db " +
                        "where `database`='gmall2023' " +
                        "and `table`='order_refund_info' " +
                        "and `type`='update' " +
                        "and `old`['refund_status'] is not null " +
                        "and `data`['refund_status'] = '0705' ");

        tEnv.createTemporaryView("order_refund_info", orderRefundInfo);
        // 4. 过滤订单表中给退款成功的订单信息
        Table orderInfo = tEnv.sqlQuery(
                "select " +
                        "data['id'] id," +
                        "data['user_id'] user_id," +
                        "data['province_id'] province_id," +
                        "`old` " +
                        "from ods_db " +
                        "where `database`='gmall2023' " +
                        "and `table`='order_info' " +
                        "and `type`='update' " +
                        "and `old`['order_status'] is not null " +
                        "and `data`['order_status'] = '1006' ");
        tEnv.createTemporaryView("order_info", orderInfo);
        // 5. join:  退化 支付类型
        readBaseDic(tEnv);

        Table result = tEnv.sqlQuery("select " +
                "rp.id," +
                "oi.user_id," +
                "rp.order_id," +
                "rp.sku_id," +
                "oi.province_id," +
                "rp.payment_type," +
                "dic.info.dic_name payment_type_name," +
                "date_format(rp.callback_time,'yyyy-MM-dd') date_id," +
                "rp.callback_time," +
                "ri.refund_num," +
                "rp.total_amount," +
                "rp.ts " +
                "from refund_payment rp " +
                "join order_refund_info ri " +
                "on rp.order_id=ri.order_id and rp.sku_id=ri.sku_id " +
                "join order_info oi " +
                "on rp.order_id=oi.id " +
                "join base_dic for system_time as of rp.pt as dic " +
                "on rp.payment_type=dic.dic_code ");

        // 6. 写出到 Kafka 中
        tEnv.executeSql("create table dwd_trade_refund_pay_suc(" +
                "id string," +
                "user_id string," +
                "order_id string," +
                "sku_id string," +
                "province_id string," +
                "payment_type_code string," +
                "payment_type_name string," +
                "date_id string," +
                "callback_time string," +
                "refund_num string," +
                "refund_amount string," +
                "ts bigint " +
                ")" + SQLUtil.getKafkaSinkSQL(GmallConstant.TOPIC_DWD_TRADE_REFUND_PAY_SUC));
        result.executeInsert("dwd_trade_refund_pay_suc");



    }
}

/*
交易域退款成功事务事实表
数据源:
    refund_payment      update refund_status => 1602
    order_refund_info   update refund_status => 0705
    order_info          update  order_status => 1006

 */