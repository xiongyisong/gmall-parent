package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.app.BaseSQLApp;
import com.atguigu.gmall.realtime.common.GmallConstant;
import com.atguigu.gmall.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @title: Dwd_04_DwdTradeOrderDetail
 * @Author joey
 * @Date: 2023/8/7 11:47
 * @Version 1.0
 * @Note:
 */
public class Dwd_04_DwdTradeOrderDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new Dwd_04_DwdTradeOrderDetail().start(30005,2,"Dwd_04_DwdTradeOrderDetail");
    }

    @Override
    public void handel(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        tEnv.getConfig().setIdleStateRetention(Duration.
                ofSeconds(5));
        //        tEnv.getConfig().getConfiguration().setString("table.exec.state.ttl", "5 second");

        // 1. 读取 ods_db
        readOdsDb(tEnv, "Dwd_04_DwdTradeOrderDetail");

        // 2. 过滤订单表数据
        Table orderInfo = tEnv.sqlQuery("select " +
                "data['id'] id," +
                "data['user_id'] user_id," +
                "data['province_id'] province_id " +
                "from ods_db " +
                "where `database`='gmall2023' " +
                "and `table`='order_info' " +
                "and `type`='insert' ");

        tEnv.createTemporaryView("order_info",orderInfo);
        // 3. 过滤 order_detail 数据
        Table orderDetail = tEnv.sqlQuery("select " +
                "data['id'] id," +
                "data['order_id'] order_id," +
                "data['sku_id'] sku_id," +
                "data['sku_name'] sku_name," +
                "data['create_time'] create_time," +
                "data['source_id'] source_id," +
                "data['source_type'] source_type," +
                "data['sku_num'] sku_num," +
                "cast(cast(data['sku_num'] as decimal(16,2)) * " +
                "  cast(data['order_price'] as decimal(16,2)) as String) split_original_amount," +  // 分摊原始总金额
                "data['split_total_amount'] split_total_amount," + // 下单总金额: 去掉减免
                "data['split_activity_amount'] split_activity_amount," +
                "data['split_coupon_amount'] split_coupon_amount," +
                "ts " +
                "from ods_db " +
                "where `database`='gmall2023' " +
                "and `table`='order_detail' " +
                "and `type`='insert' ");
        tEnv.createTemporaryView("order_detail", orderDetail);


        // 4. 过滤详情活动 order_detail_activity
        Table orderDetailActivity = tEnv.sqlQuery( "select " +
                "data['order_detail_id'] order_detail_id," +
                "data['activity_id'] activity_id," +
                "data['activity_rule_id'] activity_rule_id " +
                "from ods_db " +
                "where `database`='gmall2023' " +
                "and `table`='order_detail_activity' " +
                "and `type`='insert' ");
        tEnv.createTemporaryView("order_detail_activity", orderDetailActivity);

        // 5. 过滤详情优惠券 order_detail_coupon
        Table orderDetailCoupon = tEnv.sqlQuery("select " +
                "data['order_detail_id'] order_detail_id, " +
                "data['coupon_id'] coupon_id " +
                "from ods_db " +
                "where `database`='gmall2023' " +
                "and `table`='order_detail_coupon' " +
                "and `type`='insert' ");
        tEnv.createTemporaryView("order_detail_coupon", orderDetailCoupon);

        // 6. join 4 张表
        Table result = tEnv.sqlQuery("select " +
                "od.id, " +
                "od.order_id, " +
                "oi.user_id, " +
                "od.sku_id, " +
                "od.sku_name, " +
                "oi.province_id, " +
                "act.activity_id, " +
                "act.activity_rule_id, " +
                "cou.coupon_id, " +
                "date_format(od.create_time, 'yyyy-MM-dd') date_id, " +
                "od.create_time, " +
                "od.sku_num, " +
                "od.split_original_amount, " +
                "od.split_activity_amount, " +
                "od.split_coupon_amount, " +
                "od.split_total_amount, " +
                "od.ts  " +
                "from order_detail od " +
                "join order_info oi " +
                "on od.order_id=oi.id " +
                "left join order_detail_activity act " +
                "on od.id=act.order_detail_id " +
                "left join order_detail_coupon cou " +
                "on od.id=cou.order_detail_id ");

        // 7. 写出到 dwd 层(upsert kafka)

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
                "ts bigint, " +
                "primary key(id) not enforced " +
                ")"+
                SQLUtil.getUpsertKafkaSQL(GmallConstant.TOPIC_DWD_TRADE_ORDER_DETAIL));

        result.executeInsert("dwd_trade_order_detail");
    }
}


/*
交易域下单事务事实表

业务过程: 下单  产生订单
    mysql:
        order_info   insert          1 条
        order_detail  insert         N 条
        order_detail_activity insert N 条
        order_detail_coupon   insert N 条

粒度: 最小粒度
    商品

维度:
    时间  用户 省份  商品品牌...

度量:
    下单总金额(减免后)  优惠券减免  活动减免  原始总金额(减免前)   商品个数
order_info   insert
    insert     条件: order_id
order_detail  insert
    left join  条件: 详情id
order_detail_activity insert
    left join  条件: 详情id
order_detail_coupon   insert

注意: ttl 的设置
    5s
 */