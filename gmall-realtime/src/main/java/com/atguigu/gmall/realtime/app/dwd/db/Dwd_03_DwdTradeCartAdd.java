package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.app.BaseSQLApp;
import com.atguigu.gmall.realtime.common.GmallConstant;
import com.atguigu.gmall.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @title: Dwd_03_DwdTradeCartAdd
 * @Author joey
 * @Date: 2023/8/5 10:27
 * @Version 1.0
 * @Note:
 */
public class Dwd_03_DwdTradeCartAdd extends BaseSQLApp {
    public static void main(String[] args) {
        new Dwd_03_DwdTradeCartAdd().start(30004, 2, "Dwd_03_DwdTradeCartAdd");
    }

    @Override
    public void handel(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {

        // 1. 读取 ods_db
        readOdsDb(tEnv, "Dwd_03_DwdTradeCartAdd");

        // 2.过滤出加购数据

        Table cartInfo = tEnv.sqlQuery(
                "SELECT " +
                        "    `data`['id']  id, " +
                        "    `data`['user_id']  user_id, " +
                        "    `data`['sku_id']  sku_id, " +
                        "    IF(`type` = 'insert', " +
                        "        `data`['sku_num'], " +
                        "        CAST(CAST(`data`['sku_num'] AS INT) - CAST(`old`['sku_num'] AS INT)AS STRING)) sku_num, " +
                        "    ts " +
                        "FROM ods_db " +
                        "WHERE `database` = 'gmall2023' " +
                        "    and `table` = 'cart_info' " +
                        "    and (`type` = 'insert' " +
                        "        OR (`type` = 'update' " +
                        "            and `old`['sku_num'] IS NOT NULL " +
                        "            and CAST(`data`['sku_num'] AS INT) > CAST(`old`['sku_num'] AS INT) " +
                        "        ) " +
                        ")");


        // 3.写出
        tEnv.executeSql("create table dwd_trade_cart_add (" +
                " id string," +
                " user_id string," +
                " sku_id string," +
                " sku_num string," +
                "ts bigint)" + SQLUtil.getKafkaSinkSQL(GmallConstant.TOPIC_DWD_TRADE_CART_ADD));
        cartInfo.executeInsert("dwd_trade_cart_add");
    }
}


/*
交易域加购事实表

业务过程: 添加购物车
粒度:    一次添加
维度:    用户 时间 商品
度量:    数量

数据源:
    cart_info     insert  update(sku_num 变大)
   商品      数量
 sku_10      1  => 直接写出1  (insert)
 sku_10      4  => 4-1=3  (update)

 */