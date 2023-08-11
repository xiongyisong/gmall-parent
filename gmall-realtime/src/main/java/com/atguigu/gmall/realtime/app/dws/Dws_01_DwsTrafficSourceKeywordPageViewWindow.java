package com.atguigu.gmall.realtime.app.dws;

import com.atguigu.gmall.realtime.app.BaseSQLApp;
import com.atguigu.gmall.realtime.common.GmallConstant;
import com.atguigu.gmall.realtime.function.KwSplit;
import com.atguigu.gmall.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @title: Dws_01_DwsTrafficSourceKeywordPageViewWindow
 * @Author joey
 * @Date: 2023/8/11 17:00
 * @Version 1.0
 * @Note:
 */
public class Dws_01_DwsTrafficSourceKeywordPageViewWindow extends BaseSQLApp {
    public static void main(String[] args) {
        new Dws_01_DwsTrafficSourceKeywordPageViewWindow().start(
                40001,2,"Dws_01_DwsTrafficSourceKeywordPageViewWindow"
        );
    }

    @Override
    public void handel(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {

        // 1. 读取页面日志
        // map  row
        tEnv.executeSql(
                "create table page_log(" +
                        "page row<last_page_id string, item_type string, item string>, " +
                        "ts bigint, " +
                        "et as to_timestamp_ltz(ts, 3), " +
                        "watermark for et as et - interval '3' second " +
                        ")"+ SQLUtil.getKafkaSourceSQL("Dws_01_DwsTrafficSourceKeywordPageViewWindow", GmallConstant.TOPIC_DWD_TRAFFIC_PAGE));

        // tEnv.executeSql("select * from page_log").print();

        //2.过程搜索 map：page[‘item’] row:page.item
        Table keywordTable = tEnv.sqlQuery(
                "select " +
                        " page.item keyword, " +
                        " et " +
                        "from  page_log " +
                        "where (page.last_page_id='home' " +
                        "or page.last_page_id='search') " +
                        "and page.item_type='keyword' " +
                        "and page.item is not null" +
                        "");
        tEnv.createTemporaryView("keyword_table",keywordTable);


        //3. 进行分词: 自定义 table 函数和使用 ik 分词器
        // 3.1 注册自定义函数
        tEnv.createTemporaryFunction("kw_split", KwSplit.class);
        // 3.2 在 sql 中使用
        Table kwTable = tEnv.sqlQuery(
                "select " +
                        " kw," +
                        " et " +
                        "from keyword_table " +
                        "join lateral table(kw_split(keyword)) on true");

        tEnv.createTemporaryView("kw_table",kwTable);


        // 3.开窗聚和 tvf 中的滚动窗口
        Table result = tEnv.sqlQuery(
                "select " +
                        "date_format(window_start, 'yyyy-MM-dd HH:mm:ss') stt, " +
                        "date_format(window_end, 'yyyy-MM-dd HH:mm:ss') edt, " +
                        "kw keyword, " +
                        "date_format(window_start, 'yyyyMMdd') cur_date, " +
                        "count(*) keyword_count " +
                        "from table(tumble(table kw_table, descriptor(et), interval '5' second ))" +
                        "group by kw, window_start, window_end" +
                        "");


        //4. 写出到 doris
        tEnv.executeSql(
                "create table dws_traffic_source_keyword_page_view_window(" +
                        " stt string, " +
                        " edt string, " +
                        " keyword string, " +
                        " cur_date string, " +
                        "keyword_count bigint " +
                        ")" +SQLUtil.getDorisSink("gmall2023.dws_traffic_source_keyword_page_view_window"));

        result.executeInsert("dws_traffic_source_keyword_page_view_window");
    }
}



/*
搜索原纪录:
    华为手机
    华为 手机
    苹果手机
    小米手机
---

如何拆开搜索记录:
    制表函数(Table):
        华为
        手机

        华为
        手机

        苹果
        手机
        小米
        手机
    如何拆分中文字符:

        IK 分词器



---
sql 实现
流量域搜索关键词粒度页面浏览各窗口汇总表

`找到搜索关键词,统计每个关键词出现的次数, 最后写入到 doris 中(dws)

1. 数据源: 搜索记录, 过滤出关键词
    dwd_traffic_page

    last_page_id=home and  item_type=keyword and item != null
    ast_page_id=search and  item_type=keyword and item != null
    优化合并:
        (last_page_id=home || ast_page_id=search) and item_type=keyword and item != null

2. 统计每个关键词出现的次数
    开窗聚合
        滚动

        grouped window
            滚动
            滑动
            会话

        tvf
            滚动
                select
                ..
                from table(tumble( t, descriptor(et), interval '5' second))
                group by window_start, window_end, keyword
            滑动

            累积


        over
            topN ...

3. 写出到 doris 中





*/