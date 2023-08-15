package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseApp;
import com.atguigu.gmall.realtime.bean.UserRegisterBean;
import com.atguigu.gmall.realtime.common.GmallConstant;
import com.atguigu.gmall.realtime.function.DorisMapFunction;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.DorisUtil;
import javafx.scene.input.DataFormat;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @title: Dws_05_DwsUserUserRegisterWindow
 * @Author joey
 * @Date: 2023/8/15 16:11
 * @Version 1.0
 * @Note:
 */
public class Dws_05_DwsUserUserRegisterWindow extends BaseApp {

    public static void main(String[] args) {
        new Dws_05_DwsUserUserRegisterWindow().start(
                40005,
                2,
                "Dws_05_DwsUserUserRegisterWindow",
                GmallConstant.TOPIC_DWD_USER_REGISTER
        );
    }
    @Override
    public void handle(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {
        stream
                .map(JSON::parseObject)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((obj,ts) -> obj.getLong("create_time"))
                                .withIdleness(Duration.ofSeconds(60))
                )
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .aggregate(
                        new AggregateFunction<JSONObject, Long, Long>() {
                            @Override
                            public Long createAccumulator() {
                                // 初始化累加器: 一个窗口执行一次
                                return 0L;
                            }

                            @Override
                            public Long add(JSONObject value, Long acc) {
                                // 真正的累加: 每来一条数据执行一次
                                return acc + 1;
                            }

                            @Override
                            public Long getResult(Long acc) {
                                // 创建计算出来最终的结果的时候返回
                                return acc;
                            }

                            @Override
                            public Long merge(Long a, Long b) {
                                // 创建数据的合并. 只有当 window 是会话窗口的时候才会触发
                                return a + b;
                            }
                        }
                        ,new ProcessAllWindowFunction<Long, UserRegisterBean, TimeWindow>( ){
                            @Override
                            public void process(Context ctx,
                                                Iterable<Long> elements,
                                                Collector<UserRegisterBean> out) throws Exception {
                                Long count = elements.iterator().next();
                                UserRegisterBean bean = new UserRegisterBean(
                                        DateFormatUtil.tsToDateTime(ctx.window().getStart()),
                                        DateFormatUtil.tsToDateTime(ctx.window().getEnd()),
                                        DateFormatUtil.tsToDate(ctx.window().getStart()),
                                        count,
                                        System.currentTimeMillis()
                                );
                                out.collect(bean);

                            }
                        }
                )
                .map(new DorisMapFunction<>())
                .sinkTo(DorisUtil.getDorisSink("gmall2023.dws_user_user_register_window"));
    }
}
/*
从 DWD层用户注册表中读取数据，统计各窗口注册用户数，写入 doris

数据源:
    dwd  用户注册明细表
 */