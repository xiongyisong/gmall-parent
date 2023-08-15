package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseApp;
import com.atguigu.gmall.realtime.bean.CartAddUuBean;
import com.atguigu.gmall.realtime.common.GmallConstant;
import com.atguigu.gmall.realtime.function.DorisMapFunction;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.DorisUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @title: Dws_06_DwsTradeCartAddUuWindow
 * @Author joey
 * @Date: 2023/8/15 16:44
 * @Version 1.0
 * @Note:
 */
public class Dws_06_DwsTradeCartAddUuWindow extends BaseApp {
    public static void main(String[] args) {
        new Dws_06_DwsTradeCartAddUuWindow().start(
                40006,
                2,
                "Dws_06_DwsTradeCartAddUuWindow",
                GmallConstant.TOPIC_DWD_TRADE_CART_ADD
        );


    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        stream
                .map(JSON::parseObject)
                .keyBy(obj -> obj.getString("user_id"))
                .process(new KeyedProcessFunction<String, JSONObject, CartAddUuBean>() {
                    public ValueState<String> lastCartAddDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastCartAddDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastCartAddDate", String.class));
                    }

                    @Override
                    public void processElement(JSONObject value,
                                               Context ctx,
                                               Collector<CartAddUuBean> out) throws Exception {
                        String lastCartAddDate = lastCartAddDateState.value();
                        long ts = value.getLong("ts") * 1000;
                        String today = DateFormatUtil.tsToDate(ts);
                        if (!today.equals(lastCartAddDate)) {
                            out.collect(new CartAddUuBean("", "", "", 1L, ts));
                            lastCartAddDateState.update(today);
                        }


                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<CartAddUuBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                                .withIdleness(Duration.ofSeconds(60))
                )
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(
                        new ReduceFunction<CartAddUuBean>() {
                            @Override
                            public CartAddUuBean reduce(CartAddUuBean value1, CartAddUuBean value2) throws Exception {
                                value1.setCartAddUuCt(value1.getCartAddUuCt() + value2.getCartAddUuCt());
                                return value1;
                            }
                        },
                        new ProcessAllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
                            @Override
                            public void process(Context context,
                                                Iterable<CartAddUuBean> elements,
                                                Collector<CartAddUuBean> out) throws Exception {
                                CartAddUuBean bean = elements.iterator().next();
                                bean.setStt(DateFormatUtil.tsToDateTime(context.window().getStart()));
                                bean.setEdt(DateFormatUtil.tsToDateTime(context.window().getEnd()));

                                bean.setCurDate(DateFormatUtil.tsToDate(context.window().getStart()));


                                out.collect(bean);
                            }
                        }
                )
                .map(new DorisMapFunction<>())
                .sinkTo(DorisUtil.getDorisSink("gmall2023.dws_trade_cart_add_uu_window"));


    }


}

/*
从 Kafka 读取用户加购明细数据，统计各窗口加购独立用户数，写入 doris。

数据源:
    dwd 加购明细表


 */
