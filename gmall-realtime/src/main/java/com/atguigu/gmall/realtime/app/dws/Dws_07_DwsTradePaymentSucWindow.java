package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseApp;
import com.atguigu.gmall.realtime.bean.TradePaymentBean;
import com.atguigu.gmall.realtime.common.GmallConstant;
import com.atguigu.gmall.realtime.function.DorisMapFunction;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.DorisUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.codehaus.janino.util.Benchmark;

import java.time.Duration;

/**
 * @title: Dws_07_DwsTradePaymentSucWindow
 * @Author joey
 * @Date: 2023/8/15 17:57
 * @Version 1.0
 * @Note:
 */
public class Dws_07_DwsTradePaymentSucWindow extends BaseApp {

    public static void main(String[] args) {
        new Dws_07_DwsTradePaymentSucWindow().start(
                40007,
                2,
                "Dws_07_DwsTradePaymentSucWindow",
                GmallConstant.TOPIC_DWD_TRADE_PAY_DETAIL_SUC
        );
    }


    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        stream
                .map(JSON::parseObject)
                .keyBy(obj -> obj.getString("user_id"))
                .process(new KeyedProcessFunction<String, JSONObject, TradePaymentBean>() {

                    private ValueState<String> lastPaySucDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> desc = new ValueStateDescriptor<>("lastPaySucDate", String.class);
                        /*
                    状态的 ttl:
                        根据业务来定
                     */
                        StateTtlConfig ttlconfig = StateTtlConfig.newBuilder(Time.hours(24))
                                .neverReturnExpired()
                                .updateTtlOnReadAndWrite()
                                .build();
                        desc.enableTimeToLive(ttlconfig);
                        lastPaySucDateState = getRuntimeContext().getState(desc);

                    }

                    @Override
                    public void processElement(JSONObject value,
                                               Context ctx,
                                               Collector<TradePaymentBean> out) throws Exception {
                        String lastPaySucDate = lastPaySucDateState.value();
                        long ts = value.getLong("ts") * 1000;
                        String today = DateFormatUtil.tsToDate(ts);

                        long uuCt = 0;
                        long newUuCt = 0L;
                        if (!today.equals(lastPaySucDate)) {
                            uuCt =1 ;
                            lastPaySucDateState.update(today);

                            if (lastPaySucDate==null) {
                                newUuCt=1;
                            }
                            out.collect(new TradePaymentBean("","","",uuCt,newUuCt,ts));
                        }

                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TradePaymentBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((bean,ts) -> bean.getTs())
                                .withIdleness(Duration.ofSeconds(60))
                )
                .windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(5)))
                .reduce(
                        new ReduceFunction<TradePaymentBean>() {
                            @Override
                            public TradePaymentBean reduce(TradePaymentBean value1, TradePaymentBean value2) throws Exception {
                               value1.setPaymentSucUniqueUserCount(value1.getPaymentSucUniqueUserCount()+value2.getPaymentSucUniqueUserCount());
                                value1.setPaymentSucNewUserCount(value1.getPaymentSucNewUserCount() + value2.getPaymentSucNewUserCount());

                               return value1;
                            }
                        }, new ProcessAllWindowFunction<TradePaymentBean, TradePaymentBean, TimeWindow>() {
                            @Override
                            public void process(Context ctx,
                                                Iterable<TradePaymentBean> elements,
                                                Collector<TradePaymentBean> out) throws Exception {
                                TradePaymentBean bean = elements.iterator().next();
                                bean.setStt(DateFormatUtil.tsToDateTime(ctx.window().getStart()));
                                bean.setEdt(DateFormatUtil.tsToDateTime(ctx.window().getEnd()));
                                bean.setCurDate(DateFormatUtil.tsToDate(ctx.window().getStart()));

                                out.collect(bean);
                            }
                        }
                )
                .map(new DorisMapFunction<>())
                .sinkTo(DorisUtil.getDorisSink("gmall2023.dws_trade_payment_success_uu_window"));


    }
}
