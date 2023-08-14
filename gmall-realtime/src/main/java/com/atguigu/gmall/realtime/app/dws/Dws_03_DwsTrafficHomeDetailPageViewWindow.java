package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseApp;
import com.atguigu.gmall.realtime.bean.TrafficHomeDetailPageViewBean;
import com.atguigu.gmall.realtime.common.GmallConstant;
import com.atguigu.gmall.realtime.function.DorisMapFunction;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.DorisUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @title: Dws_03_DwsTrafficHomeDetailPageViewWindow
 * @Author joey
 * @Date: 2023/8/14 22:44
 * @Version 1.0
 * @Note:
 */
public class Dws_03_DwsTrafficHomeDetailPageViewWindow extends BaseApp {
    public static void main(String[] args) {
        new Dws_03_DwsTrafficHomeDetailPageViewWindow().start(
                40003,
                2,
                "Dws_03_DwsTrafficHomeDetailPageViewWindow",
                GmallConstant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. 先过滤出 详情页访问记录和首页的访问记录
        SingleOutputStreamOperator<JSONObject> homeAndHomeDetailStream = filterHomeAndGoodDetail(stream);

        // 2. 在封装到 pojo 中
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> beanStream = parseToPojo(homeAndHomeDetailStream);

        // 3. 开窗聚和
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> resultStream = windowAndAgg(beanStream);




        // 4. 写出到 doris 中
        writeToDoris(resultStream);


    }

    private void writeToDoris(SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> resultStream) {

        resultStream
                .map(new DorisMapFunction<>())
                .sinkTo(DorisUtil.getDorisSink("gmall2023.dws_traffic_home_detail_page_view_window"));

    }

    private SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> windowAndAgg(SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> beanStream) {
        return
        beanStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TrafficHomeDetailPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((bean,ts) -> bean.getTs())
                                .withIdleness(Duration.ofSeconds(60))
                )
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(
                        new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                            @Override
                            public TrafficHomeDetailPageViewBean reduce(
                                    TrafficHomeDetailPageViewBean value1,
                                    TrafficHomeDetailPageViewBean value2) throws Exception {
                                value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                                value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());

                                return value1;
                            }
                        }, new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow window,
                                              Iterable<TrafficHomeDetailPageViewBean> values,
                                              Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                                TrafficHomeDetailPageViewBean bean = values.iterator().next();

                                bean.setStt(DateFormatUtil.tsToDateTime(window.getStart()));
                                bean.setEdt(DateFormatUtil.tsToDateTime(window.getEnd()));
                                bean.setCurDate(DateFormatUtil.tsToDateTime(window.getStart()));

                                out.collect(bean);
                            }
                        }
                );



    }

    private SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> parseToPojo(SingleOutputStreamOperator<JSONObject> homeAndHomeDetailStream) {
        return
        homeAndHomeDetailStream
                .keyBy(obj -> obj.getJSONObject("common").getString("mid"))
                .flatMap(new RichFlatMapFunction<JSONObject, TrafficHomeDetailPageViewBean>() {

                    private ValueState<String> homeLastVisitDateState;
                    private ValueState<String> goodDetailLastVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        homeLastVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("homeLastVisitDate", String.class));
                        goodDetailLastVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("goodDetailLastVisitDate", String.class));
                    }

                    @Override
                    public void flatMap(JSONObject obj,
                                        Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                        Long ts = obj.getLong("ts");
                        String today = DateFormatUtil.tsToDate(ts);
                        String pageId = obj.getJSONObject("page").getString("page_id");

                        long homeUvCt = 0;
                        long goodDetailUvCt = 0;
                        if ("home".equals(pageId)){
                            String homeLastVisitDate = homeLastVisitDateState.value();
                            if (!today.equals(homeLastVisitDate)) {
                                homeUvCt = 1;
                                homeLastVisitDateState.update(today);
                            }

                        } else {
                            String goodDetailLastVisitDate = goodDetailLastVisitDateState.value();
                            if (!today.equals(goodDetailLastVisitDate)) {
                                goodDetailUvCt =1 ;
                                goodDetailLastVisitDateState.update(today);
                            }
                        }
                        if (homeUvCt + goodDetailUvCt == 1) {
                            out.collect(new TrafficHomeDetailPageViewBean(
                                    "", "",
                                    "",
                                    homeUvCt,
                                    goodDetailUvCt,
                                    ts
                            ));
                        }



                    }
                });

    }

    private SingleOutputStreamOperator<JSONObject> filterHomeAndGoodDetail(DataStreamSource<String> stream) {
        return stream
                .map(JSON::parseObject)
                .filter(new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String pageId = value.getJSONObject("page").getString("page_id");
                        return "home".equals(pageId) || "good_detail".equals(pageId);
                    }
                });


    }
}
