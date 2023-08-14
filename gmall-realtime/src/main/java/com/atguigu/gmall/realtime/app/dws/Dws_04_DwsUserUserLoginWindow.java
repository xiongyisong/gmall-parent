package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseApp;
import com.atguigu.gmall.realtime.bean.UserLoginBean;
import com.atguigu.gmall.realtime.common.GmallConstant;
import com.atguigu.gmall.realtime.function.DorisMapFunction;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.DorisUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @title: Dws_04_DwsUserUserLoginWindow
 * @Author joey
 * @Date: 2023/8/15 0:14
 * @Version 1.0
 * @Note:
 */
public class Dws_04_DwsUserUserLoginWindow extends BaseApp {
    public static void main(String[] args) {
        new Dws_04_DwsUserUserLoginWindow().start(
                40004,
                2,
                "Dws_04_DwsUserUserLoginWindow",
                GmallConstant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. 过滤出登录记录
        SingleOutputStreamOperator<JSONObject> loginLogStream = filterLoginLog(stream);

        // 2. 解析为 pojo
        SingleOutputStreamOperator<UserLoginBean> beanStream = parseToPojo(loginLogStream);

        // 3. 开窗聚合
        SingleOutputStreamOperator<UserLoginBean> resultStream = windowAndAgg(beanStream);

        // 4. 写出到 doris
        writeToDoris(resultStream);




    }

    private void writeToDoris(SingleOutputStreamOperator<UserLoginBean> resultStream) {
        resultStream
                .map(new DorisMapFunction<>())
                .sinkTo(DorisUtil.getDorisSink("gmall2023.dws_user_user_login_window"));
    }

    private SingleOutputStreamOperator<UserLoginBean> windowAndAgg(SingleOutputStreamOperator<UserLoginBean> beanStream) {


        return beanStream
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(
                        new ReduceFunction<UserLoginBean>() {
                            @Override
                            public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) throws Exception {
                                value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                                value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                                return value1;
                            }
                        },
                        new ProcessAllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                            @Override
                            public void process(Context ctx,
                                                Iterable<UserLoginBean> values,
                                                Collector<UserLoginBean> out) throws Exception {
                                UserLoginBean bean = values.iterator().next();

                                bean.setStt(DateFormatUtil.tsToDateTime(ctx.window().getStart()));
                                bean.setEdt(DateFormatUtil.tsToDateTime(ctx.window().getEnd()));
                                bean.setCurDate(DateFormatUtil.tsToDate(ctx.window().getStart()));

                                out.collect(bean);

                            }
                        }
                );

    }

    private SingleOutputStreamOperator<UserLoginBean> parseToPojo(SingleOutputStreamOperator<JSONObject> loginLogStream) {
      return
        loginLogStream
                .keyBy(obj -> obj.getJSONObject("common").getString("uid"))
                .process(new KeyedProcessFunction<String, JSONObject, UserLoginBean>() {


                    private ValueState<String> lastLoginDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastLoginDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastLoginDate", String.class));
                    }

                    @Override
                    public void processElement(JSONObject value,
                                               Context ctx,
                                               Collector<UserLoginBean> out) throws Exception {
                        String lastVisitDate = lastLoginDateState.value();
                        Long ts = value.getLong("ts");
                        String today = DateFormatUtil.tsToDate(ts);


                        long uuCt = 0;
                        long backCt = 0;

                        if (!today.equals(lastVisitDate)) {
                            // 改用户今天第一登录
                            uuCt = 1;
                            lastLoginDateState.update(today);
                            // 状态值不会null, 则证明以前登录过, 判断是否为回流用户
                            if (lastVisitDate != null) {
                                if (ts - DateFormatUtil.dateTimeToTs(lastVisitDate) > GmallConstant.SEVEN_DAY_MS) {
                                    backCt = 1;
                                }
                            }
                        }


                        if (uuCt == 1) {
                            out.collect(new UserLoginBean(
                                    "", "",
                                    "",
                                    backCt, uuCt,
                                    ts
                            ));
                        }

                    }

                });
    }

    private SingleOutputStreamOperator<JSONObject> filterLoginLog(DataStreamSource<String> stream) {


        return stream
                .map(JSON::parseObject)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((obj, ts) -> obj.getLong("ts"))
                                .withIdleness(Duration.ofSeconds(60))
                )
                .filter(new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String uid = value.getJSONObject("common").getString("uid");
                        String lastPageId = value.getJSONObject("page").getString("last_page_id");

                        return ("login".equals(lastPageId) || null == lastPageId) && null != uid;
                    }
                });



    }


}
