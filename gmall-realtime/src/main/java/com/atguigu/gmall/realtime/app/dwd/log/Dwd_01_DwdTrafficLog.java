package com.atguigu.gmall.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseApp;
import com.atguigu.gmall.realtime.common.GmallConstant;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.KafkaUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;

/**
 * @title: Dwd_01_DwdTrafficLog
 * @Author joey
 * @Date: 2023/8/2 13:59
 * @Version 1.0
 * @Note:
 */

@Slf4j
public class Dwd_01_DwdTrafficLog extends BaseApp {

    private final String START = "start";
    private final String ERR = "err";
    private final String DISPLAY = "display";
    private final String PAGE = "page";
    private final String ACTION = "action";


    public static void main(String[] args) {
        new Dwd_01_DwdTrafficLog().start(30001, 2, "Dwd_01_DwdTrafficLog", GmallConstant.TOPIC_ODS_LOG);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        /*
         * 1。对日志数据做清洗
         * 2. 纠正新老客户
         * 3.分流 把一个流 分成 5个流
         * 4.  5个流 分别写入到不同的 topic 中
         */
        // 1。对日志数据做清洗
        SingleOutputStreamOperator<JSONObject> etlStream = etl(stream);
        // 2. 纠正新老客户
        SingleOutputStreamOperator<JSONObject> validatedStream = validateNewOrOld(etlStream);
        //3.分流 把一个流 分成 5个流
        HashMap<String, DataStream<JSONObject>> streams = splitStream(validatedStream);



        //4.  5个流 分别写入到不同的 topic 中
        writeToKafka(streams);

    }

    private void writeToKafka(HashMap<String, DataStream<JSONObject>> streams) {
        streams.get(START).map(JSONAware::toJSONString).sinkTo(KafkaUtil.getKafkaSink(GmallConstant.TOPIC_DWD_TRAFFIC_START));
        streams.get(ERR).map(JSONAware::toJSONString).sinkTo(KafkaUtil.getKafkaSink(GmallConstant.TOPIC_DWD_TRAFFIC_ERR));
        streams.get(PAGE).map(JSONAware::toJSONString).sinkTo(KafkaUtil.getKafkaSink(GmallConstant.TOPIC_DWD_TRAFFIC_PAGE));
        streams.get(DISPLAY).map(JSONAware::toJSONString).sinkTo(KafkaUtil.getKafkaSink(GmallConstant.TOPIC_DWD_TRAFFIC_DISPLAY));
        streams.get(ACTION).map(JSONAware::toJSONString).sinkTo(KafkaUtil.getKafkaSink(GmallConstant.TOPIC_DWD_TRAFFIC_ACTION));
    }

    private HashMap<String, DataStream<JSONObject>> splitStream(SingleOutputStreamOperator<JSONObject> stream) {

        // OutputTag<JSONObject> displayTag = new OutputTag<>("display", TypeInformation.of(JSONObject.class));
        OutputTag<JSONObject> displayTag = new OutputTag<JSONObject>("display") {};

        OutputTag<JSONObject> pageTag = new OutputTag<JSONObject>("page") {};
        OutputTag<JSONObject> errTag = new OutputTag<JSONObject>("err") {};
        OutputTag<JSONObject> actionTag = new OutputTag<JSONObject>("action") {};

        // 启动: 主流
        // 其他: 侧流
        SingleOutputStreamOperator<JSONObject> startStream = stream
                .process(new ProcessFunction<JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject obj,
                                               Context ctx,
                                               Collector<JSONObject> out) throws Exception {
                        JSONObject common = obj.getJSONObject("common");
                        Long ts = obj.getLong("ts");
                        JSONObject start = obj.getJSONObject("start");
                        if (start != null) {
                            out.collect(obj); // 启动日志
                        }

                        // 曝光日志
                        JSONArray displays = obj.getJSONArray("displays");
                        if (displays != null) {
                            for (int i = 0; i < displays.size(); i++) {
                                JSONObject display = displays.getJSONObject(i);
                                display.put("ts", ts);
                                display.putAll(common);
                                ctx.output(displayTag, display);
                            }
                            obj.remove("displays");
                        }

                        // 活动
                        JSONArray actions = obj.getJSONArray("actions");
                        if (actions != null) {
                            for (int i = 0; i < actions.size(); i++) {
                                JSONObject action = actions.getJSONObject(i);
                                action.putAll(common);
                                ctx.output(actionTag, action);
                            }
                            obj.remove("actions");
                        }

                        // 错误
                        JSONObject err = obj.getJSONObject("err");
                        if (err != null) {
                            ctx.output(errTag, obj);
                            obj.remove("err");
                        }


                        // 页面
                        JSONObject page = obj.getJSONObject("page");
                        if (page != null) {
                            ctx.output(pageTag, obj);
                        }


                    }
                });



        HashMap<String, DataStream<JSONObject>> streams = new HashMap<>();
        streams.put(START,startStream);
        streams.put(PAGE,startStream.getSideOutput(pageTag));
        streams.put(DISPLAY,startStream.getSideOutput(displayTag));
        streams.put(ERR,startStream.getSideOutput(errTag));
        streams.put(ACTION,startStream.getSideOutput(actionTag));

        return streams;


    }


    private SingleOutputStreamOperator<JSONObject> validateNewOrOld(SingleOutputStreamOperator<JSONObject> stream) {
        return stream.keyBy(obj -> obj.getJSONObject("common").getString("mid")) // 按照 mid 进行 keyby
                .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    private ValueState<String> firstVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        firstVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("firstVisitDate", String.class));
                    }

                    @Override
                    public void processElement(JSONObject obj, Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject common = obj.getJSONObject("common");
                        String isNew = common.getString("is_new");
                        String firstVisitDate = firstVisitDateState.value();
                        Long ts = obj.getLong("ts");
                        String today = DateFormatUtil.tsToDate(ts);

                        if ("1".equals(isNew)) {
                            if (firstVisitDate == null) {
                                firstVisitDateState.update(today);
                            } else if (!today.equals(firstVisitDate)) {
                                common.put("is_new", "0");
                            }
                        } else if (firstVisitDate == null) {
                            firstVisitDateState.update(DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000));
                        }
                        out.collect(obj);
                    }
                });
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {

                try {
                    JSONObject obj = JSON.parseObject(value);
                    return obj.containsKey("start") || obj.containsKey("actions") || obj.containsKey("displays") || obj.containsKey("err") || obj.containsKey("page");

                } catch (Exception e) {
                    log.warn("数据不是正常的json数据:" + value);
                    return false;
                }
            }
        }).map(s -> JSON.parseObject(s));
    }
}

/**
 * 新老用户标记纠正：
 * 错误：会把老用户识别为新用户（重装应用操作，清楚缓存操作。。。）
 * <p>
 * 当 is_new = 1时候 ，需要判断做纠正
 * <p>
 * is_new =1
 * 状态== null
 * 第一天的第一次访问：不需要纠正
 * 把今天 存入到状态中
 * 状态！=null
 * 今天=状态日期
 * 不需要纠正 无需操作
 * 今天！=状态日期
 * 需要纠正 把 is_new = 0
 */