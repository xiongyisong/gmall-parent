package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseApp;
import com.atguigu.gmall.realtime.bean.TrafficPageViewBean;
import com.atguigu.gmall.realtime.common.GmallConstant;
import com.atguigu.gmall.realtime.function.DorisMapFunction;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.DorisUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @title: Dws_02_DwsTrafficVcChArIsNewPageViewWindow
 * @Author joey
 * @Date: 2023/8/11 21:06
 * @Version 1.0
 * @Note:
 */
public class Dws_02_DwsTrafficVcChArIsNewPageViewWindow extends BaseApp {
    public static void main(String[] args) {
        new Dws_02_DwsTrafficVcChArIsNewPageViewWindow().start(
                40003,
                2,
                "Dws_02_DwsTrafficVcChArIsNewPageViewWindow",
                GmallConstant.TOPIC_DWD_TRAFFIC_PAGE);
    }


    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        //1.把json字符串解析出来 封装到一个对象中
        SingleOutputStreamOperator<TrafficPageViewBean> beanStream = parseToPojo(stream);

        //2.开窗集合
        SingleOutputStreamOperator<TrafficPageViewBean> resultStream = windowAndAgg(beanStream);
        //3.写到doris
        writeToDoris(resultStream);



    }

    private SingleOutputStreamOperator<TrafficPageViewBean> windowAndAgg(SingleOutputStreamOperator<TrafficPageViewBean> beanStream) {
         /*
        简单
            sum
            max maxBy
            min minBy

        复杂
            reduce
                一般是输入类型和输出类型一致
            aggregate
                 可以不一致
            process
                自己完成
         */

       return beanStream

               //assignTimestampsAndWatermarks(...): 这一步是为输入的数据流分配事件时间戳和水印。
               // 通过使用 WatermarkStrategy，指定了最大乱序程度为 3 秒的水印生成策略，
               // 并通过 withTimestampAssigner 方法指定了事件时间戳的提取方式。
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((bean,ts) -> bean.getTs())
                                .withIdleness(Duration.ofSeconds(60))
                )
               //keyBy(...): 将数据流按照指定的键进行分组，以便在窗口操作中对具有相同键的数据进行处理。
               // 在这里，使用 keyBy 方法将数据流按照 vc、ch、ar 和 isNew 字段组合成的字符串作为键进行分组。
                .keyBy(bean -> bean.getVc() + "_" + bean.getCh() + "_" + bean.getAr() + bean.getIsNew())

               //window(...): 指定窗口操作的类型和大小。在这里，使用 TumblingEventTimeWindows 定义了一个大小为 5 秒的事件时间滚动窗口。
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))

               //在窗口内进行数据的聚合计算。通过 reduce 方法传入一个 ReduceFunction 对象，实现对窗口内数据的聚合逻辑。
               // 在这里，通过将窗口内每个元素的各个字段累加到第一个元素上，实现了对数据的求和操作。
                .reduce(
                        new ReduceFunction<TrafficPageViewBean>() {
                            @Override
                            public TrafficPageViewBean reduce(TrafficPageViewBean value1,
                                                              TrafficPageViewBean value2) throws Exception {
                                value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                                value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                                value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                                value1.setDurSum(value1.getDurSum() + value2.getDurSum());

                                return value1;
                            }
                        },
                        new ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, String, TimeWindow>() {
                            @Override
                            public void process(String key,  // key的值
                                                Context ctx, // 上下文
                                                Iterable<TrafficPageViewBean> elements, // 有且仅有一个值: 前面聚合的最终结果
                                                Collector<TrafficPageViewBean> out) throws Exception {
                                TrafficPageViewBean bean = elements.iterator().next();
                                bean.setStt(DateFormatUtil.tsToDateTime(ctx.window().getStart()));
                                bean.setEdt(DateFormatUtil.tsToDateTime(ctx.window().getEnd()));
                                bean.setCur_date(DateFormatUtil.tsToDate(ctx.window().getEnd()));  // 日期
                                out.collect(bean);

                            }
                        }
                );

    }


    private void writeToDoris(SingleOutputStreamOperator<TrafficPageViewBean> resultStream) {
        resultStream
                .map(new DorisMapFunction<>())
                .sinkTo(DorisUtil.getDorisSink("gmall2023.dws_traffic_vc_ch_ar_is_new_page_view_window"));
    }


    private SingleOutputStreamOperator<TrafficPageViewBean> parseToPojo(DataStreamSource<String> stream) {
        // TODO 4. 转换数据结构 String -> JSONObject
      return  stream
              .map(JSON::parseObject)

                // TODO 5. 按照 mid 分组
                .keyBy(Object->Object.getJSONObject("common").getString("mid"))

                // TODO 6. 统计独立访客数、会话数、页面浏览数、页面访问时长，并封装为实体类
                .map(new RichMapFunction<JSONObject, TrafficPageViewBean>() {

                    private ValueState<String> lastVisitDateState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastVisitDateState = getRuntimeContext()
                                .getState(new ValueStateDescriptor<String>("lastVisitDateState", String.class));
                    }

                    @Override
                    public TrafficPageViewBean map(JSONObject obj) throws Exception {
                        JSONObject common = obj.getJSONObject("common");
                        String vc = common.getString("vc");
                        String ar = common.getString("ar");
                        String ch = common.getString("ch");
                        String isNew = common.getString("is_new");

                        JSONObject page = obj.getJSONObject("page");
                        Long durSum = page.getLong("during_time");

                        Long ts = obj.getLong("ts");
                        String today = DateFormatUtil.tsToDate(ts);

                        //判断是否独立访客
                        long uvCt = 0;
                        // 如果是该设置的一条置为 1  TODO
                        if (!today.equals(lastVisitDateState.value())) {
                            uvCt = 1L;
                            // 更新状态
                            lastVisitDateState.update(today);
                        }
                        // 封装为实体类
                        return new TrafficPageViewBean(
                                "","",vc,ch,ar,isNew,"",uvCt,0L,1L,durSum,ts,common.getString("sid")
                        );
                    }



                })
                //按照 sid 进行分组
                .keyBy(TrafficPageViewBean::getSid)
                .process(new KeyedProcessFunction<String, TrafficPageViewBean, TrafficPageViewBean>() {

                    private ValueState<Boolean> isFirstState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        isFirstState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("isFirst", boolean.class));
                    }



                    //通过判断 isFirstState 是否为 null 来判断是否是第一条数据。如果是第一条数据，将 svCt 设置为 1，并将 isFirstState 更新为 false。
                    @Override
                    public void processElement(TrafficPageViewBean value,
                                               Context ctx,
                                               Collector<TrafficPageViewBean> out) throws Exception {
                        if (isFirstState.value()==null){
                            value.setSvCt(1L);
                            isFirstState.update(false);
                        }
                        out.collect(value);
                    }
                });

    }
}
