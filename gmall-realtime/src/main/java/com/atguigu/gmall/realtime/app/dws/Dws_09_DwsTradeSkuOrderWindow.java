package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseApp;
import com.atguigu.gmall.realtime.bean.TradeSkuOrderBean;
import com.atguigu.gmall.realtime.common.GmallConstant;
import com.atguigu.gmall.realtime.function.DimMapFunction;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @title: Dws_09_DwsTradeSkuOrderWindow
 * @Author joey
 * @Date: 2023/8/15 21:59
 * @Version 1.0
 * @Note:
 */
public class Dws_09_DwsTradeSkuOrderWindow extends BaseApp {


    public static void main(String[] args) {
        new Dws_09_DwsTradeSkuOrderWindow().start(
                40009,
                2,
                "Dws_09_DwsTradeSkuOrderWindow",
                GmallConstant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. 封装数据到 pojo 中
        SingleOutputStreamOperator<TradeSkuOrderBean> beanStream = parseToPojo(stream);

        // 2. 按照详情 id 实现去重
        beanStream = distinctByOrderDetailId(beanStream);


        // 3. 开窗聚合
        SingleOutputStreamOperator<TradeSkuOrderBean> beanStreamWithoutDims = windowAndAgg(beanStream);


        //暂时需要补充
        // 4. 补充维度信息
        SingleOutputStreamOperator<TradeSkuOrderBean> beanStreamWithDims = addDims(beanStreamWithoutDims);
        beanStreamWithDims.print();
    
    
    }
    
    private SingleOutputStreamOperator<TradeSkuOrderBean> addDims(SingleOutputStreamOperator<TradeSkuOrderBean> stream) {
        /*
        每来一条数据, 就是 hbase 中查找对应的维度信息: 根据 rowKey, 维度表的都是用的 id


        dim_sku_info
        dim_spu_info
        dim_base_trademark

        dim_base_category3
        dim_base_category2
        dim_base_category1
         */
        SingleOutputStreamOperator<TradeSkuOrderBean> skuStream = stream
                .map(new DimMapFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getTableName() {
                        return "dim_sku_info";
                    }

                    @Override
                    public String getId(TradeSkuOrderBean bean) {
                        return bean.getSkuId();
                    }

                    @Override
                    public void addDim(TradeSkuOrderBean bean,
                                       JSONObject dim) {

                        bean.setSkuName(dim.getString("sku_name"));
                        bean.setSpuId(dim.getString("spu_id"));
                        bean.setTrademarkId(dim.getString("tm_id"));
                        bean.setCategory3Id(dim.getString("category3_id"));
                    }
                });

        SingleOutputStreamOperator<TradeSkuOrderBean> spuStream = skuStream
            .map(new DimMapFunction<TradeSkuOrderBean>() {
                @Override
                public String getTableName() {
                    return "dim_spu_info";
                }
            
                @Override
                public String getId(TradeSkuOrderBean bean) {
                    return bean.getSpuId();
                }
            
                @Override
                public void addDim(TradeSkuOrderBean bean,
                                   JSONObject dim) {
                    bean.setSpuName(dim.getString("spu_name"));
                }
            });
    
        SingleOutputStreamOperator<TradeSkuOrderBean> tmStream = spuStream
            .map(new DimMapFunction<TradeSkuOrderBean>() {
                @Override
                public String getTableName() {
                    return "dim_base_trademark";
                }
            
                @Override
                public String getId(TradeSkuOrderBean bean) {
                    return bean.getTrademarkId();
                }
            
                @Override
                public void addDim(TradeSkuOrderBean bean,
                                   JSONObject dim) {
                    bean.setTrademarkName(dim.getString("tm_name"));
                }
            });
    
        SingleOutputStreamOperator<TradeSkuOrderBean> c3Stream = tmStream
            .map(new DimMapFunction<TradeSkuOrderBean>() {
                @Override
                public String getTableName() {
                    return "dim_base_category3";
                }
            
                @Override
                public String getId(TradeSkuOrderBean bean) {
                    return bean.getCategory3Id();
                }
            
                @Override
                public void addDim(TradeSkuOrderBean bean,
                                   JSONObject dim) {
                    bean.setCategory3Name(dim.getString("name"));
                    bean.setCategory2Id(dim.getString("category2_id"));
                }
            });
    
        SingleOutputStreamOperator<TradeSkuOrderBean> c2Stream = c3Stream
            .map(new DimMapFunction<TradeSkuOrderBean>() {
                @Override
                public String getTableName() {
                    return "dim_base_category2";
                }
            
                @Override
                public String getId(TradeSkuOrderBean bean) {
                    return bean.getCategory2Id();
                }
            
                @Override
                public void addDim(TradeSkuOrderBean bean,
                                   JSONObject dim) {
                    bean.setCategory2Name(dim.getString("name"));
                    bean.setCategory1Id(dim.getString("category1_id"));
                }
            });
        
        return c2Stream
            .map(new DimMapFunction<TradeSkuOrderBean>() {
                @Override
                public String getTableName() {
                    return "dim_base_category1";
                }
        
                @Override
                public String getId(TradeSkuOrderBean bean) {
                    return bean.getCategory1Id();
                }
        
                @Override
                public void addDim(TradeSkuOrderBean bean,
                                   JSONObject dim) {
                    bean.setCategory1Name(dim.getString("name"));
                }
            });
    }

    private SingleOutputStreamOperator<TradeSkuOrderBean> windowAndAgg(SingleOutputStreamOperator<TradeSkuOrderBean> beanStream) {
    return
        beanStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TradeSkuOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((bean,ts) ->bean.getTs())
                                .withIdleness(Duration.ofSeconds(60))
                )
                .keyBy(TradeSkuOrderBean::getSkuId)
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(5)))
                .reduce(
                        new ReduceFunction<TradeSkuOrderBean>() {

                            @Override
                            public TradeSkuOrderBean reduce(TradeSkuOrderBean value1,
                                                            TradeSkuOrderBean value2) throws Exception {
                                value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                                value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                                value1.setActivityAmount(value1.getActivityAmount().add(value2.getActivityAmount()));
                                value1.setCouponAmount(value1.getCouponAmount().add(value2.getCouponAmount()));

                                return value1;

                            }
                        },
                        new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                            @Override
                            public void process(String skuId,
                                                Context ctx,
                                                Iterable<TradeSkuOrderBean> elements,
                                                Collector<TradeSkuOrderBean> out) throws Exception {
                                TradeSkuOrderBean bean = elements.iterator().next();
                                bean.setStt(DateFormatUtil.tsToDateTime(ctx.window().getStart()));
                                bean.setEdt(DateFormatUtil.tsToDateTime(ctx.window().getEnd()));
                                bean.setCurDate(DateFormatUtil.tsToDate(ctx.window().getStart()));

                                out.collect(bean);
                            }
                        }
                );



    }


    /*
   为什么会有重复数据? 数据是由于 left join 写入的结果
       详情id    金额      活动表相关的金额     优惠券相关的金额          生成的时间
        1       100        null                null                    1
        null(删除)
        1       100        200                 null                    2
        null(删除)
        1       100        200                 300                     3

     用流消费:
       详情id    金额      活动表相关的金额     优惠券相关的金额
        1       100        null                null
        1       100        200                 null
        1       100        200                 300
       导致同一个详情出现多次, 后面进行聚和运算的时候会出现重复计算, 所以需要去重


       同一个详情,只保留最后一个最完整的数据

       思路1: 会话窗口
           假设 dwd 的数据添加一个数据的生成时间,则时间最大的那个一定是数据最全,我们想要的.
              详情id    金额      活动表相关的金额     优惠券相关的金额   生成的时间
                1       100        null                null             1
                1       100        200                 null             2
                1       100        200                 300              3

            按照 sku 分区,设置会话窗口 窗口 gap 5s
               当同一个详情的最后一条数据到了之后, 等待 5s 后, 窗口关闭,按照时间进行排序,取出时间最大的就是数据最完整

               优点:
                   简单,实现方便
               缺点:
                   时效性低: 最后一条数据到,5s之后才会出结果

        思路 2: 定时器
           当第一条数据来了之后,注册一个 5s 后触发定时器, 当定时器触发的时候,则数据一定全部来齐了
               每来一条就比较一下时间大小,把时间大的存入到状态中,当定时器触发的时候,转态中存储就一定是时间最大的那个

            优点:
                简单,实现方便
            缺点: 时效比较会话窗口高,但是还是有实效性问题: 第一条到了之后等到 5s 出结果

         思路3: 抵消法

                        详情id    金额      活动表相关的金额     优惠券相关的金额
              第一条      1       100        0                 0             直接输出,  存如到状态

                         1       -100       0                 0              给第一条取反输出
              第二条      1       100        200                0             直接输出

                         1       -100       -200                0           给第二条取反输出
              第三条      1       100        200                 300          直接输出

               优点: 实效性最高
               缺点: 写放大效应

            优化:
                       详情id    金额         活动表相关的金额        优惠券相关的金额
              第一条      1       100           0                           0             直接输出,  存如到状态

              第二条      1       100-100        200-0                     0-0            直接输出  存到状态(第二条原始数据)

              第三条      1       100-100         200-200                 300-0            直接输出  存到状态(第三条原始数据)

              优点: 实效性最高, 没有写放大
            思路4: 特殊情况
                  当如果需要的数据都在左表,则第一条数据就有了所有的数据了.直接输出即可.

                   找到第一条: 状态为 null 的时候

    */
    private SingleOutputStreamOperator<TradeSkuOrderBean> distinctByOrderDetailId(SingleOutputStreamOperator<TradeSkuOrderBean> beanStream) {


        return
        beanStream
                // 使用订单详情ID进行分区，相同订单详情ID的数据会被发送到同一个分区进行处理
                .keyBy(TradeSkuOrderBean::getOrderDetailId)
                .process(new KeyedProcessFunction<String, TradeSkuOrderBean, TradeSkuOrderBean>() {
                    // 定义状态变量
                    private ValueState<TradeSkuOrderBean> lastBeanState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 创建ValueStateDescriptor，用于描述ValueState的属性
                        ValueStateDescriptor<TradeSkuOrderBean> desc = new ValueStateDescriptor<>("lastBean", TradeSkuOrderBean.class);
                        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(50000))  // 创建StateTtlConfig，配置状态的存活时间
                                .updateTtlOnCreateAndWrite()  // 在读写状态时更新存活时间
                                .neverReturnExpired() // 状态永不过期
                                .build();
                        desc.enableTimeToLive(ttlConfig);   // 将ttlConfig应用到ValueStateDescriptor中，为状态设置存活时间配置
                        lastBeanState = getRuntimeContext().getState(desc);   // 获取ValueState实例，并赋值给lastBeanState变量
                    }

                    @Override
                    public void processElement(TradeSkuOrderBean bean,
					Context ctx, 
					Collector<TradeSkuOrderBean> out) throws Exception {
                        // 获取上一个订单详情ID对应的数据对象
                        TradeSkuOrderBean lastBean = lastBeanState.value();
                        if (lastBean==null) {
                            // 如果上一个数据对象为空，说明当前数据是第一条数据，直接输出到结果流
                            out.collect(bean);
                        } else {
                            // 如果上一个数据对象不为空，进行去重操作
                            // 将上一个数据对象的各个金额字段与当前数据对象的对应字段相减
                           lastBean.setOrderAmount(bean.getOrderAmount().subtract(lastBean.getOrderAmount()));
                            lastBean.setOriginalAmount(bean.getOriginalAmount().subtract(lastBean.getOriginalAmount()));
                            lastBean.setActivityAmount(bean.getActivityAmount().subtract(lastBean.getActivityAmount()));
                            lastBean.setCouponAmount(bean.getCouponAmount().subtract(lastBean.getCouponAmount()));
                            out.collect(lastBean);
                        }
                        // 更新状态，将当前数据对象更新到状态中
                        lastBeanState.update(bean);
                    }
                });

    }

    private SingleOutputStreamOperator<TradeSkuOrderBean> parseToPojo(DataStreamSource<String> stream) {
        return
        stream
                .map(new MapFunction<String, TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean map(String value) throws Exception {
                        JSONObject obj = JSON.parseObject(value);
                        return TradeSkuOrderBean.builder()
                                .orderDetailId(obj.getString("id"))
                                .skuId(obj.getString("sku_id"))
                                .orderAmount(obj.getBigDecimal("split_total_amount"))
                                .activityAmount(obj.getBigDecimal("split_activity_amount"))
                                .couponAmount(obj.getBigDecimal("split_coupon_amount"))
                                .originalAmount(obj.getBigDecimal("split_original_amount"))
                                .ts(obj.getLong("ts") * 1000)
                                .build();
                    }
                });


    }
}


/*
----------------------------------
redis 中数据结构的选择:

string
    key                       value
    表名+id                    json 格式的字符串
    dim_sku_info:1            {id:..,name:..,spu_id:...,.....}
   
    优点:
        读写方便
        可以单独的给每条维度数据设置 ttl
    缺点:
        key 特别多, 不方便管理. 与其他 key 冲突风险比较大
            专门存入一个单独库中
    

list
    key                     value
    表名                     [多行 json 数据]
    dim_sku_info            [{}, {}, {}, ...]
    
    优点:
        一张表一个 key, key 比较少,方便管理
        写方便
    缺点:
        读不方便.需要读取 list 中所有数据,然后遍历才能找到想要的数据
       
set


hash
    key             field           value
    表名              id              json 格式字符串
    dim_sku_info    1               {}
                    2               {}
                    ....
                    
      优点:
        读写方便  hset hget
        key 不多
      缺点:
        给 key 设置 ttl, 则改张表所有的维度信息同时失效
      
      

zset



-----------------------------------
缓存(内存)优化:

    1. flink 的状态
        优点:
            1. 本地内存, 读写速度极快
            2. flink 的状态提供了丰富的数据结构
                value list map reduce aggregate
                
         缺点:
            1. flink 的内存,如果存储过多的维度信息,需要占据比较多的内存,影响 flink 的计算
            2. 当维度发生变化的时候,状态中的维度不能及时更新.
 
    2. redis 旁路缓存
        
        优点:
            1. redis 提供了丰富数据结构,方便存储数据  string list set hash zset
            
            2. 当维度发生变化的时候,redis 中的维度能及时更新.
            
         缺点:
            1. 外置内存, 需要通过网络读写,速度不如本地内存速度快
            



------------------------------------
按照SKU维度分组，
    统计原始金额、
    活动减免金额、
    优惠券减免金额和
    订单金额

1. 数据源:
    下单明细事务事实表(left )

2. 去重
    由于 dwd 层的数据, 是 left join 得到, 当用普通的 kafka 消费的时候(流, sql 的普通),所以
    有重复数据.

    怎么去重?
        ....

3. 开窗聚合
    按照 sku_id 分组,开窗, 聚合

4. 补充其他维度信息
    手动补充
        1. 缓存优化
        2. 异步

5. 写出到 doris
 */
