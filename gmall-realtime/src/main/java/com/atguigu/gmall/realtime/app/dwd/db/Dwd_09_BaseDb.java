package com.atguigu.gmall.realtime.app.dwd.db;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseApp;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.GmallConstant;
import com.atguigu.gmall.realtime.util.JdbcUtil;
import com.atguigu.gmall.realtime.util.KafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;


import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @title: Dwd_09_BaseDb
 * @Author joey
 * @Date: 2023/8/7 18:48
 * @Version 1.0
 * @Note:
 */

@Slf4j
public class Dwd_09_BaseDb extends BaseApp {
    public static void main(String[] args) {
        new Dwd_09_BaseDb().start(30009, 2, "Dwd_09_BaseDb", GmallConstant.TOPIC_ODS_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        SingleOutputStreamOperator<String> etledStream = etl(stream);
        SingleOutputStreamOperator<TableProcess> tpStream = readTableProcess(env);
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> dwdDataTpStream = connect(etledStream, tpStream);
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> resultStream = delNotNeedColumns(dwdDataTpStream);

        writeToKafka(resultStream);

    }

    private void writeToKafka(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> resultStream) {
        resultStream.sinkTo(KafkaUtil.getKafkaSink());
    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> delNotNeedColumns(
	SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> dimDataAndTpStream) {
            return  dimDataAndTpStream
                    .map(new MapFunction<Tuple2<JSONObject, TableProcess>, Tuple2<JSONObject,TableProcess>>() {
                        @Override
                        public Tuple2<JSONObject, TableProcess> map(Tuple2<JSONObject, TableProcess> t) throws Exception {
                            JSONObject data = t.f0;
                            //删除不需要的列的时候，保留op_type字段
                            List<String> columns = Arrays.asList((t.f1.getSinkColumns() + ",op_type").split(","));
                            data.keySet().removeIf(key ->!columns.contains(key));
                            return t;
                        }
                    });
    }


    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connect(
            SingleOutputStreamOperator<String> dataStream,
            SingleOutputStreamOperator<TableProcess> tpStream) {
        // 1. 把配置流做成广播流
        // key: mysql中的表名+操作类型
        // value:  TableProcess
        MapStateDescriptor<String, TableProcess> desc = new MapStateDescriptor<>("tp", String.class, TableProcess.class);
        BroadcastStream<TableProcess> tpBcStream = tpStream.broadcast(desc);
        // 2. 数据流去 connect 广播流
        return dataStream
                .connect(tpBcStream)
                .process(new BroadcastProcessFunction<String, TableProcess, Tuple2<JSONObject, TableProcess>>() {
                    private Map<String, TableProcess> map;
                /*
                    open 方法中读取所有的配置信息, 预加载
                    16 条配置信息,加载成功之后,存储到什么地方?
                        1.能否存入到广播状态中? 不能在 open 方法中读写状态
                        2. 存入一个 java 提供的数据结构: Map<String, TableProcess>
                 */

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 配置信息的预加载
                        // 1. 获取 jdbc mysql 连接
                        map = new HashMap<>();
                        java.sql.Connection conn = JdbcUtil.getMysqlConnection();
                        List<TableProcess> tps = JdbcUtil.queryList(conn,
                                "select * from gmall2023_config.table_process",
                                new Object[]{},
                                TableProcess.class,
                                true
                        );

                        for (TableProcess tp : tps) {
                            map.put(getKey(tp.getSourceTable(), tp.getSourceType()), tp);
                        }
                        System.out.println("预加载的map = " + map);
                        JdbcUtil.closeConnection(conn);

                    }

                    // 4. 在处理数据流中数据的时候, 从广播状态中获取对应的配置信息
                    // 每来一条元素执行一次
                    @Override
                    public void processElement(String jsonStr,
                                               ReadOnlyContext ctx,
                                               Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                        JSONObject obj = JSON.parseObject(jsonStr);
                        ReadOnlyBroadcastState<String, TableProcess> state = ctx.getBroadcastState(desc);
                        String key = getKey(obj.getString("table"), obj.getString("type"));

                        // 1. 先从状态中查询配置
                        TableProcess tp = state.get(key);
                        // 2. 如果状态中没有找到, 再去 Map 中查找
                        if (tp == null) {
                       System.out.println("在状态中没有找到: " + key);
                            tp = map.get(key);
                            if (tp == null) {
                           System.out.println("在map中也没有找到: " + key);
                            }else{
                           System.out.println("在map中找到: " + key);
                            }
                        }else{
                       System.out.println("在状态中找到: " + key);
                        }

                        if (tp != null) { // 这条数据, 找到了配置信息
                            JSONObject data = obj.getJSONObject("data");
                            data.put("op_type", obj.getString("type").replace("bootstrap-", "")); // 后期在写入 hbase 的时候要使用

                            out.collect(Tuple2.of(data, tp));
                        }
                    }

                    // 3. 把配置信息存储到广播状态中
                    // 每来一条配置信息, 每个并行度执行一次.
                    @Override
                    public void processBroadcastElement(TableProcess tp,
                                                        Context ctx,
                                                        Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                        // 1.获取广播状态
                        BroadcastState<String, TableProcess> state = ctx.getBroadcastState(desc);
                        String key = getKey(tp.getSourceTable(), tp.getSourceType());
                        // 2. 把配置写入到广播状态
                        if ("d".equals(tp.getOp())) {
                            state.remove(key);  // 如果配置信息删除, 则删除状态
                            map.remove(key);  // 同时删除预加载的配置信息
                        } else {
                            state.put(key, tp); // 如果不是删除,则更新或者添加
                        }
                    }

                    private String getKey(String table, String operateType) {
                        return table + ":" + operateType;
                    }
                });


    }


    /**
     * 启动 flinkcdc配置 去监控binlog 读取数据变化
     *
     * @param env
     * @return
     */
    private SingleOutputStreamOperator<TableProcess> readTableProcess(StreamExecutionEnvironment env) {


         /*
        op
            r: 读取的是快照
                before: null  after: 有值
            u: 更新
                before: 有值   after: 有值
            c: 新增
                before: null  after: 有值
            d: 删除
                before: 有值  after: null

            更新主键:
                先 d 再 c

         */

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(GmallConstant.MYSQL_HOST)
                .port(GmallConstant.MYSQL_PORT)
                .databaseList(GmallConstant.CONFIG_DATABASE)
                .tableList(GmallConstant.CONFIG_DATABASE + ".table_process")
                .username("root")
                .password("aaaaaa")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        return env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "m")
                .setParallelism(1)
                .map(new MapFunction<String, TableProcess>() {
                    @Override
                    public TableProcess map(String Value) throws Exception {
                        JSONObject obj = JSON.parseObject(Value);
                        String op = obj.getString("op");
                        TableProcess tp = null;
                        if ("d".equals(op)) {
                            obj.getObject("before", TableProcess.class);
                        } else {
                            tp = obj.getObject("after", TableProcess.class);
                        }
                        tp.setOp(op);
                        return tp;
                    }
                })
                .filter(tp -> "dwd".equals(tp.getSinkType()));

    }


    /**
     * 清洗数据 只接受 json格式数据
     *
     * @param stream
     * @return
     */
    private SingleOutputStreamOperator<String> etl(DataStreamSource<String> stream) {
        return stream
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        try {
                            JSONObject obj = JSON.parseObject(value);

                            String type = obj.getString("type");
                            String data = obj.getString("data");
                            return "gmall2023".equals(obj.getString("database"))
                                    && null != obj.getString("table")
                                    && null != obj.getString("ts")
                                    && ("insert".equals(type) || "update".equals(type) || "bootstrap-insert".equals(type))
                                    && null != data
                                    && data.length() > 2;
                        } catch (Exception e) {
                            log.warn("数据格式不是 json: " + value);
                            return false;
                        }
                    }
                });
    }
}
