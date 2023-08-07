package com.atguigu.gmall.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseApp;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.GmallConstant;
import com.atguigu.gmall.realtime.util.HbaseUtil;
import com.atguigu.gmall.realtime.util.JdbcUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.JdbcUrlUtils;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
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
import org.apache.hadoop.hbase.client.Connection;

import java.util.*;


@Slf4j
public class DimApp extends BaseApp {
    public static void main(String[] args) {


        new DimApp().start(20001, 2, "DimApp", GmallConstant.TOPIC_ODS_DB);

    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

      /*
      对流做具体的处理
      1.对数据做清洗
      2.通过cdc读取配置信息
      3.在hbase中建表
      4.数据流和配置流进行 connect
      5.根据配置信息 写入到对应的hbase对应的表中
      6.数据根据配置信息，写入到hbase对应的表中
       */

        SingleOutputStreamOperator<String> etledStream = etl(stream);

        SingleOutputStreamOperator<TableProcess> tpStream = readTableProcess(env);


        tpStream = createHbaseTable(tpStream);

        // 数据流和配置流进行 connect

        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> dimDataAndTpStream = connect(etledStream, tpStream);
         dimDataAndTpStream.print();

        // 删除数据中不需要的列
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> resultSream = delNotNeedColumns(dimDataAndTpStream);
        resultSream.print();

        // 数据根据配置信息，写入到hbase对应的表中
        writeToHbase(resultSream);


    }

    private void writeToHbase(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> resultSream) {

        /*
        如何写入到hbase中
        1.查询有没有专门的hbase连接器
        没有专门的hbase连接器
        2.自定义sink
         */
        resultSream.addSink(HbaseUtil.getHbaseSink());

    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> delNotNeedColumns(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> dimDataAndTpStream) {
        return dimDataAndTpStream.map(new MapFunction<Tuple2<JSONObject, TableProcess>, Tuple2<JSONObject, TableProcess>>() {
            @Override
            public Tuple2<JSONObject, TableProcess> map(Tuple2<JSONObject, TableProcess> t) throws Exception {
                JSONObject data = t.f0;

                List<String> colums = Arrays.asList((t.f1.getSinkColumns() + ",op_type").split(","));

                // for (String key : data.keySet()) {
                //     if (!colums.contains(key)) {
                //             data.remove(key);
                //     }
                // }
                data.keySet().removeIf(key -> !colums.contains(key));

                // for (Map.Entry<String, Object> stringObjectEntry : data.entrySet()) {
                // 遍历拿到 key value
                // }

                return t;
            }
        });


    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connect(SingleOutputStreamOperator<String> dataStream, SingleOutputStreamOperator<TableProcess> tpStream) {


        // 1. 把配置流做成广播流
        // key: mysql中的表名+操作类型
        // value:  TableProcess
        MapStateDescriptor<String, TableProcess> desc = new MapStateDescriptor<>("tp", String.class, TableProcess.class);
        BroadcastStream<TableProcess> tpBcStream = tpStream.broadcast(desc);


        // 2. 数据流去 connect 广播流
        return dataStream
                .connect(tpBcStream).process(new BroadcastProcessFunction<String, TableProcess, Tuple2<JSONObject, TableProcess>>() {

                    private Map<String, TableProcess> map;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                /*
                    open 方法中读取所有的配置信息, 预加载
                    16 条配置信息,加载成功之后,存储到什么地方?
                        1.能否存入到广播状态中? 不能在 open 方法中读写状态
                        2. 存入一个 java 提供的数据结构: Map<String, TableProcess>
                 */
                        map = new HashMap<>();

                        java.sql.Connection conn = JdbcUtil.getMysqlConnection();
                        List<TableProcess> tps = JdbcUtil.queryList(conn, "select * from gmall2023_config.table_process", new Object[]{}, TableProcess.class, true);

                        for (TableProcess tp : tps) {
                            map.put(getKey(tp.getSourceTable(), tp.getSourceType()), tp);
                        }
                           System.out.println("预加载的map = " + map);
                        JdbcUtil.closeConnection(conn);
                    }


                    // 4. 在处理数据流中数据的时候, 从广播状态中获取对应的配置信息
                    // 每来一条元素执行一次
                    @Override
                    public void processElement(String s, ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcess>> ct) throws Exception {
                        JSONObject obj = JSON.parseObject(s);
                        ReadOnlyBroadcastState<String, TableProcess> state = readOnlyContext.getBroadcastState(desc);
                        String key = getKey(obj.getString("table"), "ALL");
                        TableProcess tableProcess = state.get(key);

                        if (tableProcess == null) {
                             System.out.println("在状态中没有找到：" + key);
                            tableProcess = map.get(key);
                            if (tableProcess == null) {
                                 System.out.println("在map中也没有找到：" + key);
                            } else {
                                System.out.println("在map中找到："+ key);
                            }
                        } else {

                             System.out.println("在状态中找到：" + key);
                        }

                        if (tableProcess != null) { // 这条数据, 找到了配置信息
                            JSONObject data = obj.getJSONObject("data");
                            data.put("op_type", obj.getString("type").replace("bootstrap-", ""));

                            ct.collect(Tuple2.of(data, tableProcess));
                        }

                    }


                    // 3. 处理广播流中的元素: 把配置信息存入到广播状态中
                    // 每来一个配置信息, 这个方法执行多次: 每个并行度执行一次
                    @Override
                    public void processBroadcastElement(TableProcess tableProcess, Context ctx, Collector<Tuple2<JSONObject, TableProcess>> ct) throws Exception {
                        BroadcastState<String, TableProcess> state = ctx.getBroadcastState(desc);
                        String key = getKey(tableProcess.getSourceTable(), tableProcess.getSourceType());
                        if ("d".equals(tableProcess.getOp())) {
                            state.remove(key);  // 如果配置信息删除, 则删除状态
                            map.remove(key);    // 预加载的map中也要删除
                        } else {
                            state.put(key, tableProcess); // 如果不是删除,则更新或者添加
                        }

                    }

                    private String getKey(String table, String operateType) {
                        return table + ":" + operateType;
                    }
                });
    }


    private SingleOutputStreamOperator<TableProcess> createHbaseTable(
            SingleOutputStreamOperator<TableProcess> tpStream) {
        return tpStream.map(new RichMapFunction<TableProcess, TableProcess>() {

            private Connection hbaseConn;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 1. 建立一个到 hbase 的连接
                // 2.获取一个Admin
                hbaseConn = HbaseUtil.getHbaseConnection();


            }

            @Override
            public void close() throws Exception {

                // 4.关闭hbase连接和关闭admin
                HbaseUtil.closeHbaseConnection(hbaseConn);


            }


            @Override
            public TableProcess map(TableProcess tableProcess) throws Exception {

                // 3.建表操作
                String op = tableProcess.getOp();
                if ("d".equals(op)) {
                    // 删表
                    dropTable(tableProcess);
                } else if ("r".equals(op) || "c".equals(op)) {
                    // 建表
                    createTable(tableProcess);
                } else {
                    // 删表
                    dropTable(tableProcess);
                    // 建表
                    createTable(tableProcess);
                }

                return tableProcess;
            }

            private void dropTable(TableProcess tableProcess) {
                HbaseUtil.dropHbaseTable(hbaseConn, "gmall", tableProcess.getSinkTable());
            }

            private void createTable(TableProcess tableProcess) {
                HbaseUtil.createHbaseTable(hbaseConn, "gmall", tableProcess.getSinkTable(), tableProcess.getSinkFamily());
            }

        });
    }


    private SingleOutputStreamOperator<TableProcess> readTableProcess(StreamExecutionEnvironment env) {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(GmallConstant.MYSQL_HOST)
                .port(GmallConstant.MYSQL_PORT)
                .databaseList(GmallConstant.CONFIG_DATABASE)
                .tableList(GmallConstant.CONFIG_DATABASE + ".table_process")
                .username("root").password("aaaaaa")
                .startupOptions(StartupOptions.initial())
                // 启动之后,先读取快照(表中已有所有数据), 然后再去监控 binlog 读取变化数据
                .deserializer(new JsonDebeziumDeserializationSchema())
                // converts SourceRecord to JSON String
                .build();
        return env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql-cdc-source").setParallelism(1).map(new MapFunction<String, TableProcess>() {
            @Override
            public TableProcess map(String s) throws Exception {
                JSONObject obj = JSON.parseObject(s);
                String op = obj.getString("op");
                TableProcess tp;
                if ("d".equals(op)) {
                    tp = obj.getObject("before", TableProcess.class);
                } else {
                    tp = obj.getObject("after", TableProcess.class);
                }
                tp.setOp(op);
                return tp;
            }
        })
                .filter(tp ->"dim".equals(tp.getSinkType()));

    }

    private SingleOutputStreamOperator<String> etl(DataStreamSource<String> stream) {
        return stream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {

                try {
                    JSONObject obj = JSON.parseObject(s);

                    String type = obj.getString("type");
                    String data = obj.getString("data");
                    return
                            "gmall2023".equals(obj.getString("database"))
                                    && null != obj.getString("table")
                                    && null != obj.getString("ts")
                                    && ("insert".equals(type) || "update".equals(type) || "delete".equals(type) || "bootstrap-insert".equals(type))
                                    && null != data
                                    && data.length() > 2;


                } catch (Exception e) {
                    // System.out.println("数据格式不是 json: " + value);
                    // trace debug info warn error fatal
                    log.warn("数据格式不是 json: " + s);
                    return false;
                }
            }
        });

    }
}

/*
完成维度层：
读取ods层数据（ods_db）,过滤出想要的维度表数据，写入到hbase中 一张维度表 对应hbase中的一张表

 */