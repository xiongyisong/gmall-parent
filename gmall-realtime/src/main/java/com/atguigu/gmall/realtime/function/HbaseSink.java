package com.atguigu.gmall.realtime.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.util.HbaseUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.OnlineLogRecord;

/**
 * @title: HbaseSink
 * @Author joey
 * @Date: 2023/7/31 18:51
 * @Version 1.0
 * @Note:
 */
public class HbaseSink extends RichSinkFunction<Tuple2<JSONObject, TableProcess>> {
    private Connection hbaseConn;


    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseConn = HbaseUtil.getHbaseConnection();
    }

    @Override
    public void close() throws Exception {
        HbaseUtil.closeHbaseConnection(hbaseConn);

    }


    @Override
    public void invoke(Tuple2<JSONObject, TableProcess> t, Context context) throws Exception {
        // 来一条数据，触发一次
        JSONObject data = t.f0;
        String opType = data.getString("op_type");
        // insert update delete
        // 如果是 insert 或 update的 时候，向hbase中写一行

        // 如果是delete 则是删除 hbase中一行
        if ("delete".equals(opType)) {
            deleteOneRow(t);
        } else if ("insert".equals(opType) || "update".equals(opType)) {
            putOneRow(t);
        }

    }

    private void putOneRow(Tuple2<JSONObject, TableProcess> t) {

        JSONObject data = t.f0;
        TableProcess tp = t.f1;
        HbaseUtil.putOneRow(hbaseConn,
                "gmall",
                tp.getSinkTable(),
                data.getString(tp.getSinkRowKey()),
                tp.getSinkFamily(),
                tp.getSinkColumns().split(","),
                data);


    }

    private void deleteOneRow(Tuple2<JSONObject, TableProcess> t) {
        JSONObject data = t.f0;
        TableProcess tp = t.f1;
        HbaseUtil.deleteOneRow(hbaseConn,
                "gmall",
                tp.getSinkTable(),
                data.getString(tp.getSinkRowKey()));


    }

}
