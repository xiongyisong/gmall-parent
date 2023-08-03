package com.atguigu.gmall.realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.function.HbaseSink;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


@Slf4j
public class HbaseUtil {

    /**
     * 获取hbase连接对象
     * @return hbase 连接对象
     */
    public static Connection getHbaseConnection() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop162");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.client.sync.wait.timeout.msec", "10000");

        try {
            return ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }


    /**
     * 关闭 hbase 连接
     * @param hbaseConn
     */
    public static void closeHbaseConnection(Connection hbaseConn) {
        if (hbaseConn != null && !hbaseConn.isClosed()) {
            try {
                hbaseConn.close();
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }


    /**
     * 获取 hbase Admin 对象： 用来建表 删表 判断表是否存在
     * @param hbaseConn
     * @return
     */
    public static Admin getHbaseAdmin(Connection hbaseConn) {
        try {
            return hbaseConn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    /**
     * 关闭 admin 对象
     * @param admin
     */
    public static void closeHbaseAdmin(Admin admin) {
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 在 hbase 中建表
     * @param hbaseConn 到 hbase 的链接对象
     * @param nameSpace  命名空间
     * @param table  表名
     * @param family  列族
     */

    public static void createHbaseTable(Connection hbaseConn, String nameSpace, String table, String family) {


        TableName tableName = TableName.valueOf(nameSpace, table);
        // if (family.length() < 1) {
        //     throw new IllegalArgumentException("请至少需要一个列族名...");
        // }

        // 1. 判断表是否存在, 如果存在, 则直接返回
        try (Admin admin = hbaseConn.getAdmin()) {
            // 会自动关闭 admin 对象
            if (!admin.tableExists(tableName)) {
                // 2. 如果不存在, 则建表
                // family.getBytes(StandardCharsets.UTF_8) 不建议使用
                // 2.1 创建一个列族描述器
                ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder
                        .newBuilder(Bytes.toBytes(family))
                        .build();
                // 2.2 表描述器
                TableDescriptor desc = TableDescriptorBuilder
                        .newBuilder(tableName)
                        .setColumnFamily(cfd)
                        .build();
                // 2.3 创建表
                admin.createTable(desc);

                log.info("hbase的表: " + nameSpace + ":" + table + " 创建成功....");
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }


    // public static void createHbaseTable(Connection hbaseConn,
    //                                     String nameSpace,
    //                                     String tableName,
    //                                     String... families) {
    //     if (families.length < 1) {
    //         throw new IllegalArgumentException("请至少需要一个列族名...");
    //     }
    //
    //     try (Admin admin = hbaseConn.getAdmin()) {
    //         //判断表是否存在
    //         if (admin.tableExists(TableName.valueOf(nameSpace, tableName))) {
    //             System.out.println(nameSpace + ":" + tableName + "已存在");
    //             return;
    //         }
    //         TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(TableName.valueOf(nameSpace, tableName));
    //         //指定列族
    //         for (String family : families) {
    //             ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder
    //                     .newBuilder(Bytes.toBytes(family)).build();
    //             builder.setColumnFamily(familyDescriptor);
    //         }
    //         admin.createTable(builder.build());
    //     } catch (Exception e) {
    //         e.printStackTrace();
    //     }
    // }


    /**
     *  根据传入条件 删除指定的表
     * @param hbaseConn hbase 连接对象
     * @param nameSpace  空间命名
     * @param table  表名
     */
    public static void dropHbaseTable(Connection hbaseConn, String nameSpace, String table) {
        TableName tableName = TableName.valueOf(nameSpace, table);
        try (Admin admin = hbaseConn.getAdmin()) {
            if (admin.tableExists(tableName)) {
                // 1.先 disable
                admin.disableTable(tableName);
                // 2. drop
                admin.deleteTable(tableName);

                log.info("hbase的表: " + nameSpace + ":" + table + " 删除成功....");
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }


    /**
     * 自定义 hbase 连接sink
     * @return
     */
    public static SinkFunction<Tuple2<JSONObject, TableProcess>> getHbaseSink() {
        return new HbaseSink();
    }

    /**
     * 根据指定的参数，向hbase写入一行数据
     *
     * @param hbaseConn    hbase连接
     * @param nameSpaceStr 命名空间
     * @param tableNameStr 表名
     * @param rowKey       一行数据的 rowKey
     * @param columeFamily 列族
     * @param columes      要写入的列
     * @param data         data列与对应的列值
     */
    public static void putOneRow(Connection hbaseConn, String nameSpaceStr, String tableNameStr, String rowKey, String columeFamily, String[] columes, JSONObject data) {
        // 获取一张表
        TableName tableName = TableName.valueOf(nameSpaceStr, tableNameStr);
        try (Table table = hbaseConn.getTable(tableName)) {

            // 创建一个put对象
            Put put = new Put(Bytes.toBytes(rowKey));
            for (String colume : columes) {
                String da = data.getString(colume);
                if (da != null) {
                    put.addColumn(Bytes.toBytes(columeFamily), Bytes.toBytes(colume), Bytes.toBytes(da));
                }
            }
            table.put(put);

        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }


    }

    /**
     * 删除表中数据指定的行
     * @param hbaseConn hbase 连接
     * @param nameSpaceStr 命名空间
     * @param tableNameStr  表名
     * @param rowKey  rowkey
     */
    public static void deleteOneRow(Connection hbaseConn, String nameSpaceStr, String tableNameStr, String rowKey) {


        // 获取一张表
        TableName tableName = TableName.valueOf(nameSpaceStr, tableNameStr);
        try (Table table = hbaseConn.getTable(tableName)) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }
}
