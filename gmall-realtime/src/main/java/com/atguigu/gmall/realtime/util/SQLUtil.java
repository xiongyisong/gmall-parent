package com.atguigu.gmall.realtime.util;

import com.atguigu.gmall.realtime.common.GmallConstant;

import java.text.Format;

/**
 * @title: SQLUtil
 * @Author joey
 * @Date: 2023/8/3 10:51
 * @Version 1.0
 * @Note:
 */


public class SQLUtil {
    public static String getKafkaSourceSQL(String groupId, String topic, String... format) {
        String defaultFormat = "json";
        if (format.length > 0) {
            defaultFormat = format[0];
        }

        return "with(" + "'connector' = 'kafka'," + "  'topic' = '" + topic + "'," + "  'properties.bootstrap.servers' = '" + GmallConstant.KAFKA_BROKERS + "'," + "  'properties.group.id' = '" + groupId + "'," + "  'scan.startup.mode' = 'latest-offset'," + ("json".equals(defaultFormat) ? " 'json.ignore-parse-errors' = 'true', " : "") + "  'format' = '" + defaultFormat + "'" +

                ")";

    }

    /**
     * 拼接的 flinkSql中的kafkaSink
     * @param topic topic
     * @param format 格式
     * @return
     */
    public static String getKafkaSinkSQL(String topic,String... format) {
        String defaultFormat = "json";
        if (format.length>0) {
            defaultFormat = format[0];
        }
        return "with(" +
                " 'connector' = 'kafka'," +
                "  'topic' = '" + topic + "'," +
                "  'properties.bootstrap.servers' = '" + GmallConstant.KAFKA_BROKERS + "'," +
                "  'format' = '" + defaultFormat + "'" +
                ")";

    }

    /**
     * sql拼接 upsertkafka
     * @param topic
     * @param format
     * @return
     */
    public static String getUpsertKafkaSQL(String topic, String ... format) {
        String defaultFormat = "json";
        if (format.length>0){
            defaultFormat=format[0];
        }
        return "with(" +
                "  'connector' = 'upsert-kafka'," +
                "  'topic' = '" + topic + "'," +
                "  'properties.bootstrap.servers' = '" + GmallConstant.KAFKA_BROKERS + "'," +
                ("json".equals(defaultFormat) ? " 'key.json.ignore-parse-errors' = 'true', " : "") +
                ("json".equals(defaultFormat) ? " 'value.json.ignore-parse-errors' = 'true', " : "") +
                "  'key.format' = '" + defaultFormat + "', " +
                "  'value.format' = '" + defaultFormat + "'" +
                ")";
    }
}
