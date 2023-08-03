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
}
