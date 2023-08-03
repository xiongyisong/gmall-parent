package com.atguigu.gmall.realtime.common;

/**
 * @title: GmallConstant
 * @Author joey
 * @Date: 2023/7/30 22:12
 * @Version 1.0
 * @Note:
 */
public class GmallConstant {
    public static final String KAFKA_BROKERS = "hadoop162:9092,hadoop163:9092,hadoop164:9092";
    public static final String TOPIC_ODS_DB = "ods_db";

    public static final String TOPIC_ODS_LOG = "ods_log";
    public static final String MYSQL_HOST = "hadoop162";
    public static final int MYSQL_PORT = 3306;
    public static final String CONFIG_DATABASE = "gmall2023_config";
    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://hadoop162:3306?useSSL=false";


    public static final String TOPIC_DWD_TRAFFIC_START = "dwd_traffic_start";
    public static final String TOPIC_DWD_TRAFFIC_ERR = "dwd_traffic_err";
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "dwd_traffic_action";
    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "dwd_traffic_display";
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "dwd_traffic_page";

}
