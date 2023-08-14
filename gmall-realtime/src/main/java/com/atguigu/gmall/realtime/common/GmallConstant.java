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
    public static final String TOPIC_DWD_INTERACTION_COMMENT_INFO = "dwd_interaction_comment_info";
    public static final String TOPIC_DWD_TRADE_CART_ADD = "dwd_trade_cart_add";

    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail";

    public static final String TOPIC_DWD_TRADE_CANCEL_DETAIL = "dwd_trade_cancel_detail";

    public static final String TOPIC_DWD_TRADE_PAY_DETAIL_SUC = "dwd_trade_pay_detail_suc";

    public static final String TOPIC_DWD_TRADE_ORDER_REFUND = "dwd_trade_order_refund";

    public static final String TOPIC_DWD_TRADE_REFUND_PAY_SUC = "dwd_trade_refund_pay_suc";

    public static final long SEVEN_DAY_MS = 7 * 24 * 60 * 60 * 1000;

    public static final String TOPIC_DWD_USER_REGISTER = "dwd_user_register";
    public static final int TWO_DAY_SECONDS = 2 * 24 * 60 * 60;
}