package com.atguigu.gmall.realtime.util;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * @title: DateFormatUtil
 * @Author joey
 * @Date: 2023/8/2 15:01
 * @Version 1.0
 * @Note:
 */
public class DateFormatUtil {

    private static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * 把年月日时分秒转成毫秒值  2023-08-01 11:11:11 => Long 的毫秒值
     * @param dateTime
     * @return
     */
    public static Long dateTimeToTs(String dateTime) {

        LocalDateTime localDateTime  = LocalDateTime.parse(dateTime, dateTimeFormatter);

        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }



    /**
     * 把年月日转成毫秒值  2023-08-01 => Long 的毫秒值
     * @param date
     * @return
     */
    public static Long dateToTs(String date) {
        LocalDateTime localDateTime  = LocalDateTime.parse(date + " 00:00:00", dateTimeFormatter);

        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }

    /**
     * 把毫秒值转成年月日
     * @param ts
     * @return
     */
    public static String tsToDate(Long ts) {
        Date dt = new Date(ts);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
        return dateFormatter.format(localDateTime);
    }

    /**
     * 把毫秒值转成年月日时分秒
     * @param ts
     * @return
     */
    public static String tsToDateTime(Long ts) {
        Date dt = new Date(ts);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
        return dateTimeFormatter.format(localDateTime);
    }


    public static void main(String[] args) {
        System.out.println(System.currentTimeMillis());
        System.out.println(tsToDate(System.currentTimeMillis()));

        System.out.println(dateTimeToTs("2023-08-02 10:04:28"));
        System.out.println(System.currentTimeMillis());
    }


}
