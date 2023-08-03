package com.atguigu.gmall.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @title: TableProcess
 * @Author joey
 * @Date: 2023/7/31 0:57
 * @Version 1.0
 * @Note:
 */


@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableProcess {
    String sourceTable;
    // 来源操作类型

    String sourceType;
    // 输出表

    String sinkTable;

    // 输出类型 dwd | dim

    String sinkType;
    // 数据到 hbase 的列族

    String sinkFamily;

    // 输出字段

    String sinkColumns;

    // sink到 hbase 的时候的主键字段

    String sinkRowKey;

    // 建表扩展（一些额外参数可以在此配置）

    String sinkExtend;


    String op; // 配置表操作: c r u d

}
