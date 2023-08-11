package com.atguigu.gmall.realtime.function;

import com.atguigu.gmall.realtime.util.IkUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.Set;

/**
 * @title: KwSplit
 * @Author joey
 * @Date: 2023/8/11 17:40
 * @Version 1.0
 * @Note:
 */

@FunctionHint(output = @DataTypeHint("row<kw string>"))
public class KwSplit extends TableFunction<Row> {

    public void eval(String keyword) {
        if (keyword == null) return;
        // 手机华为手机白色手机
        Set<String> kws = IkUtil.split(keyword);

        for (String kw : kws) {
            collect(Row.of(kw));  // 每调用一次, 表中的数据增加一行
        }

    }
}
