package com.atguigu.gmall.realtime.function;

import com.alibaba.fastjson.JSONObject;

public interface DimFunction<T> {
    String getTableName();

    String getId(T bean);
    void  addDim(T bean, JSONObject dim);



}
