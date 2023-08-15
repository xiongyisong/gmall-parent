package com.atguigu.gmall.realtime.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CartAddUuBean {
    // 窗口起始时间
    String stt;
    // 窗口闭合时间
    String edt;
    String curDate;
    // 加购独立用户数
    Long cartAddUuCt;
    // 时间戳
    @JSONField(serialize = false)
    Long ts;
}