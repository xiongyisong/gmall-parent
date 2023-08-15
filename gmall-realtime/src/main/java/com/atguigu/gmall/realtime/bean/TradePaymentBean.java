package com.atguigu.gmall.realtime.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TradePaymentBean {
    // 窗口起始时间
    String stt;
    // 窗口终止时间
    String edt;
    String curDate;
    // 支付成功独立用户数
    Long paymentSucUniqueUserCount;
    // 支付成功新用户数
    Long paymentSucNewUserCount;
    // 时间戳
    @JSONField(serialize = false)
    Long ts;
}