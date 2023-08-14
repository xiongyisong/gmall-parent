package com.atguigu.gmall.gmallpublisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @title: Spu
 * @Author joey
 * @Date: 2023/8/14 17:25
 * @Version 1.0
 * @Note:
 */


@Data
@NoArgsConstructor
@AllArgsConstructor
public class Spu {

    private String spu_name;
    private BigDecimal order_amount;

}
