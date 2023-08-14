package com.atguigu.gmall.gmallpublisher.mapper;


import com.atguigu.gmall.gmallpublisher.bean.Spu;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;

@Mapper
public interface TradeMapper {

    @Select("select " +
            "sum(order_amount) order_amount " +
            "from dws_trade_sku_order_window " +
            "partition (par#{date})")
    BigDecimal gmv(Integer date);


    @Select("select " +
            "spu_name, " +
            "sum(order_amount) order_amount " +
            "from dws_trade_sku_order_window " +
            "partition (par#{date}) " +
            "group by spu_name")
    List<Spu> gmvBySpu(@Param("date") Integer date);


}
