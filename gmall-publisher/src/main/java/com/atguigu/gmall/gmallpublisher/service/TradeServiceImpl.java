package com.atguigu.gmall.gmallpublisher.service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.gmallpublisher.bean.Spu;
import com.atguigu.gmall.gmallpublisher.mapper.TradeMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

/**
 * @title: TradeServiceImpl
 * @Author joey
 * @Date: 2023/8/14 17:27
 * @Version 1.0
 * @Note:
 */
@Service
public class TradeServiceImpl implements TradeService{

    @Autowired
    TradeMapper tradeMapper;

    @Override
    public String gmv(Integer date) {
        BigDecimal gmv = tradeMapper.gmv(date);
        JSONObject obj = new JSONObject();
        obj.put("status",0);
        obj.put("msg","");
        obj.put("data",gmv);

        return obj.toJSONString();


    }

    @Override
    public String gmvBySpu(Integer date) {
        List<Spu> list = tradeMapper.gmvBySpu(date);

        JSONObject result = new JSONObject();


        result.put("status", 0);
        result.put("msg", "");

        JSONObject data = new JSONObject();
        JSONArray categories = new JSONArray();
        for (Spu spu : list) {
            categories.add(spu.getSpu_name());
        }
        data.put("categories", categories);

        JSONArray series = new JSONArray();
        JSONObject obj = new JSONObject();
        obj.put("name", "商品的 spu");
        JSONArray data1 = new JSONArray();
        for (Spu spu : list) {
            data1.add(spu.getOrder_amount());
        }
        obj.put("data", data1);
        series.add(obj);

        data.put("series",series);


        result.put("data",data);
        return result.toJSONString();

    }
}
