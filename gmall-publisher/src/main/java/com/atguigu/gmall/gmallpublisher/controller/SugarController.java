package com.atguigu.gmall.gmallpublisher.controller;

import com.atguigu.gmall.gmallpublisher.service.TradeService;
import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;

/**
 * @title: SugarController
 * @Author joey
 * @Date: 2023/8/14 17:28
 * @Version 1.0
 * @Note:
 */

@RestController
public class SugarController {

    @Autowired
    TradeService tradeService;

    @RequestMapping("/sugar/trade/gmv")
    public String gmv(@RequestParam(value = "date",defaultValue = "0") Integer date){
        if (date==0) {
             date = Integer.valueOf(new SimpleDateFormat("yyyyMMdd").format(System.currentTimeMillis()));
        }
        return tradeService.gmv(date);
    }


    @RequestMapping("/sugar/trade/gmv/spu")
    public String gmvBySpu(@RequestParam(value = "date",defaultValue = "0") Integer date){
        if (date==0) {
            Integer.valueOf(new SimpleDateFormat("yyyyMMdd").format(System.currentTimeMillis()));
        }

        return tradeService.gmvBySpu(date);
    }



}
