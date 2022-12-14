package com.example.publisheryf.controller;

import com.example.publisheryf.bean.ProductStats;
import com.example.publisheryf.service.ProductStatsService;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@RestController
@RequestMapping("/api/sugar")
public class SugarController {

    @Autowired
    ProductStatsService productStatsService;

    /*
    {
        "status": 0,
        "msg": "",
        "data": 1201081.1632389291
    }
     */
//    @RequestMapping("/gmv")
//    public String getGMV(@RequestParam(value = "date",defaultValue = "0") Integer date) {
//        if(date==0){
//            date=now();
//        }
//        BigDecimal gmv = productStatsService.getGMV(date);
//        String json = "{   \"status\": 0,  \"data\":" + gmv + "}";
//        return  json;
//    }
//
    private int now(){
        String yyyyMMdd = DateFormatUtils.format(new Date(), "yyyyMMdd");
        return   Integer.valueOf(yyyyMMdd);
    }
//    @RequestMapping("/spu")
//    public String getProductStatsGroupBySpu(
//            @RequestParam(value = "date", defaultValue = "0") Integer date,
//            @RequestParam(value = "limit", defaultValue = "10") int limit) {
//        if (date == 0) date = now();
//        List<ProductStats> statsList
//                = productStatsService.getProductStatsGroupBySpu(date, limit);
//        //设置表头
//        StringBuilder jsonBuilder =
//                new StringBuilder(" " +
//                        "{\"status\":0,\"data\":{\"columns\":[" +
//                        "{\"name\":\"商品名称\",\"id\":\"spu_name\"}," +
//                        "{\"name\":\"交易额\",\"id\":\"order_amount\"}," +
//                        "{\"name\":\"订单数\",\"id\":\"order_ct\"}]," +
//                        "\"rows\":[");
//        //循环拼接表体
//        for (int i = 0; i < statsList.size(); i++) {
//            ProductStats productStats = statsList.get(i);
//            if (i >= 1) {
//                jsonBuilder.append(",");
//            }
//            jsonBuilder.append("{\"spu_name\":\"" + productStats.getSpu_name() + "\"," +
//                    "\"order_amount\":" + productStats.getOrder_amount() + "," +
//                    "\"order_ct\":" + productStats.getOrder_ct() + "}");
//
//        }
//        jsonBuilder.append("]}}");
//        return jsonBuilder.toString();
//    }
//    @RequestMapping("/category3")
//    public String getProductStatsGroupByCategory3(
//            @RequestParam(value = "date", defaultValue = "0") Integer date,
//            @RequestParam(value = "limit", defaultValue = "4") int limit) {
//        if (date == 0) {
//            date = now();
//        }
//        List<ProductStats> statsList
//                = productStatsService.getProductStatsGroupByCategory3(date, limit);
//
//        StringBuilder dataJson = new StringBuilder("{  \"status\": 0, \"data\": [");
//        int i = 0;
//        for (ProductStats productStats : statsList) {
//            if (i++ > 0) {
//                dataJson.append(",");
//            }
//            ;
//            dataJson.append("{\"name\":\"")
//                    .append(productStats.getCategory3_name()).append("\",");
//            dataJson.append("\"value\":")
//                    .append(productStats.getOrder_amount()).append("}");
//        }
//        dataJson.append("]}");
//        return dataJson.toString();
//    }
//    @RequestMapping("/trademark")
//    public String getProductStatsByTrademark(
//            @RequestParam(value = "date", defaultValue = "0") Integer date,
//            @RequestParam(value = "limit", defaultValue = "20") int limit) {
//        if (date == 0) {
//            date = now();
//        }
//        List<ProductStats> productStatsByTrademarkList
//                = productStatsService.getProductStatsByTrademark(date, limit);
//        List<String> tradeMarkList = new ArrayList<>();
//        List<BigDecimal> amountList = new ArrayList<>();
//        for (ProductStats productStats : productStatsByTrademarkList) {
//            tradeMarkList.add(productStats.getTm_name());
//            amountList.add(productStats.getOrder_amount());
//        }
//        String json = "{\"status\":0,\"data\":{" + "\"categories\":" +
//                "[\"" + StringUtils.join(tradeMarkList, "\",\"") + "\"],\"series\":[" +
//                "{\"data\":[" + StringUtils.join(amountList, ",") + "]}]}}";
//        return json;
//    }
    @RequestMapping("/amount")
    public String getAmount(@RequestParam(value = "date",defaultValue = "0") Integer date) {
        if(date==0){
            date=now();
        }
        Long amount = productStatsService.getAmount(date);
        String json = "{   \"status\": 0,  \"data\":" + amount + "}";
        return  json;
    }
    @RequestMapping("/new")
    public String getNew(@RequestParam(value = "date",defaultValue = "0") Integer date) {
        if(date==0){
            date=now();
        }
        Long amount = productStatsService.getAmount(date);
        String json = "{   \"status\": 0,  \"data\":" + amount + "}";
        return  json;
    }
    @RequestMapping("/tm")
    public String getAmountByTm(
            @RequestParam(value = "date", defaultValue = "0") Integer date,
            @RequestParam(value = "limit", defaultValue = "10") int limit) {
        if (date == 0) date = now();
        List<ProductStats> statsList
                = productStatsService.getAmountByTm(date, limit);
        //设置表头
        StringBuilder jsonBuilder =
                new StringBuilder(" " +
                        "{\"status\":0,\"data\":{\"columns\":[" +
                        "{\"name\":\"商品名称\",\"id\":\"tm\"}," +
                        "{\"name\":\"品牌交易额\",\"id\":\"total_amount\"}," +
                        "{\"name\":\"订单数\",\"id\":\"pro_cnt\"}]," +
                        "\"rows\":[");
        //循环拼接表体
        for (int i = 0; i < statsList.size(); i++) {
            ProductStats productStats = statsList.get(i);
            if (i >= 1) {
                jsonBuilder.append(",");
            }
            jsonBuilder.append("{\"tm\":\"" + productStats.getTm() + "\"," +
                    "\"total_amount\":" + productStats.getTotal_amount() + "," +
                    "\"pro_ct\":" + productStats.getPro_cnt() + "}");

        }
        jsonBuilder.append("]}}");
        return jsonBuilder.toString();
    }

}
