package com.example.publisheryf.service;

import com.example.publisheryf.bean.ProductStats;
import org.apache.ibatis.annotations.Param;

import java.math.BigDecimal;
import java.util.List;

public interface ProductStatsService {
    //获取某一天的总交易额
//    public BigDecimal getGMV(int date);
//
//    //统计某天不同SPU商品交易额排名
//    public List<ProductStats> getProductStatsGroupBySpu(int date, int limit);
//
//    //统计某天不同类别商品交易额排名
//    public List<ProductStats> getProductStatsGroupByCategory3(int date,int limit);
//
//    //统计某天不同品牌商品交易额排名
//    public List<ProductStats> getProductStatsByTrademark(int date, int limit);

    public Long getAmount(int date);
    public Long getNew(int date);
    public List<ProductStats> getAmountByTm(int date,int limit);
}
