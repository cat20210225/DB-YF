package com.example.publisheryf.service.impl;

import com.example.publisheryf.bean.ProductStats;
import com.example.publisheryf.mapper.ProductStatsMapper;
import com.example.publisheryf.service.ProductStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

@Service
public class ProductStatsServiceImpl implements ProductStatsService {

    @Autowired
    ProductStatsMapper productStatsMapper;

//    @Override
//    public BigDecimal getGMV(int date) {
//        return productStatsMapper.getGMV(date);
//    }
//    @Override
//    public List<ProductStats> getProductStatsGroupBySpu(int date, int limit) {
//        return productStatsMapper.getProductStatsGroupBySpu(date,  limit);
//    }
//
//    @Override
//    public List<ProductStats> getProductStatsGroupByCategory3(int date, int limit) {
//        return productStatsMapper.getProductStatsGroupByCategory3(date,  limit);
//    }
//
//    @Override
//    public List<ProductStats> getProductStatsByTrademark(int date,int limit) {
//        return productStatsMapper.getProductStatsByTrademark(date,  limit);
//    }

    @Override
    public Long getAmount(int date) {
        return productStatsMapper.getAmount(date);
    }

    @Override
    public Long getNew(int date) {
        return productStatsMapper.getNew(date);
    }

    @Override
    public List<ProductStats> getAmountByTm(int date, int limit) {
        return productStatsMapper.getAmountByTm(date, limit);
    }
}
