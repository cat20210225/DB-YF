package com.example.publisheryf.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.Date;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProductStats {
    public String id;
    public Long total_amount=0L;
    public String is_new;
    public Long pro_cnt=0L;
    public String tm;
    public Date date;
}
