package com.youfan.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;


@Data
@NoArgsConstructor
@AllArgsConstructor

public class TestBean {
    public String id;
    public Long total_amount=0L;
    public String is_new;
    public Long pro_cnt=0L;
    public String tm;
    public Date date;

}
