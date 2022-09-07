package com.example.publisheryf;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.example.publisheryf.mapper")
public class PublisherYfApplication {

    public static void main(String[] args) {
        SpringApplication.run(PublisherYfApplication.class, args);
    }

}
