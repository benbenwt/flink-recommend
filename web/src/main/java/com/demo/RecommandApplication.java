package com.demo;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 * @author XINZE
 */
@SpringBootApplication
@MapperScan("com.demo.dao")
@ComponentScan("com.demo")
public class RecommandApplication {

    public static void main(String[] args) {
        SpringApplication.run(RecommandApplication.class);
    }
}
