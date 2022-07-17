package com.demo.task.practice;

import com.demo.domain.LogEntity;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SavePoint {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new ClickSource())
                .uid("source-id")
                .map(LogEntity->LogEntity)
                .uid("mapper-id")
                .print();
        env.execute("savepoint");
    }
}
