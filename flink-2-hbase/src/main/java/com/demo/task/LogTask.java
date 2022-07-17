package com.demo.task;

import com.demo.map.LogMapFunction;
import com.demo.util.Property;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;

/**
 * 日志 -> Hbase
 *
 * @author XINZE
 */
public class LogTask {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//kafka 指定goupid
        Properties properties = Property.getKafkaProperties("logs");
//        读取log数据，map函数
        DataStreamSource<String> dataStream = env.addSource(new FlinkKafkaConsumer<String>("con", new SimpleStringSchema(), properties));
        dataStream.map(new LogMapFunction());

        env.execute("Log message receive");
    }
}
