package com.demo.task;

import com.demo.map.GetLogFunction;
import com.demo.map.UserHistoryWithInterestMapFunction;
import com.demo.util.Property;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class UserInterestTask {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = Property.getKafkaProperties("interest");
        DataStreamSource<String> dataStream = env.addSource(new FlinkKafkaConsumer<String>("con", new SimpleStringSchema(), properties));
        dataStream.map(new GetLogFunction()).keyBy("userId").map(new UserHistoryWithInterestMapFunction());
//如果spark要实现key分组，然后在组内的窗口上进行操作。可以使用reducebyKey。
//        flink的keyBy函数功能很强大，直接写字段即可。
        env.execute("User Product History");
    }
}
