package com.demo.task.practice;

import com.demo.domain.LogEntity;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import javax.xml.crypto.Data;

public class KeywordsFilter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<LogEntity> sourceStream=env.addSource(new ClickSource());
        DataStream<String> configStream=env.addSource(new MysqlSource("hbase",3306,"con","root","root",1));

        MapStateDescriptor<String,String> configStateDescriptor=new MapStateDescriptor<String, String>("config", Types.STRING,Types.STRING);
        BroadcastStream<String> broadcastConfigStream=configStream.broadcast(configStateDescriptor);
        BroadcastConnectedStream<LogEntity,String> broadcastConnectedStream=sourceStream.connect(broadcastConfigStream);

        DataStream<LogEntity> filterStream=broadcastConnectedStream.process(new BroadcastProcessFunction<LogEntity,String,LogEntity>(){
            private String keyword="-1";

            @Override
            public void processElement(LogEntity value, ReadOnlyContext ctx, Collector<LogEntity> out) throws Exception {
                if(value.getProductId()==Integer.parseInt(keyword))
                {
                    out.collect(value);
                }
            }

            @Override
            public void processBroadcastElement(String value, Context ctx, Collector<LogEntity> out) throws Exception {
                keyword=value;
            }
        });
        filterStream.print();
        env.execute("keywordFilter");
    }
}
