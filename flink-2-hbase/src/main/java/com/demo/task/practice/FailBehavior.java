package com.demo.task.practice;

import com.demo.domain.LogEntity;
import com.demo.util.Property;
import com.typesafe.config.ConfigIncluderFile;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.apache.kafka.common.protocol.types.Field;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FailBehavior {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        System.setProperty("HADOOP_USER_NAME", "root");
        System.setProperty("user.name", "root");

        env.setParallelism(4);
//        checkpoint
        env.enableCheckpointing(1000);
        CheckpointConfig config=env.getCheckpointConfig();
        config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        config.setMinPauseBetweenCheckpoints(500);
        config.setCheckpointTimeout(60000);
        config.setMaxConcurrentCheckpoints(1);
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        config.enableUnalignedCheckpoints();
        config.setCheckpointStorage("hdfs://hbase:9000/flink/checkpoints");

        DataStream<LogEntity> sourceStream=env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<LogEntity>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<LogEntity>() {
                                            @Override
                                            public long extractTimestamp(LogEntity value, long l)
                                            {
                                                return value.getTime();
                                            }
                                        }
                                )
                )
                .keyBy(LogEntity->LogEntity.getUserId());
//        sourceStream.print();
        Pattern<LogEntity,LogEntity> pattern=Pattern
                .<LogEntity>begin("first")
                .where(new SimpleCondition<LogEntity>() {
                    @Override
                    public boolean filter(LogEntity value) throws Exception {
                        return value.getAction().equals("0");
                    }
                })
                .next("second")
                .where(new SimpleCondition<LogEntity>() {
                    @Override
                    public boolean filter(LogEntity value) throws Exception {
                        return value.getAction().equals("1");

                    }
                })
                .next("third")
                .where(new SimpleCondition<LogEntity>() {
                    @Override
                    public boolean filter(LogEntity value) throws Exception {
                        return value.getAction().equals("2");
                    }
                });

        PatternStream<LogEntity> patternStream=CEP.pattern(sourceStream,pattern);
        DataStream<Tuple4<Integer,Long,Long,Long>> stream=patternStream.select(new PatternSelectFunction<LogEntity, Tuple4<Integer,Long,Long,Long>>() {
            @Override
            public Tuple4<Integer,Long,Long,Long> select(Map<String, List<LogEntity>> map) throws Exception {
                LogEntity first=map.get("first").get(0);
                LogEntity second=map.get("second").get(0);
                LogEntity third=map.get("third").get(0);
                return Tuple4.of(first.getUserId(),first.getTime(),second.getTime(),third.getTime());
            }
        });
        stream.print("warning");
//        sinkfunciton
//        httphost
//        essinkbulder
        ElasticsearchSinkFunction<Tuple4<Integer,Long,Long,Long>> elasticsearchSinkFunction=new ElasticsearchSinkFunction<Tuple4<Integer,Long,Long,Long>>() {
            @Override
            public void process(Tuple4<Integer,Long,Long,Long> element, RuntimeContext ctx, RequestIndexer indexer) {
                Map<String,String> result=new HashMap<>();
                result.put("userId",Integer.valueOf(element.f0).toString());
                result.put("first",Long.valueOf(element.f1).toString());
                result.put("second",Long.valueOf(element.f2).toString());
                result.put("third",Long.valueOf(element.f3).toString());
                IndexRequest indexRequest= Requests.indexRequest().index("cepfailbehavior").source(result).id(Integer.valueOf(element.f0).toString());
                indexer.add(indexRequest);
            }
        };
        List<HttpHost> httpPosts=new ArrayList<>();
        httpPosts.add(new HttpHost(new Property().getElasProperties().getProperty("host"),9201,"http"));
        ElasticsearchSink.Builder<Tuple4<Integer,Long,Long,Long>> builder=new ElasticsearchSink.Builder<Tuple4<Integer,Long,Long,Long>>(httpPosts, elasticsearchSinkFunction);
        builder.setBulkFlushMaxActions(1);
        stream.addSink(builder.build());

        env.execute("FailBehavior");
    }
}
