package com.demo.task.practice;

import akka.stream.impl.fusing.Sliding;
import com.demo.util.Property;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.*;

public class UVPVTask {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        DataStream<Tuple2<Long,Double>> stream=env.addSource(new UVPVSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UVPVEntity>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<UVPVEntity>() {
                                    @Override
                                    public long extractTimestamp(UVPVEntity element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                }
                        )
                )
                .keyBy(UVPVEntity-> UVPVEntity.getProductId())
                .window( SlidingEventTimeWindows.of(Time.seconds(60),Time.seconds(10)))
                .aggregate(new AggregateFunction<UVPVEntity, Tuple2<HashSet<String>, Long>, Double>() {
                    @Override
                    public Tuple2<HashSet<String>, Long> createAccumulator() {
                        return Tuple2.of(new HashSet<String>(), 0L);
                    }

                    @Override
                    public Tuple2<HashSet<String>, Long> add(UVPVEntity value, Tuple2<HashSet<String>, Long> accumulator) {
                        accumulator.f0.add(Long.valueOf(value.getUserId()).toString());
                        return Tuple2.of(accumulator.f0, accumulator.f1 + 1L);
                    }

                    @Override
                    public Double getResult(Tuple2<HashSet<String>, Long> accumulator) {
                        return (double) accumulator.f1 / accumulator.f0.size();
                    }

                    @Override
                    public Tuple2<HashSet<String>, Long> merge(Tuple2<HashSet<String>, Long> a, Tuple2<HashSet<String>, Long> b) {
                        return null;
                    }
                }, new WindowFunction<Double, Tuple2<Long,Double>, Long, TimeWindow>() {
                    @Override
                    public void apply(Long key, TimeWindow window, Iterable<Double> input, Collector<Tuple2<Long,Double>> out) throws Exception {
                        out.collect(Tuple2.of(key,input.iterator().next()));
                    }
                });

        ElasticsearchSinkFunction<Tuple2<Long,Double>> elasticsearchSinkFunction=new ElasticsearchSinkFunction<Tuple2<Long,Double>>() {
            @Override
            public void process(Tuple2<Long,Double> element, RuntimeContext ctx, RequestIndexer indexer) {
                Map<String,String> result=new HashMap<>();
                result.put("productId",Long.valueOf(element.f0).toString());
                result.put("pvuv",Double.valueOf(element.f1).toString());
                result.put("ts",Long.valueOf(Calendar.getInstance().getTimeInMillis()-17*24*3600*1000).toString());
                IndexRequest indexRequest= Requests.indexRequest().index("uvpv1").source(result).id(Long.valueOf(element.f0).toString());
                indexer.add(indexRequest);
            }
        };

        List<HttpHost> httpPosts=new ArrayList<>();
        httpPosts.add(new HttpHost(new Property().getElasProperties().getProperty("host"),9201,"http"));
        ElasticsearchSink.Builder<Tuple2<Long,Double>> builder=new ElasticsearchSink.Builder<Tuple2<Long,Double>>(httpPosts, elasticsearchSinkFunction);
        builder.setBulkFlushMaxActions(1);
        stream.print();
        stream.addSink(builder.build());
        env.execute("UVPV");
    }
}
