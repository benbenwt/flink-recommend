package com.demo.task.practice;

import com.demo.domain.LogEntity;
import com.demo.domain.TopProductEntity;
import com.demo.util.Property;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
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

public class ProcessAllWindowTopN {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<LogEntity> sourceStream=env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<LogEntity>forMonotonousTimestamps()
                           .withTimestampAssigner(new SerializableTimestampAssigner<LogEntity>() {
                               @Override
                               public long extractTimestamp(LogEntity element, long recordTimestamp) {
                                   return element.getTime();
                               }
                           })
                );

//        SingleOutputStreamOperator<Integer> result=sourceStream.map(new MapFunction<LogEntity, Integer>() {
//            @Override
//            public Integer map(LogEntity value) throws Exception {
//                return value.getProductId();
//            }
//        });

        SingleOutputStreamOperator<TopProductEntity> result=sourceStream.map(logEntity->logEntity.getProductId())
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(60),Time.seconds(10)))
                .process(new ProcessAllWindowFunction<Integer, TopProductEntity, TimeWindow>() {
                             @Override
                             public void process(Context context, Iterable<Integer> iterable, Collector<TopProductEntity> collector) throws Exception {
                                 HashMap<Integer,Long> productCountMap=new HashMap<>();

                                 for(Integer productId:iterable){
                                     if(productCountMap.containsKey(productId))
                                     {
                                         Long oldValue=productCountMap.get(productId);
                                         productCountMap.put(productId,oldValue+1L);
                                     }
                                     else{
                                         productCountMap.put(productId,1L);
                                     }
                                 }

                                 ArrayList<Tuple2<Integer,Long>> productIdCountList=new ArrayList<>();
                                 for(Map.Entry<Integer,Long> entry:productCountMap.entrySet()){
                                     productIdCountList.add(Tuple2.of(entry.getKey(),entry.getValue()));
                                 }
                                 productIdCountList.sort(new Comparator<Tuple2<Integer, Long>>() {
                                     @Override
                                     public int compare(Tuple2<Integer, Long> o1, Tuple2<Integer, Long> o2) {
                                         return o2.f1.intValue()-o1.f1.intValue();
                                     }
                                 });
                                for(int i=0;i<10;i++){
                                    Tuple2<Integer,Long> temp=productIdCountList.get(i);
                                    collector.collect(TopProductEntity.of(temp.f0,context.window().getEnd(),temp.f1));
                                }

                             }
                         }
                );
        result.print();

//        httphost ,essinkfucntion,essink.builder,build
        ElasticsearchSinkFunction<TopProductEntity> elasticsearchSinkFunction=new ElasticsearchSinkFunction<TopProductEntity>() {
            @Override
            public void process(TopProductEntity element, RuntimeContext ctx, RequestIndexer indexer) {
                Map<String,String> data=new HashMap<>();
                data.put("productid",Integer.valueOf(element.getProductId()).toString());
                data.put("times",Integer.valueOf(element.getActionTimes()).toString());
                data.put("windowEnd",Long.valueOf(element.getWindowEnd()).toString());

                IndexRequest indexRequest= Requests.indexRequest().index("topproduct").source(data);
                indexer.add(indexRequest);
            }
        };

        List<HttpHost> httpPosts=new ArrayList<>();
        httpPosts.add(new HttpHost(new Property().getElasProperties().getProperty("host"),9201,"http"));
        ElasticsearchSink.Builder<TopProductEntity> builder=new ElasticsearchSink.Builder<TopProductEntity>(httpPosts, elasticsearchSinkFunction);
        builder.setBulkFlushMaxActions(1);
        result.addSink(builder.build());
        env.execute();
    }
}
