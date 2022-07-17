package com.demo.task.practice;

import com.demo.domain.LogEntity;
import com.demo.util.Property;
import com.demo.util.RedisUtil;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.util.Collector;

import org.apache.http.HttpHost;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import redis.clients.jedis.Jedis;

import java.text.SimpleDateFormat;
import java.util.*;

public class DailyActiveUser {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        SingleOutputStreamOperator<StartLog> sourceStream=env.addSource(new StartLogSource())
                .process(new ProcessFunction<StartLog, StartLog>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        jedis= RedisUtil.connectRedis(Property.getStrValue("redis.host"));
                        if(jedis!=null){
                            System.out.println("jedis连接成功"+jedis);
                        }
                        simpleDateFormat=new SimpleDateFormat("yyyy-MM-dd");
                    }
                    private Jedis jedis;
                    private SimpleDateFormat simpleDateFormat;
                    @Override
                    public void processElement(StartLog value, Context ctx, Collector<StartLog> out) throws Exception {
//                        查询内存状态
                        Long userId=value.getUserId();
                        Long time=value.getTs();
//                        dt
                        Date date=new Date(time);
                        Long flag=jedis.sadd("flinkdau"+simpleDateFormat.format(date), String.valueOf(userId));
//
                        if(jedis.ttl("flinkdau"+String.valueOf(date))==-1L)
                        {
                            jedis.expire("flinkdau"+String.valueOf(date), 3600 * 24);
                        }
                        if(flag==1){
                            out.collect(value);
                        }
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        jedis.close();
                    }
                });
//        sourceStream.print();

//        httppost
//elasticsearchSinkFunction
        ElasticsearchSinkFunction<StartLog> elasticsearchSinkFunction=new ElasticsearchSinkFunction<StartLog>() {
            @Override
            public void process(StartLog element, RuntimeContext ctx, RequestIndexer indexer) {
                Map<String,String> result=new HashMap<>();
                result.put("userId",Long.valueOf(element.getUserId()).toString());
                result.put("entry",element.getEntry());
                result.put("ts",Long.valueOf(element.getTs()).toString());
                IndexRequest indexRequest= Requests.indexRequest().index("flinkdaustartlog").type("startLog").source(result).id(Long.valueOf(element.getUserId()).toString());
                indexer.add(indexRequest);
            }
        };
        List<HttpHost> httpPosts=new ArrayList<>();
        httpPosts.add(new HttpHost(new Property().getElasProperties().getProperty("host"),9200,"http"));
        ElasticsearchSink.Builder<StartLog> builder=new ElasticsearchSink.Builder<StartLog>(httpPosts, elasticsearchSinkFunction);
        builder.setBulkFlushMaxActions(1);
        sourceStream.addSink(builder.build());
        sourceStream.print();
        env.execute("flinkdau");
    }
}
