package com.demo.task.practice;

import com.demo.domain.LogEntity;
import com.demo.util.Property;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.Executors;

public class NewPurchaseUser {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        SingleOutputStreamOperator<LogEntity> sourceStream=env.addSource(new ClickSource())
//                过滤
                .process(new ProcessFunction<LogEntity, LogEntity>() {
                    private Connection conn;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
                        String url = "jdbc:phoenix:hbase,hbase1,hbase2:2181";
                        conn = DriverManager.getConnection(url);
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        conn.close();
                    }

                    @Override
                    public void processElement(LogEntity value, Context ctx, Collector<LogEntity> out) throws Exception {
                        int userId=value.getUserId();
//                        首单
                        Statement statement=conn.createStatement();
                        String sql="select userid from npuser where userid ='"+userId+"'";
                        ResultSet resultSet=statement.executeQuery(sql);

//                        收集
                        if(!resultSet.next()){
                            Statement insertStatement=conn.createStatement();
                            String insertsql="upsert into npuser values('"+userId+"','"+"1')";
                            System.out.println(insertsql);
                            insertStatement.execute(insertsql);
                            conn.commit();
                            out.collect(value);
                        }
                    }

                });

        ElasticsearchSinkFunction<LogEntity> elasticsearchSinkFunction=new ElasticsearchSinkFunction<LogEntity>() {
            @Override
            public void process(LogEntity element, RuntimeContext ctx, RequestIndexer indexer) {
                Map<String,String> result=new HashMap<>();
                result.put("userId",Integer.valueOf(element.getUserId()).toString());
                result.put("productId",Integer.valueOf(element.getProductId()).toString());
                result.put("time",Long.valueOf(element.getTime()).toString());
                IndexRequest indexRequest= Requests.indexRequest().index("newpurchaseuser").source(result).id(Integer.valueOf(element.getUserId()).toString());
                indexer.add(indexRequest);
            }
        };

        List<HttpHost> httpPosts=new ArrayList<>();
        httpPosts.add(new HttpHost(new Property().getElasProperties().getProperty("host"),9201,"http"));
        ElasticsearchSink.Builder<LogEntity> builder=new ElasticsearchSink.Builder<LogEntity>(httpPosts, elasticsearchSinkFunction);
        builder.setBulkFlushMaxActions(1);

        sourceStream.addSink(builder.build());
        sourceStream.print();
        env.execute("npuser");
    }
}
