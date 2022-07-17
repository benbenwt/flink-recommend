package com.demo.task.practice.sqlpractice;

import com.demo.task.practice.ClickSource;
import com.demo.task.practice.StartLog;
import com.demo.task.practice.StartLogSource;
import com.sun.appserv.util.cache.CacheListener;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DailyActiveUser {
    public static void main(String[] args) {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment=StreamTableEnvironment.create(env);

        DataStream<StartLog> sourceStream=env.addSource(new StartLogSource());
        Table sourceTable=tableEnvironment.fromDataStream(sourceStream);
        tableEnvironment.toDataStream(sourceTable).print();
        tableEnvironment.to
//        stream转table
//        从外部数据源直接创建table
        tableEnvironment.executeSql("CREATE TEMPORARY TABLE test(" +
                "`product_id` INT," +
                "`product_name` STRING," +
                "`color` STRING," +
                "`diameter` STRING," +
                "`style` STRING," +
                "`material` STRING," +
                "`country` STRING" +
                ")" +
                "WITH (" +
                "'connector'= 'jdbc'," +
                "'url'= 'jdbc:mysql//hbase:3306/con'," +
                "'table-name'='product'" +
                ")");
        Table queryResult= tableEnvironment.executeSql("select ")

    }

}
