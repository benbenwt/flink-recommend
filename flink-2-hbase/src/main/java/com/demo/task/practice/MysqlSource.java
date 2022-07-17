package com.demo.task.practice;


import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class MysqlSource extends RichSourceFunction<String> {
    private boolean running=true;
    private Connection connection;

    private String host;
    private Integer port;
    private String db;
    private String user;
    private String passwd;
    private Integer secondInterval;
    private PreparedStatement preparedStatement;

    public MysqlSource(String host, Integer port, String db, String user, String passwd, Integer secondInterval) {
        this.host = host;
        this.port = port;
        this.db = db;
        this.user = user;
        this.passwd = passwd;
        this.secondInterval = secondInterval;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("com.mysql.jdbc.Driver");
        connection= DriverManager.getConnection("jdbc:mysql://"+host+":"+port+"/"+db+"?useUnicode=true&characterEncoding=UTF-8",user,passwd);
        String sql="select keyword from config";
        preparedStatement=connection.prepareStatement(sql);
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (running){
            ResultSet resultset=preparedStatement.executeQuery();
            String keyword;
            while (resultset.next()){
//                action1=resultset.getString("action1");
//                action2=resultset.getString("action2");
//                action3=resultset.getString("action3");
                keyword=resultset.getString("keyword");
                ctx.collect(keyword);
            }
            Thread.sleep(1000*secondInterval);
        }
    }

    @Override
    public void cancel() {
        running=false;
    }

    @Override
    public void close() throws Exception {
        super.close();
        if(connection!=null){
            connection.close();
        }
        if(preparedStatement!=null){
            preparedStatement.close();
        }
    }
}
