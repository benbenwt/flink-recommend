package com.demo.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class PhoenixUtil {
    public static void main(String[] args) throws Throwable {

        try {

            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");

            //这里配置zookeeper的地址，可单个，多个(用","分隔)可以是域名或者ip

            String url = "jdbc:phoenix:hbase,hbase1,hbase2:2181";

            Connection conn = DriverManager.getConnection(url);

            Statement statement = conn.createStatement();

            long time = System.currentTimeMillis();

            ResultSet rs = statement.executeQuery("select * from npuser");

            while (rs.next()) {
                String myKey = rs.getString("userid");
                String myColumn = rs.getString("flag");

                System.out.println("userid=" + myKey + " flag=" + myColumn);
            }

            long timeUsed = System.currentTimeMillis() - time;

            System.out.println("time " + timeUsed + "mm");

            // 关闭连接
            rs.close();
            statement.close();
            conn.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
