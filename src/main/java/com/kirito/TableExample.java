package com.kirito;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class TableExample {


    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);
        tableEnv.executeSql("CREATE TEMPORARY table orders (\n" +
                "   order_id INT,\n" +
                "   order_date TIMESTAMP(0),\n" +
                "   customer_name STRING,\n" +
                "   price DECIMAL(10, 5),\n" +
                "   product_id INT,\n" +
                "   order_status BOOLEAN,\n" +
                "   PRIMARY KEY (order_id) NOT ENFORCED\n" +
                " ) WITH (\n" +
                "   'connector' = 'mysql-cdc',\n" +
                "   'hostname' = '119.45.20.168',\n" +
                "   'port' = '3306',\n" +
                "   'username' = 'root',\n" +
                "   'password' = 'kirito1993~!@#',\n" +
                "   'database-name' = 'flink_demo',\n" +
                "   'table-name' = 'orders'\n" +
                " )");
        final Table orders = tableEnv.from("orders");
        final Table counts = orders.groupBy($("order_id"))
                .select($("order_id"), $("order_id").count().as("数量"));
        counts.execute().print();
    }
}