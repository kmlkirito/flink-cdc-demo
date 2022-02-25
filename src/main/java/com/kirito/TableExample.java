package com.kirito;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

public class TableExample {


    public static void main(String[] args) throws Exception {
//        EnvironmentSettings settings = EnvironmentSettings
//                .newInstance()
//                .inStreamingMode()
//                .build();
//
//        TableEnvironment tableEnv = TableEnvironment.create(settings);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

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
        Table table = tableEnv.sqlQuery("select * from orders");
        TableResult execute = table.execute();
        CloseableIterator<Row> collect = execute.collect();
        collect.forEachRemaining(it -> {
            for (String fieldName : it.getFieldNames(true)) {
                System.out.print(fieldName + " -> " + it.getField(fieldName) + "  ");
            }
            System.out.println();
        });
        env.execute();
    }
}
