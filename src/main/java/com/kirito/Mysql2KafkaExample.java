package com.kirito;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class Mysql2KafkaExample {
    public static void main(String[] args) throws Exception {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("119.45.20.168")
                .port(3306)
                .databaseList("flink_demo") // set captured database
//                .tableList("flink_demo.products") // set captured table
                .username("root")
                .password("kirito1993~!@#")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .startupOptions(StartupOptions.initial())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().enableCheckpointing(30000);
        DataStreamSource<String> mySQLSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQLSource");
        mySQLSource.print();
        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<>(
                "119.45.20.168:9092",
                "quickstart-events",
                new SimpleStringSchema()   // 序列化 schema
        );

        mySQLSource.addSink(myProducer);

        env.execute("Mysql2KafkaExample");
    }
}
