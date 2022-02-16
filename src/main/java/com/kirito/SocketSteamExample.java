package com.kirito;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class SocketSteamExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);
        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("119.45.20.168", 5020);
//        SingleOutputStreamOperator<Tuple2<String, Integer>> dataStream =
//                stringDataStreamSource.flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
//                            for (String word : value.split(" ")) {
//                                out.collect(new Tuple2<>(word, 1));
//                            }
//                        })
//                        .returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
//                        }))
//                        .keyBy(it -> it.f0)
//                        .window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
//                        .sum(1);
//        dataStream.print();
//        stringDataStreamSource.print();


        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table table = tableEnv.fromDataStream(stringDataStreamSource, $("name"));
        table.groupBy($("name")).select($("name"), $("name").count().as("数量")).execute().print();

        env.execute("Window WordCount");


    }
}
