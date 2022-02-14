package com.kirito;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class SocketSteamExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, Integer>> dataStream = env.socketTextStream("119.45.20.168", 5020)
                .flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
                    for (String word : value.split(" ")) {
                        out.collect(new Tuple2<String, Integer>(word, 1));
                    }
                })
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                }))
                .keyBy(it -> it.f0)
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1);
        dataStream.print();
        env.execute("Window WordCount");
    }
}
