package com.sea.operator.keyed;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author Sea
 */
public class ReduceFunJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.readTextFile("input/words.txt");
        // ReduceFunction: value1=上次reduce的返回值，value2=新的value
        // 第一个value不会执行Reduce方法，直接作为第二个value来后的value1
        source
            .flatMap(new Tokenizer())
            .keyBy(value -> value.f0)
            .reduce((value1, value2) -> Tuple2.of(value1.f0, value1.f1 + value2.f1))
            .print();
        env.execute("Reduce Job");
    }

    private static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // 切割
            String[] tokens = value.toLowerCase().split("\\W+");
            // 输出到下一个算子
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(Tuple2.of(token, 1));
                }
            }
        }
    }
}
