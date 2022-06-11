package com.sea.operator.keyed;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Sea
 */
public class MaxJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple3<String, String, Integer>> source = env.fromElements(
            Tuple3.of("a", "a1", 1),
            Tuple3.of("a", "a2", 2),
            Tuple3.of("b", "b1", 1),
            Tuple3.of("b", "b2", 2),
            Tuple3.of("a", "a8", 8),
            Tuple3.of("a", "a5", 5)
        );
        // 取Tuple3的f2最大
        source
            .keyBy(value -> value.f0)
            .max(2)
            .print("max");
        // 自定义Reduce函数实现max方法
        source
            .keyBy(value -> value.f0)
            .reduce(new CustomMaxFun())
            .print("custom");

        env.execute("Max Job");
    }

    private static class CustomMaxFun implements ReduceFunction<Tuple3<String, String, Integer>> {

        @Override
        public Tuple3<String, String, Integer> reduce(Tuple3<String, String, Integer> value1, Tuple3<String, String, Integer> value2) {
            if (value2.f2 > value1.f2) {
                value1.f2 = value2.f2;
            }
            return value1;
        }
    }
}
