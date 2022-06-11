package com.sea.operator.data;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Sea
 */
public class FilterFunJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> source = env.fromSequence(0, 10);
        source
            .filter(value -> value % 2 == 0)
            .print("lambda");

        //noinspection Convert2Lambda
        source
            .filter(new FilterFunction<>() {
                @Override
                public boolean filter(Long value) {
                    return value % 2 == 0;
                }
            })
            .print("匿名类");

        source.filter(new EvenFilter()).print("实现接口");
        env.execute("Filter Function Job");
    }

    private static class EvenFilter implements FilterFunction<Long> {

        @Override
        public boolean filter(Long value) {
            return value % 2 == 0;
        }
    }
}
