package com.sea.operator.data;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Sea
 */
public class MapFunJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //noinspection Convert2Lambda
        env.fromSequence(0, 10)
           .map(new MapFunction<Long, String>() {
               @Override
               public String map(Long value) {
                   return (value % 2 == 0 ? "even: " : "odd: ") + value;
               }
           })
           .print();
        env.execute("Map Function Job");
    }
}
