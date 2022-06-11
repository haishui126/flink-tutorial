package com.sea.operator.data;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author Sea
 */
public class FlatMapFunJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.readTextFile("input/words.txt");
        source.flatMap(new Tokenizer()).print();
        env.execute("FlatMap Job");
    }

    private static final class Tokenizer implements FlatMapFunction<String, String> {

        @Override
        public void flatMap(String value, Collector<String> out) {
            // 切割
            String[] tokens = value.toLowerCase().split("\\W+");
            // 输出到下一个算子
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(token);
                }
            }
        }
    }
}
