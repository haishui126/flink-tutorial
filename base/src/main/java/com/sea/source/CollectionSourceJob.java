package com.sea.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

/**
 * 集合、序列Source
 *
 * @author Sea
 */
public class CollectionSourceJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 每5s判断文件有变化则重新完整输出一遍
        DataStreamSource<Long> source1 = env.fromSequence(0, 10);
        DataStreamSource<Long> source2 = env.fromCollection(List.of(1L, 2L, 3L, 4L, 5L));
        DataStreamSource<Long> source3 = env.fromElements(1L, 2L, 3L);
        source1.print("source1");
        source2.print("source2");
        source3.print("source3");
        env.execute("Collection Source Job");
    }
}
