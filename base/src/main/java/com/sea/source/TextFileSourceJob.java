package com.sea.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 按行读取文件
 *
 * @author Sea
 */
public class TextFileSourceJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.readTextFile("./input/user.txt");
        source.print();
        env.execute("TextFile Source Job");
    }
}
