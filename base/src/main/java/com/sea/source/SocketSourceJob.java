package com.sea.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 读取Socket流
 *
 * @author Sea
 */
public class SocketSourceJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 在Linux环境中 nc -lk 7777 创建socket
        DataStreamSource<String> source = env.socketTextStream("localhost", 7777);
        source.print();
        env.execute("Socket Source Job");
    }
}
