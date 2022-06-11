package com.sea.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 写Socket
 *
 * @author Sea
 */
public class SocketSinkJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.readTextFile("input/user.txt");
        // linux环境中执行 nc -l 9999
        source.writeToSocket("localhost", 9999, new SimpleStringSchema());
        env.execute("Socket Sink Job");
    }
}
