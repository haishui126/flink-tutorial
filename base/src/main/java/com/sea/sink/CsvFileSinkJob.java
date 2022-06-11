package com.sea.sink;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 写Csv文件，只支持Tuple类型，已弃用
 *
 * @author Sea
 */
public class CsvFileSinkJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple3<Long, String, Integer>> source = env.fromElements(
            Tuple3.of(1L, "user1", 11),
            Tuple3.of(2L, "user2", 12),
            Tuple3.of(3L, "user3", 23),
            Tuple3.of(4L, "user4", 13)
        );
        // 写Csv只支持Tuple类型 (将写文件的并行度设置为1，可以生成一个文件)
        source.writeAsText("result.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        env.execute("Csv Sink Job");
    }
}
