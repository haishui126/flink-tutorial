package com.sea.sink;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 写文件
 *
 * @author Sea
 */
public class TextFileSinkJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> source = env.fromSequence(0, 10);
        // 在result.txt目录下生成文件1-8，因为默认有8个并行度(根据计算机CPU核心数而定)
        // source.writeAsText("result.txt", FileSystem.WriteMode.OVERWRITE);
        // 将写文件的并行度设置为1，可以生成一个文件
        source.writeAsText("result.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        env.execute("File Sink Job");
    }
}
