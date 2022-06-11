package com.sea.source;

import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

/**
 * 检测文件内容变化
 *
 * @author Sea
 */
public class ContinuouslyFileSourceJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String filePath = "./input/user.txt";
        TextInputFormat format = new TextInputFormat(new Path(filePath));
        // 每5s判断文件有变化则重新完整输出一遍
        DataStreamSource<String> source = env.readFile(format, filePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 5000);
        source.print();
        env.execute("Continuously TextFile Source Job");
    }
}
