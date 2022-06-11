package com.sea.sink;

import com.sea.entity.User;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * 自定义POJO写入csv，第一行为字段头
 *
 * @author Sea
 */
public class CustomCsvSinkJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<User> source = env.fromElements(
            new User(1L, "user1", 11),
            new User(2L, "user2", 12),
            new User(3L, "user3", 23),
            new User(4L, "user4", 13),
            new User(10L, null, null)
        );
        // 自定义FileOutputFormat
        CsvOutputFormat<User> format = new CsvOutputFormat<>(new Path("result.txt"), User.class);
        format.setWriteMode(FileSystem.WriteMode.OVERWRITE);
        source.writeUsingOutputFormat(format).setParallelism(1);
        env.execute("Custom Csv Sink Job");
    }

    /**
     * 参考 {@link org.apache.flink.api.java.io.TextOutputFormat}
     *
     * @param <T> DataStream类型
     */
    private static class CsvOutputFormat<T> extends FileOutputFormat<T> {
        private static final int NEWLINE = '\n';
        private final Class<T> type;
        private transient List<Field> fields;

        public CsvOutputFormat(Path outputPath, Class<T> type) {
            super(outputPath);
            this.type = type;
        }

        @Override
        public void open(int taskNumber, int numTasks) throws IOException {
            super.open(taskNumber, numTasks);
            fields = TypeExtractor.getAllDeclaredFields(type, true);
            fields.forEach(field -> field.setAccessible(true));
            // 写Csv头
            String title = fields.stream().map(Field::getName).collect(Collectors.joining(","));
            this.stream.write(title.getBytes(StandardCharsets.UTF_8));
            this.stream.write(NEWLINE);
        }

        @Override
        public void writeRecord(T record) throws IOException {
            String content = fields.stream().map(field -> {
                try {
                    return Objects.toString(field.get(record));
                } catch (IllegalAccessException e) {
                    return null;
                }
            }).collect(Collectors.joining(","));
            this.stream.write(content.getBytes(StandardCharsets.UTF_8));
            this.stream.write(NEWLINE);
        }

        @Override
        public String toString() {
            return "CsvOutputFormat (" + getOutputFilePath() + ")";
        }
    }
}
