package com.sea.source;

import com.sea.entity.User;
import org.apache.flink.api.common.io.DelimitedInputFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * 自定义InputFormat
 *
 * @author Sea
 */
public class CustomFileInputFormatSourceJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<User> source = env.readFile(new JsonInputFormat(), "./input/user.txt");
        source.print();
        env.execute("Custom Format Text File Source Job");
    }

    /**
     * 参考 {@link org.apache.flink.api.java.io.TextInputFormat}
     */
    private static class JsonInputFormat extends DelimitedInputFormat<User> {
        private static final byte CARRIAGE_RETURN = (byte) '\r';
        private static final byte NEW_LINE = (byte) '\n';
        private ObjectMapper mapper;

        @Override
        public void openInputFormat() throws IOException {
            super.openInputFormat();
            mapper = new ObjectMapper();
        }

        @Override
        public User readRecord(User reusable, byte[] bytes, int offset, int numBytes) throws IOException {
            // Check if \n is used as delimiter and the end of this line is a \r, then remove \r from the line
            if (this.getDelimiter() != null
                && this.getDelimiter().length == 1
                && this.getDelimiter()[0] == NEW_LINE
                && offset + numBytes >= 1
                && bytes[offset + numBytes - 1] == CARRIAGE_RETURN) {
                numBytes -= 1;
            }
            String json = new String(bytes, offset, numBytes, StandardCharsets.UTF_8);
            return mapper.readValue(json, User.class);
        }

        @Override
        public String toString() {
            return "JsonInputFormat (" + Arrays.toString(getFilePaths()) + ")";
        }
    }
}
