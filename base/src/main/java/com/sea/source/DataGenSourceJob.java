package com.sea.source;

import com.sea.entity.User;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;
import org.apache.flink.streaming.api.functions.source.datagen.SequenceGenerator;

/**
 * @author Sea
 */
public class DataGenSourceJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 生成userId从1到100的数据，每秒1条，生成10条数据（因为多并行度，输出的Id是1到10个一个，不保证顺序）
        DataGeneratorSource<User> dataGenSource = new DataGeneratorSource<>(new SequenceUserGenerator(1, 100), 1, 10L);
        env.addSource(dataGenSource).returns(User.class).print("SequenceGenerator");

        // 每秒随机生成10条User数据，无限生成（numberOfRows=null）
        DataGeneratorSource<User> dataGenSource2 = new DataGeneratorSource<>(new RandomUserGenerator(), 10, null);
        env.addSource(dataGenSource2).returns(User.class).print("SequenceGenerator");
        env.execute("Random DateGen Source Job");
    }

    private static class RandomUserGenerator extends RandomGenerator<User> {
        @Override
        public User next() {
            return new User(
                random.nextLong(0, Long.MAX_VALUE),
                random.nextHexString(4),
                random.nextInt(1, 100)
            );
        }
    }

    private static class SequenceUserGenerator extends SequenceGenerator<User> {
        /**
         * 参考{@link RandomGenerator#random}
         */
        protected transient RandomDataGenerator random;

        public SequenceUserGenerator(long start, long end) {
            super(start, end);
        }

        @Override
        public void open(String name, FunctionInitializationContext context, RuntimeContext runtimeContext) throws Exception {
            super.open(name, context, runtimeContext);
            random = new RandomDataGenerator();
        }

        @Override
        public User next() {
            return new User(
                // 获取队列中的第一个数据但不取出
                valuesToEmit.peek(),
                // 去除队列中的第一个数据
                "user" + valuesToEmit.pop(),
                random.nextInt(1, 100)
            );
        }
    }
}
