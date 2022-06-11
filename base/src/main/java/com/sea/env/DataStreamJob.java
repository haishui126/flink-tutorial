package com.sea.env;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Sea
 */
@Slf4j
public class DataStreamJob {

    public static void main(String[] args) throws Exception {
        /*
        1. 使用IDEA直接运行：LocalStreamEnvironment
        2. 使用集群运行：StreamContextEnvironment
        3. Jar提交到Session集群，“Show Plan”：StreamPlanEnvironment
        */
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        log.info("current StreamExecutionEnvironment: {}", env.toString());

        env.fromSequence(0, 10).print();

        // Execute program, beginning computation.
        env.execute("getExecutionEnvironmentDemo");
    }
}
