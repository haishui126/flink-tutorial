package com.sea.env;


import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Sea
 */
public class LocalEnvJob {
    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.fromSequence(0, 10).print();

        env.execute("Local Env Job");
    }
}