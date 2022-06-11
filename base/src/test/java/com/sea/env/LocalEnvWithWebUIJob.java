package com.sea.env;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Sea
 */
public class LocalEnvWithWebUIJob {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // 可用配置项见 https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/config/
        conf.setInteger("rest.port", 8082);
        // 需要flink-runtime-web依赖
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        // 无界流环境，否则程序运行结束webUI会停止
        env.socketTextStream("localhost", 7777).print();

        env.execute("Local Env With WebUI Job");
    }
}