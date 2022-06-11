package com.sea.env;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Sea
 */
public class RemoteEnvJob {
    public static void main(String[] args) throws Exception {
        /*
         * 使用IDE运行代码时将任务传到远程集群运行。
         * 程序运行后，IDE显示项目在运行，日志在集群输出，print在本地，可以通过IDE或WebUI停止Job。
         * env.executeAsync()可以让程序直接结束，只能通过WebUI停止Job。
         *
         * 创建一个RemoteStreamEnvironment。将程序（部分）发送到集群以执行。
         * 请注意，程序中使用的所有文件路径都必须可从集群访问。
         * 除非通过 setParallelism 显式设置并行度，否则执行将不使用并行度。
         *
         * @param host 远程Session集群IP
         * @param port rest.port，默认8081
         * @param jarFiles 包含需要传送到集群的代码的 JAR 文件。
         * 如果程序使用用户定义的函数、用户定义的输入格式或任何库，则必须在 JAR 文件中提供这些。
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("flink-host", 8081);
        env.fromSequence(0, 10).print();
        env.execute("Remote Env Job");
//         env.executeAsync("Remote Env Job");
    }
}
