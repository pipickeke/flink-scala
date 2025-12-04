package com.example.flink.project_3

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object SocketTextStreamDemo {

    def main(args: Array[String]): Unit = {
        // 1. 创建 Flink 的流处理执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        // 2. 定义 Socket 主机和端口，例如 localhost:9999
        val hostname = "localhost"
        val port = 9999

        // 3. 从 Socket 创建数据流（逐行文本）
        val textStream: DataStream[String] = env.socketTextStream(hostname, port)

        // 4. 简单处理：打印每一行数据
        textStream.print()

        // 5. 执行 Flink 作业
        env.execute("Flink Socket Text Stream Example")


    }

}
