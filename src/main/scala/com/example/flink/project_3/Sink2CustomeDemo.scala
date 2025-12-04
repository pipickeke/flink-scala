package com.example.flink.project_3

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object Sink2CustomeDemo {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val dataStream: DataStream[View] = env.fromCollection(
            List(
                View("Tom", "/index.html", 1000L),
                View("Mike", "/home.html", 2000L),
                View("Bob", "/car.html", 4000L),
                View("Alice", "/about.html", 5000L)
            )
        )

        // 3. 使用自定义 Sink
        dataStream.addSink(new MyCustomViewSink())

        env.execute()
    }

    class MyCustomViewSink extends RichSinkFunction[View] {

        override def open(parameters: org.apache.flink.configuration.Configuration): Unit = {
            println("初始化完成，准备接收数据...")
        }

        // 核心方法：对每条数据进行处理
        override def invoke(value: View, context: Context): Unit = {
            val outputMsg =
                s"""|[自定义Sink 输出]
                    |    用户: ${value.user}
                    |    访问页面: ${value.url}
                    |    时间戳: ${value.timestamp}
                    |""".stripMargin

            println(outputMsg)
        }

        override def close(): Unit = {
            println("资源已释放，Sink 停止")
        }
    }
}
