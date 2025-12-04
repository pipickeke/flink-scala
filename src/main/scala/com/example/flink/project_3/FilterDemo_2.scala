package com.example.flink.project_3

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object FilterDemo_2 {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val dataStream: DataStream[View] = env.fromCollection(
            List(
                View("Tom", "/index.html", 1000L),
                View("Mike", "/home.html", 2000L),
                View("Bob", "/car.html", 4000L),
            )
        )

        // 只保留 timestamp > 2000 的记录
        val filteredStream = dataStream.filter(view => view.timestamp > 2000)

        // 打印过滤后的 View 对象
        filteredStream.print()
        env.execute()
    }
}
