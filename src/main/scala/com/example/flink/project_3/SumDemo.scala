package com.example.flink.project_3

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object SumDemo {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val dataStream: DataStream[View] = env.fromCollection(
            List(
                View("Tom", "/index.html", 1000L),
                View("Mike", "/home.html", 2000L),
                View("Bob", "/car.html", 4000L),
                View("Tom", "/about.html", 3000L),
            )
        )

        // 转换为 (user, timestamp)，然后按 user 分组并对 timestamp 求和
        val sumStream = dataStream
            .map(view => (view.user, view.timestamp))
            .keyBy(_._1) // 按 user 分组
            .sum(1) // 对第二个字段（timestamp）求和

        sumStream.print()
        env.execute()
    }
}
