package com.example.flink.project_3

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object RescaleDemo {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val dataStream: DataStream[View] = env.fromCollection(
            List(
                View("Tom", "/index.html", 1000L),
                View("Mike", "/home.html", 2000L),
                View("Bob", "/car.html", 4000L),
                View("Alice", "/about.html", 5000L),
            )
        )

        // 使用 rescale 算子：局部轮询分发（相邻任务间）
        val rescaledStream = dataStream.rescale

        rescaledStream.map(view => s"Rescaled: ${view.user} -> ${view.url}")
            .print()

        env.execute()
    }

}
