package com.example.flink.project_3

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object RebalanceDemo {
    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val dataStream: DataStream[View] = env.fromCollection(
            List(
                View("Tom", "/index.html", 1000L),
                View("Mike", "/home.html", 2000L),
                View("Bob", "/car.html", 4000L),
                View("Alice", "/about.html", 5000L),
                View("Eve", "/contact.html", 6000L),
            )
        )

        // 使用 rebalance 算子：均匀轮询分发
        val rebalancedStream = dataStream.rebalance
        rebalancedStream.map(view => s"Rebalanced: ${view.user} -> ${view.url}")
            .print()
        env.execute()
    }
}
