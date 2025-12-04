package com.example.flink.project_3

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object BroadcastDemo {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val dataStream: DataStream[View] = env.fromCollection(
            List(
                View("Tom", "/index.html", 1000L),
                View("Mike", "/home.html", 2000L),
                View("Bob", "/car.html", 4000L),
            )
        )

        // 使用 broadcast 算子：将每条数据广播到下游所有并行任务
        val broadcastedStream = dataStream.broadcast

        broadcastedStream.map(view => s"Broadcasted: ${view.user} -> ${view.url} (sent to ALL)")
            .print()

        env.execute()
    }

}
