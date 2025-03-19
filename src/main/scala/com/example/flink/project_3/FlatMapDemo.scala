package com.example.flink.project_3

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object FlatMapDemo {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val dataStream: DataStream[String] = env.fromCollection(List(
            "hello flink",
            "hello world",
            "hello java"
        ))

        val flatmapDataStream = dataStream.flatMap(item => item.split(" "))
        flatmapDataStream.print()
        env.execute()
    }

}
