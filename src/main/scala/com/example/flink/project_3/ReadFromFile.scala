package com.example.flink.project_3

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object ReadFromFile {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val dataStream: DataStream[String] = env.readTextFile("data/views.csv")
        dataStream.print()
        env.execute()
    }
}
