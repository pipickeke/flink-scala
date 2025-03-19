package com.example.flink.project_3

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}

object SourceDemo {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val dataStream: DataStream[View] = env.fromCollection(List(
            View("Tom", "/index.html", 1000L),
            View("Mike", "/home.html", 2000L),
            View("Bob", "/car.html", 4000L),
        ))

        dataStream.print()
        env.execute()
    }
}
