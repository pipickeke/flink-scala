package com.example.flink.project_3

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}


object FilterDemo {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val dataStream: DataStream[View] = env.fromCollection(List(
            View("Tom", "/index.html", 1000L),
            View("Mike", "/home.html", 2000L),
            View("Bob", "/car.html", 4000L),
        ))

        val filterdDataStream = dataStream.filter(_.url == "/index.html")
        filterdDataStream.print()
        env.execute()
    }
}



