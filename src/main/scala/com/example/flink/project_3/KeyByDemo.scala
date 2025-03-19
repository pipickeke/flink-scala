package com.example.flink.project_3

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object KeyByDemo {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val dataStream = env.fromCollection(List(
            View("Tom", "/index.html", 1000L),
            View("Mike", "/home.html", 2000L),
            View("Mike", "/home.html", 3000L),
            View("Bob", "/car.html", 4000L),
            View("Bob", "/car.html", 6000L),
        ))

        val result = dataStream.keyBy(item => item.user).sum(2)
        result.print()
        env.execute()
    }

}
