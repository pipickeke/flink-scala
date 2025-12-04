package com.example.flink.project_3

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}


// 使用 RichMapFunction
class RichViewToUserUrlFunction extends RichMapFunction[View, (String, String)] {

    override def open(parameters: org.apache.flink.configuration.Configuration): Unit = {
        println("RichMapFunction opened.")
    }

    override def map(view: View): (String, String) = {
        (view.user, view.url)
    }

    override def close(): Unit = {
        println("RichMapFunction closed.")
    }
}

object RichFunctionDemo {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val dataStream: DataStream[View] = env.fromCollection(
            List(
                View("Tom", "/index.html", 1000L),
                View("Mike", "/home.html", 2000L),
                View("Bob", "/car.html", 4000L),
            )
        )
        val mappedStream = dataStream.map(new RichViewToUserUrlFunction())
        mappedStream.print()
        env.execute()
    }
}
