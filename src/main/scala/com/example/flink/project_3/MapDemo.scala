package com.example.flink.project_3

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}


case class View(user: String, url: String, timestamp: Long)
object MapDemo {


    def main(args: Array[String]): Unit = {


        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val dataStream: DataStream[View] = env.fromCollection(
            List(
                View("Tom", "/index.html", 1000L),
                View("Mike", "/home.html", 2000L),
                View("Bob", "/car.html", 4000L),
            )
        )

        val mapDataStream = dataStream.map(view => (view.user, view.url))
        mapDataStream.print()
        env.execute()

    }
}
