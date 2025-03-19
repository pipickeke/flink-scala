package com.example.flink.project_3

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object ReduceDemo {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val dataStream = env.fromCollection(List(
            ("Tom", 1000L),
            ("Mike", 2000L),
            ("Bob", 4000L),
            ("Mike", 5000L),
            ("Bob", 6000L),
        ))

        val keyByStream = dataStream.keyBy(0)
        val res = keyByStream.reduce((viewdata, newviewdata) => {
            (viewdata._1, viewdata._2.min(viewdata._2))
        })
        res.print()

        env.execute()
    }

}
