package com.example.flink.project_4

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

object WindowDemo {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val dataStream = env.fromCollection(List(
            ("Tom", 1000L),
            ("Mike", 2000L),
            ("Bob", 4000L),
            ("Mike", 5000L),
            ("Mike", 5500L),
            ("Bob", 6000L),
        ))

//        dataStream.assignTimestampsAndWatermarks()

        val res = dataStream.keyBy(_._1).countWindow(2).max(1)
        res.print()
        env.execute()
    }


}
