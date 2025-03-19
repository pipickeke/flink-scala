package com.example.flink.project_3

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object StreamingFileSinkDemo {


    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val dataStream = env.fromCollection(List(
            ("Tom", 1000L),
            ("Mike", 2000L),
            ("Bob", 4000L),
            ("Mike", 5000L),
            ("Bob", 6000L),
        ))

        val sink = StreamingFileSink.forRowFormat(new Path("data/output"),
            new SimpleStringEncoder[String]("UTF-8")).build()

        dataStream.map(_.toString()).addSink(sink)
        env.execute()

    }

}
