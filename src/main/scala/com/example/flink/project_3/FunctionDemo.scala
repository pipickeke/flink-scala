package com.example.flink.project_3

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}


// 自定义 MapFunction 类
class ViewToUserUrlMapFunction extends MapFunction[View, (String, String)] {
    override def map(view: View): (String, String) = {
        (view.user, view.url)
    }
}

object FunctionDemo {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val dataStream: DataStream[View] = env.fromCollection(
            List(
                View("Tom", "/index.html", 1000L),
                View("Mike", "/home.html", 2000L),
                View("Bob", "/car.html", 4000L),
            )
        )

        // 使用自定义的 Function Class 进行 map 操作
        val mappedStream = dataStream.map(new ViewToUserUrlMapFunction())
        mappedStream.print()
        env.execute()
    }

}
