package com.example.flink.project_3

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object FlatMapDemo_2 {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val dataStream: DataStream[View] = env.fromCollection(
            List(
                View("Tom", "/index.html", 1000L),
                View("Mike", "/home.html", 2000L),
                View("Bob", "/car.html", 4000L),
            )
        )

        // ✅ 使用 flatMap：将每个 View 的 url 按 "/" 分割，展开成多个路径部分
        val flatMappedStream = dataStream.flatMap { view =>
            // 按 "/" 分割 URL，过滤掉空字符串（比如开头的 ""）
            view.url.split("/").filter(_.nonEmpty).map(part => (view.user, part))
        }

        // 打印展开后的 (user, pathPart) 元组
        flatMappedStream.print()

        env.execute("FlatMap Demo: Split URL into path parts")
    }
}
