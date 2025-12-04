package com.example.flink.project_5

import com.example.flink.project_5.common.View
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object SimpleStatelessExample {

    def main(args: Array[String]): Unit = {
        // 创建执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        // 设置并行度为1，方便查看结果
        env.setParallelism(1)

        // 模拟数据源
        val viewData = List(
            View("user001", "https://example.com/home", 1625097600000L),
            View("user002", "https://example.com/products", 1625097601000L),
            View("user001", "https://example.com/about", 1625097602000L),
            View("user003", "https://example.com/home", 1625097603000L),
            View("user002", "https://example.com/contact", 1625097604000L),
            View("user004", "https://example.com/products", 1625097605000L),
            View("user001", "https://example.com/products", 1625097606000L),
            View("user005", "https://example.com/blog", 1625097607000L)
        )

        // 创建数据流
        val viewStream: DataStream[View] = env.fromCollection(viewData)

        viewStream.print("原始数据: ")

        // 简单的Map转换
        val simpleMapStream = viewStream
            .map(view =>
                s"[MAP] 用户 ${view.userId} 在时间 ${view.timestamp} 访问了 ${view.url}"
            )
        simpleMapStream.print("map转换后的数据: ")

        println("程序开始运行...")
        env.execute()

    }

}
