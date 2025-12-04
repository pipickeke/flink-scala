package com.example.flink.project_4.chapter_4_2

import com.example.flink.project_4.common.{UserWindowStats, View}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.util.Collector


object ProcessWindowFunctionDemo {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 创建带时间戳的数据流
        val viewStream: DataStream[View] = env.fromElements(
            View("user1", "https://example.com/page1", 1698765432000L),
            View("user2", "https://example.com/page2", 1698765433000L),
            View("user1", "https://example.com/page3", 1698765434000L),
            View("user3", "https://example.com/page1", 1698765435000L),
            View("user2", "https://example.com/page4", 1698765436000L)
        )
            .assignAscendingTimestamps(_.timestamp) // 分配时间戳

        val resultStream = viewStream
            .keyBy(_.userId)
            .window(TumblingEventTimeWindows.of(Time.seconds(10))) // 基于事件时间的窗口
            .process(new ViewAnalysisProcessWindowFunction())

        resultStream.print()
        println("程序开始执行...")
        env.execute()
    }
}

class ViewAnalysisProcessWindowFunction
    extends ProcessWindowFunction[View, String, String, TimeWindow] {

    override def process(
                            key: String,
                            context: Context,
                            elements: Iterable[View],
                            out: Collector[String]): Unit = {

        // 统计访问的URL
        val urlCounts = elements.groupBy(_.url).mapValues(_.size)

        // 找到最常访问的页面
        val mostVisitedPage = urlCounts.maxBy(_._2)

        // 构建输出字符串
        val result = s"User: $key | Window: [${context.window.getStart}-${context.window.getEnd}] | " +
            s"Total views: ${elements.size} | Most visited: ${mostVisitedPage._1} (${mostVisitedPage._2} times)"

        out.collect(result)
    }
}



