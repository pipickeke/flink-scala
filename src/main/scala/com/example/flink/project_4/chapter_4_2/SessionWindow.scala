package com.example.flink.project_4.chapter_4_2

import com.example.flink.project_4.common.View
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time

object SessionWindow {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 数据 - 时间戳间隔展示会话分割
        val viewData = List(
            View("张三", "首页", 1000),
            View("张三", "商品", 2000),
            View("张三", "购物车", 3000),
            View("张三", "搜索", 8000), // 间隔5秒，新会话开始
            View("张三", "订单", 9000),
            View("李四", "首页", 12000),
            View("李四", "商品", 13000)
        )

        val stream = env.fromCollection(viewData)

        // 分配事件时间
        val timedStream = stream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[View] {
            var currentMaxTimestamp: Long = 0L

            override def extractTimestamp(element: View, previousElementTimestamp: Long): Long = {
                val ts = element.timestamp
                if (ts > currentMaxTimestamp) currentMaxTimestamp = ts
                ts
            }

            override def getCurrentWatermark(): Watermark = {
                new Watermark(currentMaxTimestamp - 500L)
            }
        })

        // 使用 EventTimeSessionWindows
        val result = timedStream
            .keyBy(_.userId)
            .window(EventTimeSessionWindows.withGap(Time.seconds(5))) // 5秒会话间隙
            .reduce((v1, v2) => View(v1.userId, v2.url, v1.timestamp + 1))

        // 输出
        result.map { view =>
            val userName = view.userId
            (userName, s"最后页面:${view.url}")
        }.print()

        println("程序开始执行...")
        env.execute()
    }
}
