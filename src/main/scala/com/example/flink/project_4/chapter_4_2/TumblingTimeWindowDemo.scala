package com.example.flink.project_4.chapter_4_2

import com.example.flink.project_4.common.View
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

object TumblingTimeWindowDemo {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 数据 - 时间戳用于事件时间
        val viewData = List(
            View("张三", "首页.html", 1000),
            View("李四", "商品页.html", 1500),
            View("张三", "购物车.html", 2000),
            View("王五", "首页.html", 3500), // 进入下一个窗口
            View("李四", "订单页.html", 4000), // 进入下一个窗口
            View("张三", "搜索.html", 5500) // 进入下一个窗口
        )

        val stream = env.fromCollection(viewData)

        // 分配事件时间（关键步骤！）
        val timedStream = stream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[View] {
            var currentMaxTimestamp: Long = 0L
            val maxOutOfOrderness: Long = 500L

            override def extractTimestamp(element: View, previousElementTimestamp: Long): Long = {
                val ts = element.timestamp
                if (ts > currentMaxTimestamp) currentMaxTimestamp = ts
                ts // 直接返回数据中的timestamp作为事件时间
            }

            override def getCurrentWatermark(): Watermark = {
                new Watermark(currentMaxTimestamp - maxOutOfOrderness)
            }
        })

        // 简单统计
        val result = timedStream
            .keyBy(_.userId)
            .timeWindow(Time.seconds(3))
            .reduce((v1, v2) => View(v1.userId, v2.url, v1.timestamp + 1))

        // 输出时显示窗口信息
        result.map { view =>
            // 根据时间戳推算窗口
            val windowStart = (view.timestamp / 3000) * 3000
            val windowEnd = windowStart + 3000
            (view.userId, s"窗口[$windowStart-${windowEnd}ms]进行访问")
        }.print()

        println("程序开始执行...")
        env.execute()
    }
}
