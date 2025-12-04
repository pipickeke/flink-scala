package com.example.flink.project_4.chapter_4_2

import com.example.flink.project_4.common.View
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

object SlideTimeWindowDemo {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 数据保持不变
        val viewData = List(
            View("张三", "首页.html", 1000),
            View("李四", "商品页.html", 1500),
            View("张三", "购物车.html", 2000),
            View("王五", "首页.html", 3500),
            View("李四", "订单页.html", 4000),
            View("张三", "搜索.html", 5500)
        )

        val stream = env.fromCollection(viewData)

        // 水位线逻辑保持不变
        val timedStream = stream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[View] {
            var currentMaxTimestamp: Long = 0L
            val maxOutOfOrderness: Long = 500L

            override def extractTimestamp(element: View, previousElementTimestamp: Long): Long = {
                val ts = element.timestamp
                if (ts > currentMaxTimestamp) currentMaxTimestamp = ts
                ts
            }

            override def getCurrentWatermark(): Watermark = {
                new Watermark(currentMaxTimestamp - maxOutOfOrderness)
            }
        })

        // 滑动窗口
        val result = timedStream
            .keyBy(_.userId)
            .timeWindow(Time.seconds(4), Time.seconds(2)) // 窗口4秒，滑动2秒
            .reduce((v1, v2) => View(v1.userId, v2.url, v1.timestamp + 1))

        // 简化输出
        result.map { view =>
            val windowStart = (view.timestamp / 2000) * 2000 // 滑动步长是2秒
            val windowEnd = windowStart + 4000 // 窗口大小是4秒
            (view.userId, s"窗口[$windowStart-${windowEnd}ms] 进行访问")
        }.print()

        println("程序开始执行...")
        env.execute()
    }

}
