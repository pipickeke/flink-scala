package com.example.flink.project_4.chapter_4_1

import com.example.flink.project_4.common.View
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.watermark.Watermark

object WatermarkExample {

    def main(args: Array[String]): Unit = {
        // 1. 创建Flink环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        // 2. 准备测试数据
        val viewData = List(
            View("张三", "首页.html", 1000), // 时间点1
            View("李四", "商品页.html", 500), // 时间点0.5（比上一条早！）
            View("张三", "购物车.html", 2000), // 时间点2
            View("王五", "首页.html", 1500), // 时间点1.5（乱序）
            View("李四", "订单页.html", 3000) // 时间点3
        )

        val stream = env.fromCollection(viewData)

        // 3. 分配时间戳和水位线
        val withTimestampsAndWatermarks = stream.assignTimestampsAndWatermarks(
            new MyAssigner()
        )

        // 4. 简单输出带时间戳的数据，看看水位线怎么工作
        withTimestampsAndWatermarks
            .map(view => {
                println(s"收到数据: ${view.userId} 访问 ${view.url}, 时间戳: ${view.timestamp}")
                view
            })
            .print()

        println("程序开始运行...")
        env.execute()
    }
}

/**
 * 作用：告诉Flink如何提取时间戳，以及如何生成水位线
 */
class MyAssigner extends AssignerWithPeriodicWatermarks[View] {

    // 当前最大时间戳
    var currentMaxTimestamp: Long = 0L
    // 最大延迟时间（2秒）
    val maxOutOfOrderness: Long = 2000L

    /**
     * 提取时间戳：告诉Flink用数据的哪个字段作为事件时间
     */
    override def extractTimestamp(element: View, previousElementTimestamp: Long): Long = {
        val timestamp = element.timestamp
        // 更新最大时间戳
        if (timestamp > currentMaxTimestamp) {
            currentMaxTimestamp = timestamp
        }
        println(s"  提取时间戳: ${element.userId} -> ${timestamp}, 当前最大时间戳: $currentMaxTimestamp")
        timestamp
    }

    /**
     * 生成水位线：告诉Flink现在处理到哪个时间点了
     */
    override def getCurrentWatermark(): Watermark = {
        // 水位线 = 当前最大时间戳 - 允许的最大延迟
        val watermark = currentMaxTimestamp - maxOutOfOrderness
        println(s"生成水位线: $watermark (基于最大时间戳: $currentMaxTimestamp)")
        new Watermark(watermark)
    }
}
