package com.example.flink.project_4.task_2

import com.example.flink.project_4.common.{Order, OrderFrequency, OrderSource}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Duration

object OrderFrequencyAnalysis {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 1. 创建订单数据流
        val orderStream = env.addSource(new OrderSource)

        // 2. 分配时间戳和水印
        val timedStream = orderStream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner(new SerializableTimestampAssigner[Order] {
                        override def extractTimestamp(element: Order, recordTimestamp: Long): Long = {
                            element.timestamp
                        }
                    })
            )

        // 3. 滑动窗口分析订单频率
        val frequencyStream = timedStream
            .keyBy(_.userId) // 按用户ID分组
            .window(SlidingEventTimeWindows.of(Time.minutes(3), Time.minutes(1))) // 3分钟窗口，1分钟滑动
            .apply(new OrderFrequencyWindowFunction)

        // 4. 打印结果
        frequencyStream.print("Order Frequency")

        println("程序开始运行...")
        env.execute()
    }

}

// 自定义聚合函数计算订单数量
class OrderFrequencyWindowFunction
    extends WindowFunction[Order, OrderFrequency, String, TimeWindow] {

    override def apply(
                          userId: String,
                          window: TimeWindow,
                          orders: Iterable[Order],
                          out: Collector[OrderFrequency]
                      ): Unit = {
        out.collect(OrderFrequency(
            userId = userId,
            windowStart = window.getStart,
            windowEnd = window.getEnd,
            orderCount = orders.size
        ))
    }
}