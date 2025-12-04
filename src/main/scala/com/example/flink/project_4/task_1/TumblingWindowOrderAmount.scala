package com.example.flink.project_4.task_1

import com.example.flink.project_4.common.{Order, OrderSource}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.time.Duration

object TumblingWindowOrderAmount {

    def main(args: Array[String]): Unit = {
        // 创建执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        // 设置并行度为1，方便查看结果
        env.setParallelism(1)

        // 使用事件时间滚动窗口
        eventTimeTumblingWindow(env)

        // 执行任务
        println("程序开始运行...")
        env.execute()
    }

    /**
     * 使用事件时间滚动窗口统计订单金额（推荐）
     */
    def eventTimeTumblingWindow(env: StreamExecutionEnvironment): Unit = {

        // 创建带时间戳和水印的数据流
        val orderWithTimestampStream = env.addSource(new OrderSource)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2)) // 允许2秒乱序
                    .withTimestampAssigner(new SerializableTimestampAssigner[Order] {
                        override def extractTimestamp(element: Order, recordTimestamp: Long): Long = {
                            element.timestamp
                        }
                    })
            )

        import org.apache.flink.streaming.api.scala.function.WindowFunction
        import org.apache.flink.streaming.api.windowing.windows.TimeWindow

        // 全局统计所有用户的订单总金额
        val globalAmountStream = orderWithTimestampStream
            .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10))) // 10秒滚动窗口
            .apply(new AllWindowFunction[Order, (Long, Double, Int), TimeWindow] {
                override def apply(
                                      window: TimeWindow,
                                      input: Iterable[Order],
                                      out: Collector[(Long, Double, Int)]
                                  ): Unit = {
                    val count = input.size
                    val totalAmount = input.map(_.amount).sum
                    val firstTimestamp = input.head.timestamp

                    out.collect((firstTimestamp, totalAmount, count))
                }
            })

        // 按标签分组统计
        val labelAmountStream = orderWithTimestampStream
            .keyBy(_.label) // 按商品标签分组
            .window(TumblingProcessingTimeWindows.of(Time.seconds(8))) // 8秒滚动窗口
            .sum("amount") // 直接对金额字段求和
            .map(order => (order.label, order.amount, order.timestamp))

        // 按用户分组统计，并计算平均订单金额
        val userAvgAmountStream = orderWithTimestampStream
            .keyBy(_.userId)
            .window(TumblingProcessingTimeWindows.of(Time.seconds(6)))
            .apply(new WindowFunction[Order, (String, Double, Double, Int), String, TimeWindow] {
                override def apply(userId: String,
                                   window: TimeWindow,
                                   orders: Iterable[Order],
                                   out: org.apache.flink.util.Collector[(String, Double, Double, Int)]): Unit = {

                    val orderList = orders.toList
                    val totalAmount = orderList.map(_.amount).sum
                    val avgAmount = if (orderList.nonEmpty) totalAmount / orderList.size else 0.0
                    val orderCount = orderList.size

                    out.collect((userId, totalAmount, avgAmount, orderCount))
                }
            })

        // 打印各种统计结果
        globalAmountStream.print("Global-Amount-Result: ")
        labelAmountStream.print("Label-Amount-Result: ")
        userAvgAmountStream.print("User-AvgAmount-Result: ")
    }
}



