package com.example.flink.project_4.task_3

import com.example.flink.project_4.common.{EnhancedOrderSource, LateOrder, NormalResult, Order}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, TimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat
import java.time.Duration

object LateDataProcessingWithSideOutput {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 1. 创建数据流
        val orderStream = env.addSource(new EnhancedOrderSource)

        // 2. 配置水位线和时间戳（重点：设置较短的乱序容忍时间）
        val watermarkedStream = orderStream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .forBoundedOutOfOrderness(Duration.ofSeconds(5)) // 只允许5秒乱序
                    .withTimestampAssigner(new SerializableTimestampAssigner[Order] {
                        override def extractTimestamp(element: Order, recordTimestamp: Long): Long = {
                            element.timestamp
                        }
                    })
            )

        // 3. 定义迟到数据的侧输出标签
        val lateDataTag = new OutputTag[Order]("late-orders")

        // 4. 主处理流程：窗口计算 + 侧输出流
        val mainResult = watermarkedStream
            .keyBy(_.userId)
            .window(TumblingEventTimeWindows.of(Time.seconds(10))) // 10秒滚动窗口
            .allowedLateness(Time.seconds(2)) // 额外允许2秒迟到
            .sideOutputLateData(lateDataTag) // 收集超出容忍范围的迟到数据
            .aggregate(
                new OrderAmountAggregator,
                new WindowResultFunction
            )

        // 5. 获取迟到数据
        val lateOrders = mainResult.getSideOutput(lateDataTag)

        // 7. 输出结果
        mainResult.print("正常结果")
        lateOrders.print("迟到数据")

        // 统计正常处理的订单
        mainResult
            .map(result => s"正常处理: 用户${result.userId}, 窗口[${new SimpleDateFormat("HH:mm:ss").format(result.windowStart)}], " +
                f"金额${result.totalAmount}%.2f, ${result.orderCount}笔订单")
            .print("统计-正常")

        // 统计迟到订单
        lateOrders
            .map(late => s"迟到订单: ${late.orderId}")
            .print("统计-迟到")

        println("程序开始运行...")
        env.execute()

    }

}


// 聚合函数：计算订单总金额和数量
class OrderAmountAggregator extends AggregateFunction[Order, (Double, Int), (Double, Int)] {
    override def createAccumulator(): (Double, Int) = (0.0, 0)

    override def add(value: Order, accumulator: (Double, Int)): (Double, Int) = {
        (accumulator._1 + value.amount, accumulator._2 + 1)
    }

    override def getResult(accumulator: (Double, Int)): (Double, Int) = accumulator

    override def merge(a: (Double, Int), b: (Double, Int)): (Double, Int) = {
        (a._1 + b._1, a._2 + b._2)
    }
}

// 窗口函数：生成最终结果
class WindowResultFunction
    extends ProcessWindowFunction[(Double, Int), NormalResult, String, TimeWindow] {

    override def process(
                            key: String,
                            context: Context,
                            elements: Iterable[(Double, Int)],
                            out: Collector[NormalResult]
                        ): Unit = {

        val (totalAmount, orderCount) = elements.head
        val window = context.window

        out.collect(NormalResult(
            windowStart = window.getStart,
            windowEnd = window.getEnd,
            userId = key,
            totalAmount = totalAmount,
            orderCount = orderCount
        ))
    }
}