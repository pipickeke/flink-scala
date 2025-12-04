package com.example.flink.project_8

import com.example.flink.model.{ItemViewCount, UserBehavior}
import com.example.flink.source.EcommerceDataGenerator
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessAllWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Duration

object EcommerceAnalysis {
    def main(args: Array[String]): Unit = {
        // 创建执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)


        // 分配时间戳和水位线
        val dataStream = env.addSource(new EcommerceDataGenerator)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.forBoundedOutOfOrderness[UserBehavior](Duration.ofSeconds(5))
                    .withTimestampAssigner(new SerializableTimestampAssigner[UserBehavior] {
                        override def extractTimestamp(element: UserBehavior, recordTimestamp: Long): Long = {
                            element.timestamp * 1000 // 转换为毫秒
                        }
                    })
            )

        // 1. 热门商品统计（保持不变）
        val hotItems = dataStream
            .filter(_.behavior == "pv")
            .keyBy(_.itemId)
            .timeWindow(Time.hours(1), Time.minutes(5))
            .aggregate(new CountAgg(), new WindowResult())
            .keyBy(_.windowEnd)
            .process(new TopNHotItems(3))

        // 2. 流量统计
        val pvUvStream = dataStream
            .filter(_.behavior == "pv")
            .windowAll(TumblingEventTimeWindows.of(Time.minutes(5)))
            .process(new PvUvProcessFunction())

        // 打印结果
        hotItems.print("Hot Items")
        pvUvStream.print("PV/UV Stats")

        env.execute("Ecommerce Analysis Job")
    }
}

// 自定义预聚合函数 - 计数
class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
}

// 自定义窗口函数 - 输出ItemViewCount
class WindowResult extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
    override def apply(key: Long,
                       window: TimeWindow,
                       input: Iterable[Long],
                       out: Collector[ItemViewCount]): Unit = {
        out.collect(ItemViewCount(key, window.getEnd, input.iterator.next()))
    }
}

// 自定义处理函数 - 求TopN热门商品
class TopNHotItems(topSize: Int)
    extends ProcessFunction[ItemViewCount, String] {

    override def processElement(value: ItemViewCount,
                                ctx: ProcessFunction[ItemViewCount, String]#Context,
                                out: Collector[String]): Unit = {
        // 这里简化处理，实际应用中应该收集多个窗口的数据再排序
        out.collect(s"窗口结束时间: ${new java.util.Date(value.windowEnd)}\n" +
            s"商品ID: ${value.itemId}, 点击量: ${value.count}\n" +
            "====================================")
    }
}


//使用ProcessFunction同时计算PV和UV
class PvUvProcessFunction extends ProcessAllWindowFunction[UserBehavior, (Long, Long), TimeWindow] {
    override def process(context: Context,
                         elements: Iterable[UserBehavior],
                         out: Collector[(Long, Long)]): Unit = {
        val pv = elements.size
        val uv = elements.map(_.userId).toSet.size
        out.collect((pv, uv))
    }
}