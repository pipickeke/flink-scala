package com.example.flink.project_7

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}

import java.time.Duration
import java.sql.Timestamp
import com.example.flink.model._
import com.example.flink.source.LogisticsEventSource
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.util.Collector

import org.apache.flink.api.common.eventtime._
import org.apache.flink.streaming.api.TimeCharacteristic
object LogisticsProcessingJob {
    def main(args: Array[String]): Unit = {
        // 1. 创建执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1) // 生产环境应根据实际情况设置

        // 2. 配置状态后端 (生产环境应使用分布式后端如RocksDB)
        // env.setStateBackend(new RocksDBStateBackend("hdfs://namenode:8020/flink/checkpoints"))

        // 3. 启用检查点 (生产环境需要)
        // env.enableCheckpointing(60000) // 每60秒一次checkpoint

        // 4. 创建数据源 (这里使用模拟数据，生产环境可连接Kafka等)
        val logisticsEventStream = env
            .addSource(new LogisticsEventSource)
            .name("logistics-event-source")

        // 5. 分配时间戳和水位线
        val timedEvents = logisticsEventStream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .forBoundedOutOfOrderness[LogisticsEvent](Duration.ofSeconds(5))
                    .withTimestampAssigner(new SerializableTimestampAssigner[LogisticsEvent] {
                        override def extractTimestamp(element: LogisticsEvent, recordTimestamp: Long): Long = {
                            element.timestamp
                        }
                    })
            )
            .name("assign-watermark")

        // 6. 分流处理不同类型的事件
        val orderCreatedTag = OutputTag[OrderCreated]("created")
        val statusUpdatedTag = OutputTag[StatusUpdated]("updated")
        val orderCompletedTag = OutputTag[OrderCompleted]("completed")

        val processedStream = timedEvents
            .process(new ProcessFunction[LogisticsEvent, LogisticsEvent] {
                override def processElement(
                                               value: LogisticsEvent,
                                               ctx: ProcessFunction[LogisticsEvent, LogisticsEvent]#Context,
                                               out: Collector[LogisticsEvent]
                                           ): Unit = {
                    value match {
                        case created: OrderCreated =>
                            ctx.output(orderCreatedTag, created)
                        case updated: StatusUpdated =>
                            ctx.output(statusUpdatedTag, updated)
                        case completed: OrderCompleted =>
                            ctx.output(orderCompletedTag, completed)
                        case _ => // 其他事件处理
                    }
                    out.collect(value)
                }
            })

        // 7. 获取各事件流
        val orderCreatedStream = processedStream.getSideOutput(orderCreatedTag)
        val statusUpdatedStream = processedStream.getSideOutput(statusUpdatedTag)
        val orderCompletedStream = processedStream.getSideOutput(orderCompletedTag)

        // 8. 业务处理
        // 8.1 按目的地统计订单量
        processRegionOrderStats(orderCreatedStream)

        // 8.2 检测异常订单
        detectAbnormalOrders(statusUpdatedStream)

        // 9. 执行作业
        env.execute("Logistics Real-time Processing Job")
    }

    // 按地区统计订单量
    def processRegionOrderStats(stream: DataStream[OrderCreated]): Unit = {
        import org.apache.flink.streaming.api.scala._
        import org.apache.flink.api.common.functions.AggregateFunction

        // 每5分钟统计一次各目的地的订单量
        val regionStats = stream
            .keyBy(_.destination)
            .window(TumblingEventTimeWindows.of(Time.minutes(5)))
            .aggregate(new OrderStatsAggregator)
            .name("region-order-stats")

        // 输出结果 (生产环境可输出到Kafka、数据库等)
        regionStats.print().name("print-region-stats")
    }

    // 检测异常订单(超过12小时未更新的订单)
    def detectAbnormalOrders(stream: DataStream[StatusUpdated]): Unit = {
        import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
        import org.apache.flink.api.common.typeinfo.TypeInformation
        import org.apache.flink.configuration.Configuration
        import org.apache.flink.streaming.api.functions.KeyedProcessFunction
        import org.apache.flink.util.Collector

        val abnormalOrders = stream
            .keyBy(_.orderId)
            .process(new AbnormalOrderDetector)
            .name("abnormal-order-detector")

        // 输出异常订单预警 (生产环境可对接告警系统)
        abnormalOrders.print().name("print-abnormal-orders")
    }
}

// 订单统计聚合器
class OrderStatsAggregator extends AggregateFunction[OrderCreated, (Long, Long), RegionOrderStats] {
    override def createAccumulator(): (Long, Long) = (0L, 0L)

    override def add(value: OrderCreated, accumulator: (Long, Long)): (Long, Long) = {
        (accumulator._1 + 1, accumulator._2 + (if(value.weight > 50) 1 else 0))
    }

    override def getResult(accumulator: (Long, Long)): RegionOrderStats = {
        RegionOrderStats(
            "region", // 实际应从订单获取地区
            new Timestamp(System.currentTimeMillis() - 300000), // 窗口开始时间
            new Timestamp(System.currentTimeMillis()),          // 窗口结束时间
            accumulator._1,                                    // 总订单数
            accumulator._2,                                    // 大件订单数
            0                                                  // 完成订单数
        )
    }

    override def merge(a: (Long, Long), b: (Long, Long)): (Long, Long) = {
        (a._1 + b._1, a._2 + b._2)
    }
}

// 异常订单检测处理器
class AbnormalOrderDetector extends KeyedProcessFunction[String, StatusUpdated, AbnormalOrderAlert] {
    private var lastUpdateState: ValueState[Long] = _

    override def open(parameters: Configuration): Unit = {
        val lastUpdateDesc = new ValueStateDescriptor[Long]("last-update-time", classOf[Long])
        lastUpdateState = getRuntimeContext.getState(lastUpdateDesc)
    }

    override def processElement(
                                   event: StatusUpdated,
                                   ctx: KeyedProcessFunction[String, StatusUpdated, AbnormalOrderAlert]#Context,
                                   out: Collector[AbnormalOrderAlert]
                               ): Unit = {
        // 更新最后状态时间
        val lastUpdateTime = lastUpdateState.value()
        lastUpdateState.update(event.updateTime)

        // 注册定时器，12小时后触发检查
        ctx.timerService().registerEventTimeTimer(event.updateTime + 12 * 60 * 60 * 1000)
    }

    override def onTimer(
                            timestamp: Long,
                            ctx: KeyedProcessFunction[String, StatusUpdated, AbnormalOrderAlert]#OnTimerContext,
                            out: Collector[AbnormalOrderAlert]
                        ): Unit = {
        val lastUpdateTime = lastUpdateState.value()

        // 如果最后更新时间与定时器时间相差12小时，说明期间没有更新
        if (lastUpdateTime != null && (timestamp - lastUpdateTime) >= 12 * 60 * 60 * 1000) {
            out.collect(AbnormalOrderAlert(
                ctx.getCurrentKey,
                "UNKNOWN", // 实际应从状态获取最后状态
                "UNKNOWN", // 实际应从状态获取最后位置
                new Timestamp(lastUpdateTime),
                12
            ))
        }
    }
}
