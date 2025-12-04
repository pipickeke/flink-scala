package com.example.flink.project_6

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.pattern.conditions.SimpleCondition
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.time.Duration
import java.util

object OrderStateMachineCEP {
    // 定义订单事件样例类
    case class OrderEvent(orderId: String, state: String, eventTime: Long)

    // 定义异常告警样例类
    case class OrderAlert(orderId: String, currentState: String, expectedStates: String, message: String)

    def main(args: Array[String]): Unit = {
        // 1. 创建流执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 使用事件时间
        env.getConfig.setAutoWatermarkInterval(1000L)

        // 2. 创建模拟订单事件流
        val orderEvents = env.fromElements(
            // 正常流程
            OrderEvent("order_1", "CREATED", 1000L),
            OrderEvent("order_1", "PAID", 2000L),
            OrderEvent("order_1", "SHIPPED", 3000L),
            OrderEvent("order_1", "COMPLETED", 4000L),

            // 异常流程1: 跳过PAID状态
            OrderEvent("order_2", "CREATED", 5000L),
            OrderEvent("order_2", "SHIPPED", 6000L),  // 应触发告警

            // 异常流程2: 状态回退
            OrderEvent("order_3", "CREATED", 7000L),
            OrderEvent("order_3", "PAID", 8000L),
            OrderEvent("order_3", "CREATED", 9000L),  // 应触发告警

            // 异常流程3: 超时未完成
            OrderEvent("order_4", "CREATED", 10000L),
            OrderEvent("order_4", "PAID", 11000L)
            // 缺少SHIPPED和COMPLETED状态
        )
            // 分配时间戳和水位线
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner(new SerializableTimestampAssigner[OrderEvent] {
                        override def extractTimestamp(element: OrderEvent, recordTimestamp: Long): Long = element.eventTime
                    })
            )

        // 3. 定义正常的状态流转模式
        val normalOrderPattern = Pattern
            .begin[OrderEvent]("created").where(_.state == "CREATED")
            .next("paid").where(_.state == "PAID")
            .next("shipped").where(_.state == "SHIPPED")
            .next("completed").where(_.state == "COMPLETED")
            .within(Time.hours(24))  // 24小时内应完成整个流程

        // 4. 定义异常模式1: 跳过PAID状态
        val skipPaidPattern = Pattern
            .begin[OrderEvent]("created").where(_.state == "CREATED")
            .next("shipped").where(_.state == "SHIPPED")

        // 5. 定义异常模式2: 状态回退
        val stateRollbackPattern = Pattern
            .begin[OrderEvent]("paid").where(_.state == "PAID")
            .next("created").where(_.state == "CREATED")

        // 6. 将模式应用到数据流上，按订单ID分组
        val keyedStream = orderEvents.keyBy(_.orderId)

        // 正常流程检测
        val normalPatternStream = CEP.pattern(keyedStream, normalOrderPattern)

        // 异常流程检测
        val skipPaidPatternStream = CEP.pattern(keyedStream, skipPaidPattern)
        val rollbackPatternStream = CEP.pattern(keyedStream, stateRollbackPattern)

        // 7. 处理匹配结果
        // 7.1 检测未完成的订单
        val timeoutOrders = normalPatternStream.flatSelect(
            (pattern: util.Map[String, util.List[OrderEvent]], out: Collector[OrderAlert]) => {
                val createdOpt = Option(pattern.get("created"))
                val paidOpt = Option(pattern.get("paid"))
                val shippedOpt = Option(pattern.get("shipped"))
                val completedOpt = Option(pattern.get("completed"))

                (createdOpt, paidOpt, shippedOpt, completedOpt) match {
                    case (Some(created), Some(paid), Some(shipped), None) =>
                        out.collect(OrderAlert(
                            shipped.get(0).orderId,
                            "SHIPPED",
                            "COMPLETED",
                            "Order shipped but not completed within 24 hours"
                        ))
                    case (Some(created), Some(paid), None, None) =>
                        out.collect(OrderAlert(
                            paid.get(0).orderId,
                            "PAID",
                            "SHIPPED → COMPLETED",
                            "Order paid but not shipped within 24 hours"
                        ))
                    case (Some(created), None, None, None) =>
                        out.collect(OrderAlert(
                            created.get(0).orderId,
                            "CREATED",
                            "PAID → SHIPPED → COMPLETED",
                            "Order created but not paid within 24 hours"
                        ))
                    case _ => // 正常完成，不处理
                }
            }
        )

        // 7.2 检测跳过PAID状态的订单
        val skipPaidAlerts = skipPaidPatternStream.select(
            (pattern: util.Map[String, util.List[OrderEvent]]) => {
                val created = pattern.get("created").get(0)
                val shipped = pattern.get("shipped").get(0)
                OrderAlert(
                    shipped.orderId,
                    shipped.state,
                    "PAID",
                    "Order skipped PAID state, went directly from CREATED to SHIPPED"
                )
            }
        )

        // 7.3 检测状态回退的订单
        val rollbackAlerts = rollbackPatternStream.select(
            (pattern: util.Map[String, util.List[OrderEvent]]) => {
                val paid = pattern.get("paid").get(0)
                val created = pattern.get("created").get(0)
                OrderAlert(
                    created.orderId,
                    created.state,
                    "SHIPPED or COMPLETED",
                    s"Order state rolled back from PAID to CREATED"
                )
            }
        )

        // 8. 合并所有告警并输出
        val allAlerts = timeoutOrders.union(skipPaidAlerts, rollbackAlerts)
        allAlerts.print("Order State Alert")

        // 9. 执行程序
        env.execute("Order State Machine Monitoring with CEP")
    }
}
