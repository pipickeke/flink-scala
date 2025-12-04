package com.example.flink.project_6
import com.example.flink.common.Order
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.pattern.conditions.SimpleCondition
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{Duration, LocalDateTime, format}
import java.util
import scala.collection.JavaConverters.{asScalaBufferConverter, mapAsScalaMapConverter}

object FrequentSmallOrdersDetection {
    // 定义告警样例类
    case class OrderAlert(userId: String, orderCount: Int, totalAmount: Double, firstOrderTime: String, lastOrderTime: String)

    // 解决方案1: 使用 transient lazy val 定义不可序列化的格式化器
    @transient private lazy val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    def main(args: Array[String]): Unit = {
        // 1. 创建流执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 使用事件时间
        env.getConfig.setAutoWatermarkInterval(1000L)

        // 2. 创建模拟订单数据流
        val orders = env.fromElements(
            // 用户1 - 正常订单
            Order("order1", "user1", 150.0, "2023-01-01 10:00:00"),
            Order("order2", "user1", 200.0, "2023-01-01 10:01:00"),

            // 用户2 - 高频小额订单 (应触发告警)
            Order("order3", "user2", 50.0, "2023-01-01 10:02:00"),
            Order("order4", "user2", 80.0, "2023-01-01 10:03:00"),
            Order("order5", "user2", 90.0, "2023-01-01 10:04:00"),
            Order("order6", "user2", 70.0, "2023-01-01 10:05:00"),  // 第4笔，也应包含在告警中

            // 用户3 - 小额但不够高频
            Order("order7", "user3", 60.0, "2023-01-01 10:06:00"),
            Order("order8", "user3", 70.0, "2023-01-01 10:07:00"),

            // 用户4 - 高频但金额不小
            Order("order9", "user4", 50.0, "2023-01-01 10:08:00"),
            Order("order10", "user4", 120.0, "2023-01-01 10:09:00"),  // 金额大于100
            Order("order11", "user4", 80.0, "2023-01-01 10:10:00")
        )
            // 转换时间戳为毫秒并分配水位线
            .map(order => {
                val timeMillis = LocalDateTime.parse(order.timestamp, formatter)
                    .atZone(java.time.ZoneId.systemDefault())
                    .toInstant
                    .toEpochMilli
                (order.orderId, order.userId, order.amount, order.timestamp, timeMillis)
            })
            .map(t => Order(t._1, t._2, t._3, t._4))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner(new SerializableTimestampAssigner[Order] {
                        override def extractTimestamp(element: Order, recordTimestamp: Long): Long = {
                            LocalDateTime.parse(element.timestamp, formatter)
                                .atZone(java.time.ZoneId.systemDefault())
                                .toInstant
                                .toEpochMilli
                        }
                    })
            )

        // 3. 定义CEP模式：5分钟内超过3笔小额订单
        val smallOrderPattern = Pattern
            .begin[Order]("first")  // 第一笔订单
            .where(new SimpleCondition[Order] {
                override def filter(order: Order): Boolean = order.amount < 100
            })
            .next("second")  // 第二笔订单
            .where(new SimpleCondition[Order] {
                override def filter(order: Order): Boolean = order.amount < 100
            })
            .next("third")  // 第三笔订单
            .where(new SimpleCondition[Order] {
                override def filter(order: Order): Boolean = order.amount < 100
            })
            // 允许更多小额订单继续匹配
            .oneOrMore
            .consecutive()  // 要求连续匹配
            .within(Time.minutes(5))  // 5分钟内

        // 4. 将模式应用到数据流上，按用户ID分组
        val patternStream: PatternStream[Order] = CEP.pattern(
            orders.keyBy(_.userId),
            smallOrderPattern
        )

        // 5. 处理匹配事件并输出告警
        val alerts = patternStream.select(
            (pattern: util.Map[String, util.List[Order]]) => {
                // 获取所有匹配的订单
                val firstOrder = pattern.get("first").get(0)
                val secondOrder = pattern.get("second").get(0)
                val thirdOrder = pattern.get("third").get(0)

                // 获取后续订单（如果有）
                val additionalOrders = if (pattern.size() > 3) {
                    pattern.asScala
                        .filterKeys(k => !Set("first", "second", "third").contains(k))
                        .values
                        .flatMap(_.asScala)
                        .toList
                } else {
                    Nil
                }

                val allOrders = List(firstOrder, secondOrder, thirdOrder) ++ additionalOrders
                val totalAmount = allOrders.map(_.amount).sum

                OrderAlert(
                    firstOrder.userId,
                    allOrders.size,
                    totalAmount,
                    allOrders.head.timestamp,
                    allOrders.last.timestamp
                )
            }
        )

        // 6. 打印结果
        alerts.print("High Frequency Small Orders Alert")

        // 7. 执行程序
        env.execute("Frequent Small Orders Detection with CEP")
    }
}