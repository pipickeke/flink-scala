package com.example.flink.project_4.common

import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import java.text.SimpleDateFormat

class EnhancedOrderSource extends RichSourceFunction[Order]{

    private var isRunning = true
    private val dateFormat = new SimpleDateFormat("HH:mm:ss.SSS")
    private val windowSize = 10000L // 10秒窗口

    override def run(ctx: SourceContext[Order]): Unit = {
        val users = Seq("user1", "user2", "user3", "user4")
        val labels = Seq("electronics", "clothing", "food", "books")
        val rand = new scala.util.Random

        var orderCounter = 0

        // 创建不同时间段的订单，模拟乱序
        val baseTime = System.currentTimeMillis()

        while (isRunning && orderCounter < 20) {
            try {
                val userId = users(rand.nextInt(users.length))
                val amount = 50 + rand.nextDouble() * 200
                val label = labels(rand.nextInt(labels.length))

                // 制造不同程度的乱序和迟到
                val delayLevel = rand.nextInt(5) match {
                    case 0 => -5000L // 提前5秒（未来数据）
                    case 1 => -2000L // 提前2秒
                    case 2 => 0L // 准时
                    case 3 => 8000L // 迟到8秒（在容忍范围内）
                    case 4 => 15000L // 迟到15秒（超出容忍范围，将成为迟到数据）
                }

                val eventTime = baseTime - delayLevel
                val processingTime = System.currentTimeMillis()

                val order = Order(
                    orderId = f"order_${orderCounter}%03d",
                    userId = userId,
                    amount = amount,
                    label = label,
                    timestamp = eventTime
                )

                ctx.collect(order)

                // 输出生成信息
                val delayDesc = delayLevel match {
                    case d if d < 0 => s"提前 ${-d}ms"
                    case d if d == 0 => "准时"
                    case d if d <= 10000 => s"轻微迟到 ${d}ms"
                    case _ => s"严重迟到"
                }

//                println(s"[生成] ${dateFormat.format(eventTime)} | ${order.orderId} | ${userId} | $${amount}%.2f | $delayDesc")

                orderCounter += 1
                Thread.sleep(400 + rand.nextInt(600))

            } catch {
                case e: InterruptedException => isRunning = false
            }
        }
    }

    override def cancel(): Unit = isRunning = false

}
