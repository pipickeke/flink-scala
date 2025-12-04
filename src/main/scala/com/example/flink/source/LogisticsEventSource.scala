package com.example.flink.source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import scala.util.Random
import java.util.concurrent.TimeUnit
import com.example.flink.model._

class LogisticsEventSource extends SourceFunction[LogisticsEvent] {
    private var isRunning = true
    private val random = new Random()

    // 模拟的城市列表
    private val cities = List(
        "北京", "上海", "广州", "深圳", "成都",
        "杭州", "武汉", "西安", "南京", "重庆"
    )

    // 模拟的货物类型
    private val goodsTypes = List(
        "电子产品", "服装", "食品", "家具", "书籍",
        "药品", "化妆品", "玩具", "体育用品", "日用品"
    )

    // 模拟的运输状态
    private val statuses = List(
        "已揽收", "运输中", "到达分拣中心", "派送中", "已签收"
    )

    override def run(ctx: SourceContext[LogisticsEvent]): Unit = {
        var orderId = 10000

        while (isRunning) {
            // 随机生成物流事件
            val eventType = random.nextInt(3)

            val now = System.currentTimeMillis()
            val event = eventType match {
                case 0 =>
                    // 生成订单创建事件
                    OrderCreated(
                        s"ORD${orderId}",
                        s"CUST${1000 + random.nextInt(9000)}",
                        cities(random.nextInt(cities.length)),
                        cities(random.nextInt(cities.length)),
                        goodsTypes(random.nextInt(goodsTypes.length)),
                        1 + random.nextInt(100),
                        now - random.nextInt(3600000) // 1小时内的时间
                    )

                case 1 =>
                    // 生成状态更新事件
                    StatusUpdated(
                        s"ORD${10000 + random.nextInt(orderId - 10000 + 1)}",
                        statuses(random.nextInt(statuses.length)),
                        cities(random.nextInt(cities.length)),
                        now - random.nextInt(3600000) // 1小时内的时间
                    )

                case 2 =>
                    // 生成订单完成事件
                    OrderCompleted(
                        s"ORD${10000 + random.nextInt(orderId - 10000 + 1)}",
                        now - random.nextInt(3600000) // 1小时内的时间
                    )
            }

            ctx.collect(event)

            // 每100毫秒生成一个事件
            TimeUnit.MILLISECONDS.sleep(100)

            // 每生成100个事件增加一个订单ID
            if (random.nextInt(100) == 0) {
                orderId += 1
            }
        }
    }

    override def cancel(): Unit = {
        isRunning = false
    }
}