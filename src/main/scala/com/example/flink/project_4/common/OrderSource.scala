package com.example.flink.project_4.common

import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

class OrderSource extends RichSourceFunction[Order]  {

    private var running = true

    override def run(ctx: SourceContext[Order]): Unit = {
        val labels = List("electronics", "clothing", "food", "books", "home")
        val users = List("user1", "user2", "user3", "user4", "user5", "user6")

        var orderId = 100
        while (running) {
            val order = Order(
                orderId.toString,
                users(scala.util.Random.nextInt(users.length)),
                scala.util.Random.nextDouble() * 500 + 50, // 50-550之间的随机金额
                labels(scala.util.Random.nextInt(labels.length)),
                System.currentTimeMillis()
            )

            ctx.collect(order)
            orderId += 1

            Thread.sleep(1000) // 每秒产生一个订单
        }
    }

    override def cancel(): Unit = {
        running = false
    }
}
