package com.example.flink.project_4.chapter_4_2

import com.example.flink.project_4.common.View
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

object SlidingCountWindow {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 数据
        val viewData = List(
            View("张三", "首页", 1L),
            View("张三", "商品页", 2L),
            View("张三", "购物车", 3L), // 第3条 → 窗口1触发
            View("张三", "搜索", 4L), // 第4条 → 窗口2触发
            View("张三", "订单", 5L), // 第5条 → 窗口3触发
            View("张三", "支付", 6L) // 第6条 → 窗口4触发
        )

        val stream = env.fromCollection(viewData)
        var windowCounter = 0
        val result = stream
            .keyBy(_.userId)
            .countWindow(3, 2) // 窗口3条，滑动2条
            .reduce((v1, v2) => {
                windowCounter += 1
                // 直接显示窗口编号和包含的数据
                val dataRange = s"[第${v1.timestamp.toInt}-${v2.timestamp.toInt}条]"
                View(s"窗口$windowCounter", dataRange, v2.timestamp)
            })

        result.map { view =>
            (view.userId, s"包含${view.url}")
        }.print()

        println("程序开始执行...")
        env.execute()
    }

}
