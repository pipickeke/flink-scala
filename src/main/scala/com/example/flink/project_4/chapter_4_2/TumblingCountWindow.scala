package com.example.flink.project_4.chapter_4_2

import com.example.flink.project_4.common.View
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

object TumblingCountWindow {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1) // 保持并行度为1，输出不乱序

        // 数据
        val viewData = List(
            View("张三", "首页.html", 1000),
            View("李四", "商品页.html", 1500),
            View("张三", "购物车.html", 2000),
            View("王五", "首页.html", 3500),
            View("李四", "订单页.html", 4000),
            View("张三", "搜索.html", 5500),
            View("赵六", "分类.html", 6000), // 额外的数据
            View("王五", "详情.html", 7000) // 额外的数据
        )

        val stream = env.fromCollection(viewData)

        // 每3条数据形成一个窗口
        val result = stream
            .keyBy(_.userId)
            .countWindow(3) //滚动计数窗口：每3条数据一个窗口
            .reduce((v1, v2) => View(v1.userId, v2.url, v1.timestamp + 1))

        // 输出时显示窗口信息（基于数据条数）
        result.map { view =>
            // 计数窗口无法直接知道是第几个窗口，我们通过数据来推断
            println(s"用户${view.userId}: 累计访问3次")
        }

        println("程序开始执行...")
        env.execute()
    }
}
