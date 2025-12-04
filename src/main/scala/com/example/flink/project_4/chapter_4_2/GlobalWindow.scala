package com.example.flink.project_4.chapter_4_2

import com.example.flink.project_4.common.View
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger

object GlobalWindow {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 简单数据
        val viewData = List(
            View("张三", "首页", 1),
            View("张三", "商品", 2),
            View("张三", "购物车", 3),
            View("张三", "搜索", 4),
            View("李四", "首页", 5),
            View("李四", "商品", 6)
        )

        val stream = env.fromCollection(viewData)

        // 全局窗口 + 计数触发器
        val result = stream
            .keyBy(_.userId)
            .window(GlobalWindows.create()) // 创建全局窗口
            .trigger(CountTrigger.of(3)) // 每3条数据触发一次
            .reduce((v1, v2) => View(v1.userId, s"合并${v2.url}", v1.timestamp + 1))

        // 输出
        result.map { view =>
            val userName = view.userId
            val mergeCount = view.timestamp.toInt
            (userName, s"合并${mergeCount}条")
        }.print()

        println("程序开始执行...")
        env.execute("全局窗口演示")
    }

}
