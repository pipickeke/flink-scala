package com.example.flink.project_4.chapter_4_2

import com.example.flink.project_4.common.{UserVisitSummary, View}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

object AggregateFunctionWithView {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 数据
        val viewData = List(
            View("张三", "首页.html", 1000), // 时间戳1000ms
            View("张三", "商品页.html", 2000), // 时间戳2000ms，间隔1000ms
            View("张三", "购物车.html", 3500), // 时间戳3500ms，间隔1500ms
            View("李四", "首页.html", 1200), // 李四的访问
            View("李四", "商品页.html", 2400) // 李四第二次访问
        )

        val stream = env.fromCollection(viewData)

        // 使用 AggregateFunction 进行增量聚合
        val aggregateResult = stream
            .keyBy(_.userId)
            .countWindow(2) // 每2条数据聚合一次
            .aggregate(new VisitAggregateFunction())

        aggregateResult.map { result =>
            println(s"${result.userId}: 总访问${result.totalVisits}次, 页面:${result.pages}, 平均间隔${result.avgInterval}ms")
            (result.userId, s"${result.totalVisits}次访问")
        }.print()

        println("程序开始执行...")
        env.execute()
    }

}


// 自定义 AggregateFunction
class VisitAggregateFunction extends AggregateFunction[
    View,                               // 输入：View
    (String, Int, String, Long),        // 累加器：(用户, 次数, 页面, 时间间隔和)
    UserVisitSummary                     // 输出：UserVisitSummary
] {

    // 创建初始累加器
    override def createAccumulator(): (String, Int, String, Long) = ("", 0, "", 0L)

    // 添加元素到累加器
    override def add(value: View, accumulator: (String, Int, String, Long)): (String, Int, String, Long) = {
        val (user, count, pages, timeSum) = accumulator
        val newCount = count + 1
        val newPages = if (pages.isEmpty) value.url else pages + "|" + value.url

        // 计算时间间隔（如果是第一次访问，间隔为0）
        val timeInterval = if (count == 0) 0L else value.timestamp - accumulator._3.hashCode % 10000 // 简化处理
        val newTimeSum = timeSum + timeInterval

        (value.userId, newCount, newPages, newTimeSum)
    }

    // 从累加器获取最终结果
    override def getResult(accumulator: (String, Int, String, Long)): UserVisitSummary = {
        val (userId, totalVisits, pages, timeSum) = accumulator
        val avgInterval = if (totalVisits > 1) timeSum.toDouble / (totalVisits - 1) else 0.0
        UserVisitSummary(userId, totalVisits, pages, avgInterval)
    }

    // 合并累加器
    override def merge(a: (String, Int, String, Long), b: (String, Int, String, Long)): (String, Int, String, Long) = {
        (a._1, a._2 + b._2, a._3 + "|" + b._3, a._4 + b._4)
    }
}
