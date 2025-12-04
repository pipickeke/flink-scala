package com.example.flink.project_4.chapter_4_2

import com.example.flink.project_4.common.View
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object CustomTriggerExample {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 创建测试数据，包含特殊页面
        val viewStream: DataStream[View] = env.fromElements(
            View("user1", "home.html", System.currentTimeMillis()),
            View("user1", "about.html", System.currentTimeMillis() + 100),
            View("user1", "checkout.html", System.currentTimeMillis() + 200), // 特殊页面
            View("user2", "login.html", System.currentTimeMillis() + 300),
            View("user2", "search.html", System.currentTimeMillis() + 400),
            View("user1", "payment.html", System.currentTimeMillis() + 500), // 特殊页面
            View("user2", "cart.html", System.currentTimeMillis() + 600), // 特殊页面
            View("user1", "confirmation.html", System.currentTimeMillis() + 700),
            View("user2", "profile.html", System.currentTimeMillis() + 800)
        )

        // 使用自定义触发器：遇到 checkout/payment/cart 等特殊页面时触发
        val resultStream = viewStream
            .keyBy(_.userId)
            .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
            .trigger(new SpecialPageTrigger) // 自定义触发器
            .process(new CountProcessFunction)

        resultStream.print()
        println("程序开始运行...")

        env.execute()
        Thread.sleep(2000)
    }

}


/**
 * 自定义触发器：当遇到特殊页面（如 checkout、payment、cart）时触发
 */
class SpecialPageTrigger extends Trigger[View, TimeWindow] {

    // 特殊页面关键词
    private val SPECIAL_PAGES = Set("checkout.html", "payment.html", "cart.html", "order.html")

    override def onElement(
                              element: View,
                              timestamp: Long,
                              window: TimeWindow,
                              ctx: Trigger.TriggerContext): TriggerResult = {

        println(s"[触发器检查] 用户: ${element.userId}, 页面: ${element.url}, 时间戳: $timestamp")

        // 如果是特殊页面，立即触发
        if (SPECIAL_PAGES.contains(element.url)) {
            println(s"检测到特殊页面: ${element.url} - 触发窗口计算!")
            TriggerResult.FIRE  // 触发计算但保留窗口中的数据
        } else {
            TriggerResult.CONTINUE  // 继续收集元素
        }
    }

    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
        // 不响应处理时间
        TriggerResult.CONTINUE
    }

    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
        // 不响应事件时间
        TriggerResult.CONTINUE
    }

    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
        // 清理工作（本例不需要状态清理）
        println("[触发器清理] 清理窗口状态")
    }
}


class CountProcessFunction
    extends ProcessWindowFunction[View, String, String, TimeWindow] {

    override def process(
                            key: String,
                            context: Context,
                            elements: Iterable[View],
                            out: Collector[String]): Unit = {

        val urls = elements.map(_.url).toList
        val count = elements.size

        val result = f"CountTrigger - User: $key%-6s | " +
            f"触发批次: Pages=${urls.mkString(",")}%-30s | Count: $count"

        println(s"[Count触发] $result")
        out.collect(result)
    }
}
