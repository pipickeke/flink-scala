package com.example.flink.project_4.chapter_4_2

import com.example.flink.project_4.common.View
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{EventTimeTrigger, ProcessingTimeTrigger}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object SimpleProcessingTimeTriggerExample {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 数据
        val viewStream = env.fromElements(
            View("user1", "home.html", 1000L),
            View("user2", "about.html", 2000L),
            View("user1", "products.html", 3000L),
            View("user3", "contact.html", 4000L),
            View("user2", "pricing.html", 5000L),
            View("user1", "support.html", 6000L)
        )

        // 分配时间戳和水印
        val timedStream = viewStream.assignAscendingTimestamps(_.timestamp)

        println("开始处理数据，使用事件时间窗口...")

        // 使用事件时间窗口，更容易控制输出
        val resultStream = timedStream
            .keyBy(_.userId)
            .window(TumblingEventTimeWindows.of(Time.seconds(5))) // 5秒事件时间窗口
            .trigger(EventTimeTrigger.create())
            .process(new SimpleEventTimeProcessFunction)

        resultStream.print()

        // 执行并等待
        val jobResult = env.execute()

        // 给足够时间让窗口触发
        Thread.sleep(2000)
        println("程序执行完毕！")
    }

}
class SimpleEventTimeProcessFunction
    extends ProcessWindowFunction[View, String, String, TimeWindow] {

    override def process(
                            key: String,
                            context: Context,
                            elements: Iterable[View],
                            out: Collector[String]): Unit = {

        val windowStartSec = context.window.getStart / 1000
        val windowEndSec = context.window.getEnd / 1000
        val urls = elements.map(_.url).toList

        val result = f"User: $key%-6s | Window: [$windowStartSec%ds - $windowEndSec%ds] | " +
            f"Pages: ${urls.mkString(",")}%-25s | Count: ${elements.size}"

        println(s"[事件时间窗口] $result")
        out.collect(result)
    }
}