package com.example.flink.project_4.chapter_4_2

import com.example.flink.project_4.common.View
import com.example.flink.project_4.common.{UserWindowStats, View}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.util.Collector

object EventTimeTriggerExample {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 数据
        val viewStream: DataStream[View] = env.fromElements(
            View("user1", "page1", 1000L),
            View("user2", "page2", 2000L),
            View("user1", "page3", 3500L),
            View("user3", "page1", 4000L),
            View("user2", "page4", 5500L),
            View("user1", "page5", 7000L)
        )
            .assignAscendingTimestamps(_.timestamp)

        // 使用内置的 EventTimeTrigger（默认行为）
        val resultStream = viewStream
            .keyBy(_.userId)
            .window(TumblingEventTimeWindows.of(Time.seconds(5))) // 5秒的滚动窗口
            .trigger(EventTimeTrigger.create()) // 使用事件时间触发器
            .process(new SimpleProcessWindowFunction())

        resultStream.print()

        println("程序开始执行...")
        env.execute()
    }

}

class SimpleProcessWindowFunction
    extends ProcessWindowFunction[View, String, String, TimeWindow] {

    override def process(
                            key: String,
                            context: Context,
                            elements: Iterable[View],
                            out: Collector[String]): Unit = {

        val windowStart = context.window.getStart / 1000
        val windowEnd = context.window.getEnd / 1000
        val urls = elements.map(_.url).toList

        val result = s"User: $key | Window: [$windowStart-${windowEnd}s] | " +
            s"Pages: ${urls.mkString(", ")} | Count: ${elements.size}"

        out.collect(result)
    }
}
