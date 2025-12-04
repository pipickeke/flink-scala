package com.example.flink.project_4.chapter_4_2

import com.example.flink.project_4.common.View
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object CountTriggerExample {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 创建测试数据
        val viewStream: DataStream[View] = env.fromElements(
            View("user1", "home.html", System.currentTimeMillis()),
            View("user1", "about.html", System.currentTimeMillis() + 100),
            View("user2", "products.html", System.currentTimeMillis() + 200),
            View("user1", "contact.html", System.currentTimeMillis() + 300),
            View("user2", "pricing.html", System.currentTimeMillis() + 400),
            View("user1", "support.html", System.currentTimeMillis() + 500),
            View("user2", "login.html", System.currentTimeMillis() + 600),
            View("user3", "register.html", System.currentTimeMillis() + 700)
        )

        // 使用 CountTrigger：每收到2个元素就触发一次窗口计算
        val resultStream = viewStream
            .keyBy(_.userId)
            .window(TumblingProcessingTimeWindows.of(Time.seconds(10))) // 大窗口确保数据不被丢弃
            .trigger(CountTrigger.of(2)) // 每2个元素触发一次
            .process(new CountTriggerProcessFunction)

        resultStream.print()

        println("程序开始运行...")
        env.execute()
        // 等待输出完成
        Thread.sleep(2000)
        println("程序执行完毕！")
    }

}

class CountTriggerProcessFunction
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
