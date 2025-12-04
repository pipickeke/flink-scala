package com.example.flink.project_5

import com.example.flink.project_5.common.View
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{AggregatingState, AggregatingStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

object AggStateDemo {

    def main(args: Array[String]): Unit = {
        // 创建执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        // 设置并行度为1，方便查看结果
        env.setParallelism(1)

        // 模拟数据源
        val viewData = List(
            View("user001", "https://example.com/home", 1000L),
            View("user002", "https://example.com/products", 1010L),
            View("user001", "https://example.com/about", 1020L),
            View("user003", "https://example.com/home", 1030L),
            View("user002", "https://example.com/contact", 1040L),
            View("user001", "https://example.com/home", 1050L), // user001再次访问home
            View("user004", "https://example.com/blog", 1060L),
            View("user002", "https://example.com/products", 1070L), // user002再次访问products
            View("user001", "https://example.com/contact", 1080L)
        )

        // 创建数据流
        val viewStream: DataStream[View] = env.fromCollection(viewData)

        viewStream.print("原始数据: ")

        // 使用 MapState 统计每个用户的最新时间戳
        val urlCountStream = viewStream
            .keyBy(_.userId) // 按用户ID分组
            .process(new ViewCountFunction)
        urlCountStream.print()

        println("程序开始运行...")
        env.execute()
    }

}


// 定义 AggregateFunction：输入 View，累加器是 Int（计数），输出也是 Int
class ViewCountAggregateFunction extends AggregateFunction[View, Int, Int] {
    override def createAccumulator(): Int = 0
    override def add(value: View, accumulator: Int): Int = accumulator + 1
    override def getResult(accumulator: Int): Int = accumulator
    override def merge(a: Int, b: Int): Int = a + b
}

// KeyedProcessFunction 使用 AggregatingState
class ViewCountFunction extends KeyedProcessFunction[String, View, String] {

    private var countState: AggregatingState[View, Int] = _

    override def open(parameters: Configuration): Unit = {
        val descriptor = new AggregatingStateDescriptor[View, Int, Int](
            "view-count",
            new ViewCountAggregateFunction(),
            classOf[Int] // accumulator type
        )
        countState = getRuntimeContext.getAggregatingState(descriptor)
    }

    override def processElement(
                                   value: View,
                                   ctx: KeyedProcessFunction[String, View, String]#Context,
                                   out: Collector[String]
                               ): Unit = {
        // 添加新元素，状态自动聚合
        countState.add(value)

        // 获取当前聚合结果（即总访问次数）
        val currentCount = countState.get()

        out.collect(s"User ${value.userId} has viewed $currentCount pages")
    }
}



