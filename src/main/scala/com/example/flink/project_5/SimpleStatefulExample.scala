package com.example.flink.project_5

import com.example.flink.project_5.common.View
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

object SimpleStatefulExample {

    def main(args: Array[String]): Unit = {
        // 创建执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 模拟数据源
        val viewData = List(
            View("user001", "https://example.com/home", 1625097600000L),
            View("user002", "https://example.com/products", 1625097601000L),
            View("user001", "https://example.com/about", 1625097602000L),
            View("user003", "https://example.com/home", 1625097603000L),
            View("user002", "https://example.com/contact", 1625097604000L),
            View("user004", "https://example.com/products", 1625097605000L),
            View("user001", "https://example.com/products", 1625097606000L),
            View("user005", "https://example.com/blog", 1625097607000L)
        )

        val viewStream: DataStream[View] = env.fromCollection(viewData)
        viewStream.print("原始数据: ")

        // 无状态Map转换
        val simpleMapStream = viewStream
            .map(view => s"[MAP] 用户 ${view.userId} 访问了 ${view.url}")
        simpleMapStream.print("map转换后的数据: ")

        // 有状态处理
        val simpleStateStream = viewStream
            .keyBy(_.userId)
            .process(new SimpleCountFunction)
        simpleStateStream.print("累计访问: ")

        println("程序开始运行...")
        env.execute()
    }

}


// 简单的有状态函数：统计每个用户的累计访问次数
class SimpleCountFunction extends KeyedProcessFunction[String, View, String] {

    private var countState: ValueState[Int] = _

    override def open(parameters: Configuration): Unit = {
        // 初始化状态
        val descriptor = new ValueStateDescriptor[Int]("simpleCount", classOf[Int])
        countState = getRuntimeContext.getState(descriptor)
    }

    override def processElement(
                                   value: View,
                                   ctx: KeyedProcessFunction[String, View, String]#Context,
                                   out: Collector[String]
                               ): Unit = {

        // 读取并更新状态
        val current = Option(countState.value()).getOrElse(0)
        val newCount = current + 1
        countState.update(newCount)

        // 输出
        out.collect(s"用户 ${value.userId} 累计访问 ${newCount} 次")
    }
}
