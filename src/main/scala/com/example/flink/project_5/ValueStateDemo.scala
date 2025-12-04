package com.example.flink.project_5

import com.example.flink.project_5.common.View
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

object ValueStateDemo {


    def main(args: Array[String]): Unit = {
        // 创建执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        // 设置并行度为1，方便查看结果
        env.setParallelism(1)

        // 模拟数据源 - 故意设计一些重复用户和特殊场景
        val viewData = List(
            View("user001", "https://example.com/home", 1625097600000L),
            View("user002", "https://example.com/products", 1625097601000L),
            View("user001", "https://example.com/about", 1625097602000L),
            View("user003", "https://example.com/home", 1625097603000L),
            View("user002", "https://example.com/contact", 1625097604000L),
            View("user001", "https://example.com/home", 1625097605000L), // user001再次访问home
            View("user004", "https://example.com/blog", 1625097606000L),
            View("user002", "https://example.com/products", 1625097607000L), // user002再次访问products
            View("user001", "https://example.com/contact", 1625097608000L)
        )

        // 创建数据流
        val viewStream: DataStream[View] = env.fromCollection(viewData)

        viewStream.print("原始数据: ")

        // 简单的Map转换（无状态）
        val simpleMapStream = viewStream
            .map(view =>
                s"[MAP] 用户 ${view.userId} 在时间 ${view.timestamp} 访问了 ${view.url}"
            )
        simpleMapStream.print("map转换后的数据: ")

        val firstVisitStateStream = viewStream
            .keyBy(_.userId)
            .process(new FirstVisitFunction)
        firstVisitStateStream.print("首次访问记录: ")

        println("程序开始运行...")
        env.execute()

    }

}


// ValueState案例: 记录用户首次访问的页面
class FirstVisitFunction extends KeyedProcessFunction[String, View, String] {

    // 定义ValueState来存储首次访问信息
    private var firstVisitState: ValueState[View] = _

    override def open(parameters: Configuration): Unit = {
        val stateDescriptor = new ValueStateDescriptor[View]("firstVisit", classOf[View])
        firstVisitState = getRuntimeContext.getState(stateDescriptor)
    }

    override def processElement(
                                   value: View,
                                   ctx: KeyedProcessFunction[String, View, String]#Context,
                                   out: Collector[String]
                               ): Unit = {

        // 检查是否已经有首次访问记录
        val firstVisit = Option(firstVisitState.value())

        if (firstVisit.isEmpty) {
            // 第一次访问，保存状态
            firstVisitState.update(value)
            out.collect(s"用户 ${value.userId} 首次访问: ${value.url} (时间: ${value.timestamp})")
        } else {
            // 不是首次访问，显示首次访问信息
            val first = firstVisit.get
            out.collect(s"用户 ${value.userId} 本次访问: ${value.url}, 首次访问: ${first.url} (时间: ${first.timestamp})")
        }
    }
}
