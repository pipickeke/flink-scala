package com.example.flink.project_5

import com.example.flink.project_5.common.View
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

object ListStateExample {

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

        // 简单的Map转换（无状态）
        val simpleMapStream = viewStream
            .map(view =>
                s"[MAP] 用户 ${view.userId} 在时间 ${view.timestamp} 访问了 ${view.url}"
            )
        simpleMapStream.print("map转换后的数据: ")

        // 使用 ListState 维护用户浏览历史
        val browseHistoryStream = viewStream
            .keyBy(_.userId)
            .process(new BrowseHistoryFunction)
        browseHistoryStream.print("用户浏览历史: ")

        // 使用 ListState 统计用户访问次数并检测重复访问
        val visitStatsStream = viewStream
            .keyBy(_.userId)
            .process(new BrowseHistoryFunction)
        visitStatsStream.print("用户访问统计: ")

        println("程序开始运行...")
        env.execute()

    }
}


/**
 * 使用 ListState 维护用户浏览历史的函数
 */
class BrowseHistoryFunction extends KeyedProcessFunction[String, View, String] {

    private var browseHistoryState: ListState[View] = _

    override def open(parameters: Configuration): Unit = {
        // 初始化 ListState，用于存储用户的浏览历史
        val historyDescriptor = new ListStateDescriptor[View](
            "browseHistory",
            createTypeInformation[View]
        )
        browseHistoryState = getRuntimeContext.getListState(historyDescriptor)
    }

    override def processElement(
                                   value: View,
                                   ctx: KeyedProcessFunction[String, View, String]#Context,
                                   out: Collector[String]
                               ): Unit = {

        // 将当前浏览记录添加到 ListState
        browseHistoryState.add(value)

        // 从状态中获取该用户的所有浏览历史
        import scala.collection.JavaConverters._
        val history = browseHistoryState.get().asScala.toList.sortBy(_.timestamp)

        // 生成输出信息
        val output =
            s"用户 ${value.userId} 当前共有 ${history.size} 条浏览记录: " +
                history.map(view => s"${view.url}(${view.timestamp})").mkString(" -> ")

        out.collect(output)
    }
}





