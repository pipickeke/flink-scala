package com.example.flink.project_5

import com.example.flink.project_5.common.View
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector


object MapStateDemo {

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
            .process(new UserUrlTimestampFunction)
        urlCountStream.print()

        println("程序开始运行...")
        env.execute()
    }

}



/**
 * 每次收到新记录时，更新该用户对应 URL 的最新时间戳
 * 最终输出每个用户的 URL → 最新时间戳 映射
 */
class UserUrlTimestampFunction extends KeyedProcessFunction[String, View, String] {

    private var urlTimestampState: MapState[String, Long] = _

    override def open(parameters: Configuration): Unit = {
        // 定义 MapState：key=url, value=timestamp
        val descriptor = new MapStateDescriptor[String, Long](
            "user-url-timestamp-map",
            classOf[String],
            classOf[Long]
        )
        urlTimestampState = getRuntimeContext.getMapState(descriptor)
    }

    override def processElement(
                                   value: View,
                                   ctx: KeyedProcessFunction[String, View, String]#Context,
                                   out: Collector[String]
                               ): Unit = {
        // 更新或插入当前 url 的 timestamp
        urlTimestampState.put(value.url, value.timestamp)

        // 构建当前用户的所有 (url -> timestamp) 映射
        import scala.collection.JavaConverters._
        val mapSnapshot = urlTimestampState.entries().asScala.map { entry =>
            s"${entry.getKey}@${entry.getValue}"
        }.mkString(", ")

        out.collect(s"User ${value.userId} latest URL timestamps: $mapSnapshot")
    }
}