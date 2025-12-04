package com.example.flink.project_5

import com.example.flink.project_5.common.View
import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor, ReadOnlyBroadcastState}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{BroadcastConnectedStream, DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

object BroadcastStateExample {


    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

        // 模拟主数据流 - 用户浏览记录
        val viewData = List(
            View("user001", "https://example.com/home", 1000L),
            View("user002", "https://example.com/products", 1010L),
            View("user001", "https://example.com/bad-site", 1020L), // 黑名单 URL
            View("user003", "https://example.com/home", 1030L),
            View("user002", "https://example.com/malware", 1040L), // 黑名单 URL
            View("user001", "https://example.com/home", 1050L),
            View("user004", "https://example.com/blog", 1060L),
            View("user002", "https://example.com/products", 1070L),
            View("user001", "https://example.com/phishing", 1080L) // 黑名单 URL
        )

        val viewStream: DataStream[View] = env.fromCollection(viewData)

        // 模拟广播流 - 动态下发的黑名单 URL 列表 (timestamp, url)
        val blacklistData = List(
            ("blacklist", "https://example.com/bad-site"),
            ("blacklist", "https://example.com/malware"),
            ("blacklist", "https://example.com/phishing")
        )
        val blacklistStream: DataStream[(String, String)] = env.fromCollection(blacklistData)

        // 定义 BroadcastState 描述符：key=标识(这里固定为"blacklist")，value=URL字符串
        val blacklistDescriptor = new MapStateDescriptor[String, String](
            "blacklistState",
            BasicTypeInfo.STRING_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO
        )

        // 连接主流和广播流
        val connectedStream: BroadcastConnectedStream[View, (String, String)] =
            viewStream.connect(blacklistStream.broadcast(blacklistDescriptor))

        // 处理连接后的流
        val result = connectedStream.process(new BlacklistProcessFunction)

        result.print("处理结果: ")

        println("程序开始运行...")
        env.execute()

    }

}


/**
 * 处理 BroadcastState 的函数
 * - 主流：View 用户浏览记录
 * - 广播流：动态添加黑名单 URL
 */
class BlacklistProcessFunction extends BroadcastProcessFunction[View, (String, String), String] {

    // 声明 BroadcastState
    private val blacklistStateDesc = new MapStateDescriptor[String, String](
        "blacklistState",
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO
    )

    // 处理主流数据（每条 View 记录）
    override def processElement(
                                   value: View,
                                   ctx: BroadcastProcessFunction[View, (String, String), String]#ReadOnlyContext,
                                   out: Collector[String]
                               ): Unit = {
        // 获取只读的 BroadcastState
        val blacklist: ReadOnlyBroadcastState[String, String] =
            ctx.getBroadcastState(blacklistStateDesc)

        if (blacklist.contains(value.url)) {
            out.collect(s"[ALERT] 用户 ${value.userId} 访问了黑名单 URL: ${value.url}")
        } else {
            out.collect(s"[OK] 用户 ${value.userId} 访问了正常 URL: ${value.url}")
        }
    }

    // 处理广播流数据（添加新的黑名单 URL）
    override def processBroadcastElement(
                                            value: (String, String),
                                            ctx: BroadcastProcessFunction[View, (String, String), String]#Context,
                                            out: Collector[String]
                                        ): Unit = {
        // 获取可写的 BroadcastState
        val blacklist: BroadcastState[String, String] =
            ctx.getBroadcastState(blacklistStateDesc)

        // 将新的黑名单 URL 加入状态
        blacklist.put(value._2, value._2)
        out.collect(s"[UPDATE] 黑名单已更新，新增: ${value._2}")
    }
}


