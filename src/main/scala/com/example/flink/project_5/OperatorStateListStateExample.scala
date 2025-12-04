package com.example.flink.project_5

import com.example.flink.project_5.common.View
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

object OperatorStateListStateExample {


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
        val result = viewStream.flatMap(new GlobalViewCollector)
        result.print()

        println("程序开始运行...")
        env.execute()

    }

}

// 自定义 RichFlatMapFunction，使用 Operator State (ListState)
class GlobalViewCollector extends RichFlatMapStructure with CheckpointedFunction {

    // 用于存储 Operator State 的 ListState
    private var operatorState: ListState[View] = _

    // 本地缓存
    private val localBuffer = scala.collection.mutable.ListBuffer[View]()

    override def flatMap(value: View, out: Collector[String]): Unit = {
        // 将新元素加入本地缓冲区
        localBuffer += value

        // 打印当前所有已收集的 View
        val allViews = localBuffer.toList
        out.collect(s"Global collected views (${allViews.size}): ${allViews.map(v => s"${v.userId}@${v.url}").mkString(", ")}")
    }

    override def snapshotState(context: FunctionSnapshotContext): Unit = {
        // 清空状态并写入当前 localBuffer
        operatorState.clear()
        localBuffer.foreach(operatorState.add)
    }

    override def initializeState(context: FunctionInitializationContext): Unit = {
        val descriptor = new ListStateDescriptor[View]("global-view-list", classOf[View])
        operatorState = context.getOperatorStateStore.getListState(descriptor)

        // 如果是恢复（restore），从 state 重建 localBuffer
        if (context.isRestored) {
            localBuffer.clear()
            import scala.collection.JavaConverters._
            operatorState.get().asScala.foreach(localBuffer += _)
        }
    }
}

// 解决 Scala trait 初始化顺序问题：拆分结构
trait RichFlatMapStructure extends RichFlatMapFunction[View, String]
