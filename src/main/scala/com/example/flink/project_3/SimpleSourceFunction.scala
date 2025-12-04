package com.example.flink.project_3

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object SimpleSourceFunction {

}


// 自定义 Source，继承 SourceFunction[String]
class SimpleSourceFunction extends SourceFunction[String] {

    // 标志位，控制是否继续发送数据
    @volatile private var isRunning = true

    override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
        var count = 0
        while (isRunning) {
            // 产生一条数据，比如模拟日志或事件
            val msg = s"message-$count"
            // 使用 SourceContext 收集并发出数据
            ctx.collect(msg)
            count += 1

            // 模拟间隔，避免过快
            Thread.sleep(1000)
        }
    }

    override def cancel(): Unit = {
        // 用户触发停止时，设置标志位为 false
        isRunning = false
    }
}

// 主程序：使用自定义的 SourceFunction
object CustomSourceFunctionExample {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        // 使用自定义 Source
        val stream: DataStream[String] = env.addSource(new SimpleSourceFunction)

        // 打印数据
        stream.print("Custom Source Output")

        env.execute("Custom SourceFunction Example")
    }
}