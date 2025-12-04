package com.example.flink.project_3

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}


// 自定义 RichSourceFunction，继承自 RichSourceFunction[String]
class RichSimpleSourceFunction extends RichSourceFunction[String] {

    @volatile private var isRunning = true

    // 可选的：在 open() 方法中进行初始化操作，比如建立数据库连接等
    override def open(parameters: Configuration): Unit = {
        println(s"RichSourceFunction 初始化，并行度 = ${getRuntimeContext.getNumberOfParallelSubtasks}")
        // 你可以在这里初始化资源，如数据库连接、网络客户端等
    }

    override def run(ctx: org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext[String]): Unit = {
        var count = 0
        while (isRunning) {
            val msg = s"rich-message-${count} [from subtask]"
            ctx.collect(msg)
            count += 1
            Thread.sleep(1500) // 比上面慢一点，便于观察
        }
    }

    override def cancel(): Unit = {
        println("RichSourceFunction 正在停止...")
        isRunning = false
    }

    // 可选的：在 close() 方法中释放资源，如关闭连接
    override def close(): Unit = {
        println("RichSourceFunction 资源已释放")
    }
}

// 主程序：使用 RichSourceFunction
//object CustomRichSourceFunctionExample {
//    def main(args: Array[String]): Unit = {
//        val env = StreamExecutionEnvironment.getExecutionEnvironment
//
//        // 使用自定义的 RichSourceFunction
////        val stream: DataStream[String] = env.addSource(new RichSimpleSourceFunction)
//
//        stream.print("Rich Source Output")
//
//        env.execute("Custom RichSourceFunction Example")
//    }
//}