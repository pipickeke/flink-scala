package com.example.flink.project_3

import com.example.flink.common.UserOrder
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import java.util.Random

object Task_3 {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        implicit val orderTypeInfo: TypeInformation[UserOrder] = TypeInformation.of(classOf[UserOrder])

        // 自定义 Source：每秒生成一条订单
        val UserOrderStream: DataStream[UserOrder] = env.addSource(new SourceFunction[UserOrder] {
            private var isRunning = true
            private var UserOrderNum = 101
            private val userIds = Array("user_001", "user_002", "user_003")
            private val labels = Array("vip", "normal", "promotion")
            private val random = new Random()

            override def run(ctx: SourceFunction.SourceContext[UserOrder]): Unit = {
                while (isRunning) {
                    val UserOrderId = s"UserOrder_$UserOrderNum"
                    UserOrderNum += 1
                    val userId = userIds(random.nextInt(userIds.length))
                    val amount = 50 + random.nextDouble() * 200 // 50 ~ 250
                    val label = labels(random.nextInt(labels.length))
                    ctx.collect(UserOrder(UserOrderId, userId, amount, label))
                    Thread.sleep(1000)
                }
            }

            override def cancel(): Unit = {
                isRunning = false
            }
        })

        // Sink 到文件（
        UserOrderStream.writeAsText("UserOrder_output.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1)
        env.execute()
    }
}
