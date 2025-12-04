package com.example.flink.project_3

import com.example.flink.common.Order
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import java.util.Random

// 订单样例类
//case class Order(orderId: String, userId: String, amount: Double, label: String)

object Task_1 {


    def main(args: Array[String]): Unit = {

        // 1. 获取执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        // 手动指定 TypeInformation
        val orderTypeInfo: TypeInformation[Order] = TypeInformation.of(classOf[Order])

        // 2. 自定义 Source 生成订单数据
        val orderStream: DataStream[Order] = env.addSource(new SourceFunction[Order] {
            private var isRunning = true
            private var orderNum = 1
            private val userIds = Array("user_001", "user_002", "user_003")
            private val labels = Array("normal", "promotion", "vip")
            private val random = new Random()

            override def run(ctx: SourceFunction.SourceContext[Order]): Unit = {
                while (isRunning) {
                    val orderId = s"order_$orderNum"
                    orderNum += 1
                    val userId = userIds(random.nextInt(userIds.length))
                    val amount = 50 + random.nextDouble() * 200 // 50 ~ 250
                    val label = labels(random.nextInt(labels.length))
                    ctx.collect(Order(orderId, userId, amount, label))
                    Thread.sleep(1000) // 每秒一条
                }
            }

            override def cancel(): Unit = {
                isRunning = false
            }
        })(orderTypeInfo)

        // 3. 打印订单数据
        orderStream.print("模拟订单数据：")

        // 4. 执行作业
        env.execute()
    }

}
