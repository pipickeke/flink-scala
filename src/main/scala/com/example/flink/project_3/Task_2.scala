package com.example.flink.project_3

import com.example.flink.common.UserOrder
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.util.Random

//case class UserOrder(orderId: String, userId: String, amount: Double, label: String)

object Task_2 {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        implicit val orderTypeInfo: TypeInformation[UserOrder] = TypeInformation.of(classOf[UserOrder])

        // 自定义 Source
        val orderStream: DataStream[UserOrder] = env.addSource(new SourceFunction[UserOrder] {
            private var isRunning = true
            private var orderNum = 1
            private val userIds = Array("user_001", "user_002", "user_003")
            private val labels = Array("normal", "promotion", "vip")
            private val random = new Random()

            override def run(ctx: SourceFunction.SourceContext[UserOrder]): Unit = {
                while (isRunning) {
                    val orderId = s"order_$orderNum"
                    orderNum += 1
                    val userId = userIds(random.nextInt(userIds.length))
                    val amount = 50 + random.nextDouble() * 200
                    val label = labels(random.nextInt(labels.length))
                    ctx.collect(UserOrder(orderId, userId, amount, label))
                    Thread.sleep(1000)
                }
            }

            override def cancel(): Unit = {
                isRunning = false
            }
        })

        // 过滤：金额 >= 100
        val highAmountOrders = orderStream.filter(_.amount >= 100)

        // 分组聚合：按用户累计金额
        val keyedStream = highAmountOrders.keyBy(_.userId)
        val userTotalAmount = keyedStream.reduce((a,b) => UserOrder(
            userId = a.userId,
            orderId = a.orderId,
            amount = a.amount + b.amount,
            label = a.label
        ))

        // 打新标签
        val labeledOrders = orderStream.map { order =>
            val newLabel = if (order.amount >= 200) s"高价值_${order.label}" else s"普通_${order.label}"
            order.copy(label = newLabel)
        }

        // 输出
        highAmountOrders.print("金额≥100的订单：")
        userTotalAmount.print("用户累计金额：")
        labeledOrders.print("带新标签的订单：")

        env.execute()
    }
}