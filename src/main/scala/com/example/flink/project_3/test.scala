package com.example.flink.project_3

import com.example.flink.common.Order
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}

import java.util.Random

object test {

    def main(args: Array[String]): Unit = {
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


//        orderStream.keyBy(_.userId).sum("amount")

//        import org.apache.flink.api.java.tuple.Tuple3
//
//        val result = orderStream
//            .keyBy(_.userId)
//            .map(o => new Tuple3(o.userId, 1, o.amount)) // 使用 Tuple3
//            .reduce { (a, b) =>
//                new Tuple3(a.f0, a.f1 + b.f1, a.f2 + b.f2) // f0: userId, f1: count, f2: amount
//            }
//
//        result.print("用户统计：")

        env.execute()
    }

}
