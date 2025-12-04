package com.example.flink.project_5.task_1

import com.example.flink.project_5.common.Order
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

object ValueStateOrderExample {

    def main(args: Array[String]): Unit = {
        // 创建执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        // 设置并行度为1，方便查看结果
        env.setParallelism(1)

        // 模拟订单数据
        val orders = List(
            Order("o1", "user1", 100.0, "normal", 1000L),
            Order("o2", "user2", 200.5, "vip", 2000L),
            Order("o3", "user1", 50.0, "promo", 3000L),
            Order("o4", "user1", 25.5, "normal", 4000L),
            Order("o5", "user2", 75.0, "normal", 5000L)
        )

        val stream = env.fromCollection(orders)

        // 按 userId 分组
        val keyedStream = stream.keyBy(_.userId)

        // 应用处理函数
        val result = keyedStream.process(new UserTotalAmountFunction)

        result.print()

        println("程序开始运行...")
        env.execute()
    }

}


// 自定义 KeyedProcessFunction，使用 ValueState 累计金额
class UserTotalAmountFunction extends KeyedProcessFunction[String, Order, String] {


    private var totalAmountState: ValueState[Double] = _

    override def open(parameters: Configuration): Unit = {
        // 定义 ValueState：存储 Double 类型的累计金额
        val descriptor = new ValueStateDescriptor[Double]("user-total-amount", classOf[Double])
        totalAmountState = getRuntimeContext.getState(descriptor)
    }

    override def processElement(
                                   order: Order,
                                   ctx: KeyedProcessFunction[String, Order, String]#Context,
                                   out: Collector[String]
                               ): Unit = {
        // 获取当前累计金额（若为 null，则初始化为 0.0）
        val currentTotal = Option(totalAmountState.value()).getOrElse(0.0)

        // 加上新订单金额
        val newTotal = currentTotal + order.amount

        // 更新状态
        totalAmountState.update(newTotal)

        // 输出结果
        out.collect(s"User ${order.userId} total amount updated to: $newTotal (added ${order.amount})")
    }
}
