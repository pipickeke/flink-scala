package com.example.flink.project_5.task_2

import com.example.flink.project_5.common.Order
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

object ListStateOrderExample {

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
            Order("o4", "user2", 75.0, "normal", 4000L)
        )

        val stream = env.fromCollection(orders)

        // 按 userId 分组
        val keyedStream = stream.keyBy(_.userId)

        // 应用处理函数
        val result = keyedStream.process(new UserOrderListFunction)

        result.print()

        println("程序开始运行...")
        env.execute()
    }

}

// 自定义 KeyedProcessFunction，使用 ListState 存储用户订单列表
class UserOrderListFunction extends KeyedProcessFunction[String, Order, String] {

    private var orderListState: ListState[Order] = _

    override def open(parameters: Configuration): Unit = {
        // 定义 ListState：存储 Order 对象列表
        val descriptor = new ListStateDescriptor[Order]("user-order-list", classOf[Order])
        orderListState = getRuntimeContext.getListState(descriptor)
    }

    override def processElement(
                                   order: Order,
                                   ctx: KeyedProcessFunction[String, Order, String]#Context,
                                   out: Collector[String]
                               ): Unit = {
        // 将新订单添加到列表
        orderListState.add(order)

        // 获取当前所有订单（用于输出）
        import scala.collection.JavaConverters._
        val allOrders = orderListState.get().asScala.toList

        // 输出：只显示 orderId 和 amount
        val summary = allOrders.map(o => s"${o.orderId}(${o.amount})").mkString(", ")
        out.collect(s"User ${order.userId} has orders: $summary")
    }
}
