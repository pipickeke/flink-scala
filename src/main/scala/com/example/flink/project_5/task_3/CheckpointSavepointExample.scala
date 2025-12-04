package com.example.flink.project_5.task_3

import com.example.flink.project_5.common.Order
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.logging.log4j.core.time.Instant

object CheckpointSavepointExample {

    def main(args: Array[String]): Unit = {
        // 创建执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        // 设置并行度为1，方便查看结果
        env.setParallelism(1)

        // 启用 Checkpoint
        env.enableCheckpointing(5000) // 每 5 秒 checkpoint 一次
        env.getCheckpointConfig.setCheckpointTimeout(60000)
        env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
        env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)

        // 设置状态后端
        //env.setStateBackend(new FsStateBackend("file:///tmp/flink-checkpoints"))

        // 模拟持续输入
        val orders = List(
            Order("o1", "user1", 100.0, "normal", System.currentTimeMillis()),
            Order("o2", "user2", 200.5, "vip", System.currentTimeMillis()),
            Order("o3", "user1", 50.0, "promo", System.currentTimeMillis()),
            Order("o4", "user2", 75.0, "normal", System.currentTimeMillis()),
            Order("o5", "user1", 30.0, "gift", System.currentTimeMillis()) // 新增一条，用于验证恢复后继续处理
        )

        import scala.concurrent.duration._
        val stream = env.fromCollection(orders).map { order =>
            Thread.sleep(2000) // 每 2 秒发一条，便于手动操作
            order
        }

        val keyedStream = stream.keyBy(_.userId)
        val result = keyedStream.process(new UserOrderListWithRecovery)

        result.print()
        println("程序开始运行...")
        env.execute()
    }

}


class UserOrderListWithRecovery extends KeyedProcessFunction[String, Order, String] {

    private var orderListState: ListState[Order] = _

    override def open(parameters: Configuration): Unit = {
        val descriptor = new ListStateDescriptor[Order]("user-order-list", classOf[Order])
        orderListState = getRuntimeContext.getListState(descriptor)
    }

    override def processElement(
                                   order: Order,
                                   ctx: KeyedProcessFunction[String, Order, String]#Context,
                                   out: Collector[String]
                               ): Unit = {
        // 添加新订单
        orderListState.add(order)

        // 获取当前所有订单（用于输出）
        import scala.collection.JavaConverters._
        val allOrders = orderListState.get().asScala.toList

        val summary = allOrders.map(o => s"${o.orderId}(${o.amount})").mkString(", ")
        out.collect(s"[User ${order.userId} orders: $summary]")
    }
}