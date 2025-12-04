package com.example.flink.project_3

import com.example.flink.common.{Order, OrderWithLabel}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties

object EnhancedOrderProcessing {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        // Kafka配置
        val prop = new Properties()
        prop.setProperty("bootstrap.servers", "master:9092")
        prop.setProperty("group.id", "Flink-order-group")
        prop.setProperty("auto.offset.reset", "latest")

        // 创建Kafka源
        val kafkaSource = new FlinkKafkaConsumer[String](
            "order-topic",
            new SimpleStringSchema(),
            prop
        )

        // 处理流 - 使用局部变量方式避免序列化问题
        val ordersStream = env.addSource(kafkaSource)
            .map { jsonStr =>
                // 在map函数内部创建ObjectMapper，避免序列化问题
                val mapper = new ObjectMapper()
                mapper.registerModule(DefaultScalaModule)

                try {
                    mapper.readValue(jsonStr, classOf[Order])
                } catch {
                    case e: Exception =>
                        println(s"Failed to parse JSON: $jsonStr, Error: ${e.getMessage}")
                        null
                }
            }
            .filter(_ != null)

        //过滤金额大于500的订单
        val highvalueOrders = ordersStream.filter(_.amount > 500)

        //添加标签，标记高价值订单
        val labeledOrders = highvalueOrders.map(order =>
            OrderWithLabel(orderId = order.orderId, userId = order.userId, amount = order.amount, label = "HighValue")
        )

        labeledOrders.print("High Value Orders: ")
        env.execute("Enhanced Order Processing")
    }


}
