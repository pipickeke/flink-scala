package com.example.flink.project_5

import com.example.flink.common.Order
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.{AnyWithOperations, FieldExpression}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

import java.text.SimpleDateFormat
import java.util.Properties

object SqlDemo {

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


        val tableEnv = StreamTableEnvironment.create(env)
        // 在创建完 tEnv 后添加这行：
        tableEnv.createTemporaryView("Orders",
            ordersStream, $"orderId", $"userId", $"amount")

        val resultSql = tableEnv.sqlQuery(
            """
          SELECT userId
          FROM Orders
        """)

        tableEnv.toAppendStream[Row](resultSql).print("User total via SQL: ")
        env.execute("Flink Table API and SQL Example")
    }

}
