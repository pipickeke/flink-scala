package com.example.flink.project_4

import com.example.flink.common.Order
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat
import java.util.Properties

object EventTimeAndWatermarksFromKafka {

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
            .assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor[Order](Time.seconds(5)){
                    override def extractTimestamp(t: Order): Long = {
                        val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX")
                        df.parse(t.timestamp).getTime
                    }
                }
            )

        //分组，每5分钟统计统计每个用户的总消费金额
        val totalAmountPer5MinPerUser = ordersStream.keyBy(_.userId)
            .timeWindow(Time.minutes(5))
            .process(new ProcessWindowFunction[Order, (String, Double), String, TimeWindow] {
                override def process(key: String, context: Context, elements: Iterable[Order], out: Collector[(String, Double)]): Unit = {
                    val totalAmount = elements.map(_.amount).sum
                    out.collect(key, totalAmount)
                }
            })

        totalAmountPer5MinPerUser.print()
        env.execute("Event Time and Watermarks from Kafka")
    }

}
