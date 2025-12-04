package com.example.spark.project_10

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._

object KafkaStreamingDemo {

    def main(args: Array[String]): Unit = {
        //1, 配置环境
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("KafkaStreamingDemo")
        val ssc = new StreamingContext(sparkConf, Seconds(5))

        //2,kafka参数配置
        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> "192.168.10.110:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "kafka-streaming-demo",
            "auto.offset.reset" -> "latest",
            "enable.auto.commit" -> (false: java.lang.Boolean)
        )

        val topics = Array("input-topic") // Kafka 输入主题

        //3,创建Kafka DStream
        val stream = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
        )

        //4, 处理
        val result = stream.map(record => record.value())
            .flatMap(_.split(" "))
            .map(word => (word, 1))
            .reduceByKey(_ + _)

        //5, 打印
        result.print()

        //6, 启动
        ssc.start()
        ssc.awaitTermination()
    }

}
