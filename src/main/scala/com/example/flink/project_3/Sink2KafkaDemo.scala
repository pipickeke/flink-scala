package com.example.flink.project_3

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.kafka.clients.producer.ProducerRecord

import java.nio.charset.StandardCharsets
import java.util.Properties

object Sink2KafkaDemo {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val dataStream: DataStream[View] = env.fromCollection(
            List(
                View("Tom", "/index.html", 1000L),
                View("Mike", "/home.html", 2000L),
                View("Bob", "/car.html", 4000L),
            )
        )
         writeToKafka(dataStream)

        env.execute()
    }

    def writeToKafka(dataStream: DataStream[View]): Unit = {
        val kafkaBootstrapServers = "localhost:9092"
        val kafkaTopic = "views-topic"

        // 将 View 转为 Kafka 消息（JSON 或字符串）
        val serializationSchema = new KafkaSerializationSchema[View] {
            override def serialize(element: View, timestamp: java.lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
                val message = s"User:${element.user},URL:${element.url},Time:${element.timestamp}"
                new ProducerRecord[Array[Byte], Array[Byte]](kafkaTopic, message.getBytes(StandardCharsets.UTF_8))
            }
        }

        val props = new Properties()
        props.setProperty("bootstrap.servers", kafkaBootstrapServers)
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

        val kafkaSink = new FlinkKafkaProducer[View](
            "test-output-topic", // 默认 topic（如果 serializationSchema 中未指定）
            serializationSchema,
            props,
            Semantic.EXACTLY_ONCE // 或 AT_LEAST_ONCE（默认），EXACTLY_ONCE 需要开启 checkpoint
        )

        dataStream.addSink(kafkaSink).name("KafkaSink")

        println(s"数据将写入 Kafka Topic: $kafkaTopic @ $kafkaBootstrapServers")
    }

}
