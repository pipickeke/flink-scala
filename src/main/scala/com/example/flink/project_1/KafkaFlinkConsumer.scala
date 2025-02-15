package com.example.flink.project_1

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties

object KafkaFlinkConsumer {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        //配置
        val prop = new Properties()
        prop.setProperty("bootstrap.servers", "master:9092")
        prop.setProperty("group.id", "kafka-flink-consumer")
        prop.setProperty("auto.offset.reset", "latest")

        //创建kafka数据源
        val topic = "test_001"
        val kafkaSource = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), prop)

        //读取kafka数据
        val kafkaStream = env.addSource(kafkaSource)

        kafkaStream.print()

        env.execute("Kakfa Flink Consumer")
    }

}
