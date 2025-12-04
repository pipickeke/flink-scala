package com.example.flink.project_2

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties
import scala.util.matching.Regex

object LogAnalysis {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        //配置
        val prop = new Properties()
        prop.setProperty("bootstrap.servers", "master:9092")
        prop.setProperty("group.id", "kafka-flink-consumer")
        prop.setProperty("auto.offset.reset", "earliest")

        //创建kafka数据源
        val topic = "test_001"
        val kafkaSource = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), prop)

        val logStream = env.addSource(kafkaSource)

        //解析ip并统计日志
        val ipPattern: Regex = """(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})""".r

        val res = logStream.map(log => ipPattern.findFirstIn(log).getOrElse("UNKNOW")) //提取ip
            .filter(_ != "UNKNOW") //过滤无效ip
            .map(x => (x, 1)) //格式转换
            .keyBy(_._1) //按照ip进行keyby
            .sum(1) //统计

        res.print()
        env.execute()
    }

}
