package com.example.spark.project_10

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCountStreaming {

    def main(args: Array[String]): Unit = {
        //1，创建环境
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount-streaming")
        val ssc = new StreamingContext(sparkConf, Seconds(5))

        //2, 创建 socket
        val lines = ssc.socketTextStream("192.168.10.110", 7777)
        lines.print()

        //3, 处理
        val result = lines.flatMap(_.split(" "))
            .map(word => (word, 1))
            .reduceByKey(_ + _)

        //4, 输出
        result.print()

        //5，启动流处理
        ssc.start()
        ssc.awaitTermination()
    }
}
