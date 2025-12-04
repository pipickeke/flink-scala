package com.example.spark.project_9

import org.apache.spark.{SparkConf, SparkContext}

object WorkCount {

    def main(args: Array[String]): Unit = {

        //1,创建环境
        val sparkConf = new SparkConf().setMaster("local").setAppName("word-count")
        val sc = new SparkContext(sparkConf)

        //2,读取文件
        val lines = sc.textFile("data/words.txt")

        //3，对单词进行分组
        val wordGroup = lines.flatMap(_.split(" "))
            .groupBy(word => word)

        //4，转换
        val wordMap = wordGroup.map{
            case (word, list) => {
                (word, list.size)
            }
        }

        //5，打印
        wordMap.collect().foreach(println)

        //关闭连接
        sc.stop()
    }
}
