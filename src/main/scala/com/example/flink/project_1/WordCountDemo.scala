package com.example.flink.project_1

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.scala.{ExecutionEnvironment, createTypeInformation}
import org.apache.flink.util.Collector

object WordCountDemo {

    def main(args: Array[String]): Unit = {
        //1,创建执行环境
        val env = ExecutionEnvironment.getExecutionEnvironment

        env.readTextFile("input/testdata.txt")
            .flatMap(_.split(" "))
            .map(x => (x,1))
            .print()

    }

    class Tokenizer extends FlatMapFunction[String, (String, Int)] {
        override def flatMap(value: String, out: Collector[(String, Int)]): Unit = {
            val words = value.split("\\W+")
            for (word <- words if word.nonEmpty) {
                out.collect((word.toLowerCase, 1))
            }
        }
    }

}
