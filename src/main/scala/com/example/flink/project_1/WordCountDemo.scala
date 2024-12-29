package com.example.flink.project_1

import org.apache.flink.api.scala.{ExecutionEnvironment, createTypeInformation}

object WordCountDemo {

    def main(args: Array[String]): Unit = {
        //1,创建执行环境
        val env = ExecutionEnvironment.getExecutionEnvironment
        //2,读取源数据
        val inputDataSet = env.readTextFile("input/testdata.txt")
        //3,数据处理
        val wordWithOne = inputDataSet
            .flatMap(_.split(" "))
            .map(x => (x, 1))
        //4,根据单词进行分组
        val wordGrouped = wordWithOne.groupBy(0)
        //5,分组内进行数量统计
        val result = wordGrouped.sum(1)
        //6，打印输出
        result.print()
    }
}
