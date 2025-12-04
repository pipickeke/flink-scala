package com.example.flink.project_3

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

object CollectionSourceDemo {

    def main(args: Array[String]): Unit = {
        //1，创建Flink流处理环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        // 2. 定义集合数据
        val userList: List[String] = List(
            "Alice,20",
            "Bob,25",
            "Charlie,30",
            "David,28"
        )

        //3,从集合中读取数据
        val data = env.fromCollection(userList)

    }

}
