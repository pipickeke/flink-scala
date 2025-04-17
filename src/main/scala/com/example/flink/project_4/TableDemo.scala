package com.example.flink.project_4

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

case class Person(id:Int, name:String, age:Int)
object TableDemo {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        //创建表环境
        val tableEnv = StreamTableEnvironment.create(env)

        //创建数据
        val input = env.fromElements(
            Person(1, "Alice", 28),
            Person(2, "Bob", 35),
            Person(3, "Cathy", 23)
        )

        //将 DataStream转换为table
        val personTable = tableEnv.fromDataStream(input)
        //创建临时视图，方便使用字段
        tableEnv.createTemporaryView("person_view", personTable)
        //使用 table api查询
        val res = tableEnv.sqlQuery(
            """
              |select id,name,age
              |from person_view
              |where age > 25
              |""".stripMargin
        )

        //输出
        tableEnv.toAppendStream[Row](res).print()

        env.execute()
    }

}
