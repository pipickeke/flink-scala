package com.example.flink.project_4

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object SqlDemo {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        //创建表环境
        // 创建 Table 环境
        val settings = EnvironmentSettings.newInstance()
            .inStreamingMode()
            .build()
        val tableEnv = StreamTableEnvironment.create(env, settings)

        //注册表
        tableEnv.executeSql(
            """
              |CREATE TEMPORARY TABLE person_source (
              |  id INT,
              |  name STRING,
              |  age INT
              |) WITH (
              |  'connector' = 'datagen',
              |  'rows-per-second' = '1',
              |  'fields.id.kind' = 'sequence',
              |  'fields.id.start' = '1',
              |  'fields.id.end' = '10',
              |  'fields.name.length' = '5',
              |  'fields.age.min' = '20',
              |  'fields.age.max' = '40'
              |)
              |""".stripMargin)

        //注册输出表
        tableEnv.executeSql(
            """
              |CREATE TEMPORARY TABLE person_sink (
              |  id INT,
              |  age INT
              |) WITH (
              |  'connector' = 'print'
              |)
              |""".stripMargin)

        // 执行 SQL 逻辑
        tableEnv.executeSql(
            """
              |INSERT INTO person_sink
              |SELECT id, age
              |FROM person_source
              |WHERE age > 25
              |""".stripMargin)
    }

}
