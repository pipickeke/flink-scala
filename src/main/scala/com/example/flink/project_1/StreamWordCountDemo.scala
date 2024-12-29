package com.example.flink.project_1

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

object StreamWordCountDemo {

    def main(args: Array[String]): Unit = {
        //1,创建流处理环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        //2,接入socket流数据
        val socketDataSet = env.socketTextStream("192.168.10.110", 6666)
        //3,数据处理
        val wordWithOne = socketDataSet.flatMap(_.split(" "))
            .map(item => (item, 1))
        //4,分组
        val wordGrouped = wordWithOne.keyBy(0)
        //5,统计数量
        val result = wordGrouped.sum(1)
        //6,打印
        result.print()
        //7,启动流式任务
        env.execute()
    }

}
