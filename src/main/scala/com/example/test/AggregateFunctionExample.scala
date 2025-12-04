package com.example.test

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * 统计24小时内每个用户的订单平均消费额
  */
object AggregateFunctionExample {
    def main(args: Array[String]): Unit = {
        //第一步：创建流处理的执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        //第二步：读取流数据
        val stream = env.socketTextStream("localhost", 9999)
        //用户ID,订单金额
        //1001,102.5
        //1002,98.3
        //1001,88.5
        //1001,56

//        val stream = env.fromElements(
//            "1001,102.5",
//            "1001,98.3",
//            "1001,88.5",
//            "1001,56"
//        )

        //第三步：转换并计算数据
        val data = stream.map(line=>{
            (line.split(",")(0),line.split(",")(1).toDouble)
        })
        .keyBy(_._1)
        //定义24小时滚动窗口
        .window(TumblingProcessingTimeWindows.of(Time.hours(24)))
        .aggregate(new MyAggregateFunction)//指定自定义的聚合函数
        .print("output")
        //触发任务执行
        env.execute("AggregateFunctionExample")
    }

    /**
      * 自定义聚合类，实现AggregateFunction接口
      * 泛型的三个数据类型：输入数据(String,Double)、累加器（即中间数据）(Int, Double)、结果数据Double
      */
    class MyAggregateFunction extends AggregateFunction[(String,Double), (Int, Double), Double] {
        /**
          * 创建累加器
          * 数据类型为(Int, Double)，即(订单数量,总金额)
          */
        override def createAccumulator(): (Int, Double) = (0, 0)
        /**
          * 将给定的输入值添加到给定的累加器中，返回新的累加器
          * @param value 给定的输入值，格式为（用户ID，订单金额）
          * @param accumulator 累加器，格式为（订单数量，总金额）
          * @return 新的累加器
          */
        override def add(value: (String,Double), accumulator: (Int, Double)):(Int, Double) = {
            (accumulator._1 + 1, accumulator._2 + value._2)
        }
        /**
          * 根据累加器计算聚合结果
          * 累加器的第二个字段为总金额，第一个字段为订单数量
          */
        override def getResult(accumulator: (Int, Double)): Double = {
            //计算平均值并保留两位小数
            (accumulator._2/accumulator._1).formatted("%.2f").toDouble
        }
        /**
          * 合并两个累加器，返回具有合并状态的累加器（只对会话窗口起作用，其他窗口不会调用该方法）
          */
        override def merge(a: (Int, Double), b: (Int, Double)): (Int, Double) = {
            (a._1 + b._1, a._2 + b._2)
        }
    }
}
