package com.example.flink.project_4

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

class CountAverage extends RichFlatMapFunction[(Long,Long),(Long,Long)]{

    //1,创建状态对象，用来存放数据
    private var sum: ValueState[(Long,Long)] = _

    //2, 状态初始化
    override def open(parameters: Configuration): Unit = {
        sum = getRuntimeContext.getState(new ValueStateDescriptor[(Long, Long)]("average", classOf[(Long,Long)]))
    }

    override def flatMap(in: (Long, Long), collector: Collector[(Long, Long)]): Unit = {
        val tmpSum = sum.value()
        val curSum = if (tmpSum != null) {
            tmpSum
        } else {
            (0L,0L)
        }
        // 更新计数
        val newSum = (curSum._1 + 1, curSum._2 + in._2)
        sum.update(newSum)
        //计数达到2，
        if (newSum._1 >= 2){
            collector.collect((in._1, newSum._2 / newSum._1))
            sum.clear()
        }
    }
}
