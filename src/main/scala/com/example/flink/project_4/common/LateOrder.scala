package com.example.flink.project_4.common

case class LateOrder(order: Order,
                     delayTime: Long, // 延迟时间（毫秒）
                     arrivalTime: Long, // 实际到达时间
                     expectedWindow: String )// 应该在哪个窗口被处理)
