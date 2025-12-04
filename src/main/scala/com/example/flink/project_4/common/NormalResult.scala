package com.example.flink.project_4.common

case class NormalResult(windowStart: Long,
                        windowEnd: Long,
                        userId: String,
                        totalAmount: Double,
                        orderCount: Int)
