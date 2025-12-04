package com.example.flink.project_4.common

case class OrderFrequency(userId: String, windowStart: Long, windowEnd: Long, orderCount: Int)
