package com.example.flink.common

// 定义增强版 OrderWithLabel 类
case class OrderWithLabel(orderId: String, userId: String, amount: Double, label: String)
