package com.example.flink.common

case class Order(orderId: String, userId: String, amount: Double, timestamp: String)

case class UserOrder(orderId: String, userId: String, amount: Double, label: String)
