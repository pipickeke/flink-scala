package com.example.spark.project_12.models

import com.example.spark.project_12.models
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import java.sql.Timestamp

// 用户行为事件
case class UserEvent(
                        userId: String,
                        sessionId: String,
                        eventType: String, // "view", "click", "add_to_cart", "purchase", "search"
                        productId: Option[String],
                        categoryId: Option[String],
                        price: Option[Double],
                        quantity: Option[Int],
                        searchQuery: Option[String],
                        eventTime: Timestamp,
                        ipAddress: String,
                        deviceType: String
                    )

object UserEvent {
    private val mapper = {
        val m = new ObjectMapper()
        m.registerModule(DefaultScalaModule)
        m
    }

    def fromJson(json: String): Option[models.UserEvent] = {
        try {
            Some(mapper.readValue(json, classOf[models.UserEvent]))
        } catch {
            case e: Exception => None
        }
    }
}


// 订单事件
case class OrderEvent(
                         orderId: String,
                         userId: String,
                         productId: String,
                         quantity: Int,
                         unitPrice: Double,
                         totalPrice: Double,
                         paymentMethod: String,
                         orderTime: Timestamp,
                         status: String // "created", "paid", "shipped", "delivered", "cancelled"
                     )

object OrderEvent {
    private val mapper = {
        val m = new ObjectMapper()
        m.registerModule(DefaultScalaModule)
        m
    }

    def fromJson(json: String): Option[models.OrderEvent] = {
        try {
            Some(mapper.readValue(json, classOf[models.OrderEvent]))
        } catch {
            case e: Exception => None
        }
    }
}

// 商品实时统计
case class ProductStats(
                           productId: String,
                           viewCount: Long,
                           purchaseCount: Long,
                           addToCartCount: Long,
                           revenue: Double,
                           windowStart: Timestamp,
                           windowEnd: Timestamp
                       )

object ProductStats {
    private val mapper = {
        val m = new ObjectMapper()
        m.registerModule(DefaultScalaModule)
        m
    }

    def fromJson(json: String): Option[models.ProductStats] = {
        try {
            Some(mapper.readValue(json, classOf[models.ProductStats]))
        } catch {
            case e: Exception => None
        }
    }
}


// 用户会话分析
case class SessionAnalysis(
                              sessionId: String,
                              userId: String,
                              startTime: Timestamp,
                              endTime: Timestamp,
                              eventCount: Int,
                              productViews: Int,
                              purchases: Int,
                              totalValue: Double
                          )

// 异常交易
case class FraudulentTransaction(
                                    orderId: String,
                                    userId: String,
                                    amount: Double,
                                    transactionTime: Timestamp,
                                    reason: String // "high_value", "high_frequency", "unusual_hour"
                                )
