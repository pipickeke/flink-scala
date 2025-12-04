package com.example.flink.model

import java.sql.Timestamp

// 订单创建事件
// 物流事件类型
sealed trait LogisticsEvent {
    def orderId: String
    def timestamp: Long
}

case class OrderCreated(
                           orderId: String,
                           customerId: String,
                           origin: String, // 发货地
                           destination: String, // 目的地
                           goodsType: String, // 货物类型
                           weight: Double, // 重量(kg)
                           timestamp: Long
                       ) extends LogisticsEvent

// 运输状态更新事件
case class StatusUpdated(
                            orderId: String,
                            status: String,      // 运输状态
                            location: String,    // 当前位置
                            updateTime: Long,    // 状态更新时间
                            timestamp: Long = System.currentTimeMillis()
                        ) extends LogisticsEvent

// 订单完成事件
case class OrderCompleted(
                             orderId: String,
                             deliveryTime: Long,  // 送达时间
                             timestamp: Long = System.currentTimeMillis()
                         ) extends LogisticsEvent

// 地区订单统计结果
case class RegionOrderStats(
                               region: String,
                               windowStart: Timestamp,
                               windowEnd: Timestamp,
                               orderCount: Long,
                               processingCount: Long,
                               completedCount: Long
                           )

// 异常订单预警
case class AbnormalOrderAlert(
                                 orderId: String,
                                 lastStatus: String,
                                 lastLocation: String,
                                 lastUpdateTime: Timestamp,
                                 duration: Long  // 未更新时间(小时)
                             )