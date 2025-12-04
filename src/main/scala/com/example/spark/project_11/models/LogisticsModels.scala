package com.example.spark.project_11.models

import com.example.spark.project_11.models
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import java.sql.Timestamp
import java.text.SimpleDateFormat

// 物流订单事件
case class OrderEvent(
                         orderId: String,
                         customerId: String,
                         warehouseId: String,
                         destination: String,
                         productId: String,
                         quantity: Int,
                         orderTime: Timestamp,
                         expectedDelivery: Timestamp,
                         status: String // "CREATED", "PROCESSING", "SHIPPED", "DELIVERED", "DELAYED"
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

// 车辆GPS数据
case class VehicleGPS(
                         vehicleId: String,
                         driverId: String,
                         latitude: Double,
                         longitude: Double,
                         speed: Double,
                         timestamp: Timestamp,
                         status: String // "IDLE", "LOADING", "IN_TRANSIT", "UNLOADING"
                     )

object VehicleGPS {
    private val mapper = {
        val m = new ObjectMapper()
        m.registerModule(DefaultScalaModule)
        m
    }

    def fromJson(json: String): Option[VehicleGPS] = {
        try {
            Some(mapper.readValue(json, classOf[VehicleGPS]))
        } catch {
            case e: Exception => None
        }
    }
}


// 区域配送统计
case class AreaDeliveryStats(
                                area: String,
                                totalOrders: Long,
                                onTimeDeliveries: Long,
                                delayedDeliveries: Long,
                                avgDeliveryTime: Double,
                                windowStart: Timestamp,
                                windowEnd: Timestamp
                            )

object ModelUtils {
    private val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    def parseTimestamp(timestampStr: String): Timestamp = {
        new Timestamp(dateFormat.parse(timestampStr).getTime)
    }
}



