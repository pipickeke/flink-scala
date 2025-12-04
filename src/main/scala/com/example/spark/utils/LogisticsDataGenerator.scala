package com.example.spark.utils

import com.example.spark.project_11.models.VehicleGPS
import com.example.spark.project_11.models
import com.example.spark.project_11.models.OrderEvent
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties, Random, UUID}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object LogisticsDataGenerator {
    private val random = new Random()
    private val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    private val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    // 模拟数据
    private val warehouses = Array("WH_NYC", "WH_LA", "WH_CHI", "WH_HOU", "WH_MIA")
    private val destinations = Array("East", "West", "North", "South", "Central")
    private val products = Array("P001", "P002", "P003", "P004", "P005")
    private val orderStatuses = Array("CREATED", "PROCESSING", "SHIPPED", "DELIVERED", "DELAYED")
    private val vehicleStatuses = Array("IDLE", "LOADING", "IN_TRANSIT", "UNLOADING")
    private val vehicleIds = (1 to 20).map(i => s"VH${"%03d".format(i)}").toArray
    private val driverIds = (1 to 50).map(i => s"DR${"%03d".format(i)}").toArray

    def main(args: Array[String]): Unit = {
        val props = new Properties()
        props.put("bootstrap.servers", "master:9092")
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

        val orderProducer = new KafkaProducer[String, String](props)
        val vehicleProducer = new KafkaProducer[String, String](props)

        // 持续生成数据
        while (true) {
            // 生成订单事件
            val orderEvent = generateOrderEvent()
            val orderJson = mapper.writeValueAsString(orderEvent)
            orderProducer.send(new ProducerRecord[String, String]("order-events", orderJson))

            // 生成车辆GPS数据
            val vehicleGPS = generateVehicleGPS()
            val vehicleJson = mapper.writeValueAsString(vehicleGPS)
            vehicleProducer.send(new ProducerRecord[String, String]("vehicle-gps", vehicleJson))

            Thread.sleep(random.nextInt(1000) + 500) // 0.5-1.5秒间隔
        }
    }

    private def generateOrderEvent(): OrderEvent = {
        val calendar = Calendar.getInstance()
        val orderTime = calendar.getTime
        calendar.add(Calendar.HOUR, random.nextInt(72) + 24) // 预计1-3天后送达
        val expectedDelivery = calendar.getTime

        models.OrderEvent(
            orderId = UUID.randomUUID().toString,
            customerId = s"CUST${random.nextInt(1000)}",
            warehouseId = warehouses(random.nextInt(warehouses.length)),
            destination = destinations(random.nextInt(destinations.length)),
            productId = products(random.nextInt(products.length)),
            quantity = random.nextInt(10) + 1,
            orderTime = new java.sql.Timestamp(orderTime.getTime),
            expectedDelivery = new java.sql.Timestamp(expectedDelivery.getTime),
            status = orderStatuses(random.nextInt(orderStatuses.length))
        )
    }

    private def generateVehicleGPS(): VehicleGPS = {
        VehicleGPS(
            vehicleId = vehicleIds(random.nextInt(vehicleIds.length)),
            driverId = driverIds(random.nextInt(driverIds.length)),
            latitude = 30 + random.nextDouble() * 20, // 模拟美国大致纬度范围
            longitude = -120 + random.nextDouble() * 60, // 模拟美国大致经度范围
            speed = random.nextInt(80) + 10, // 10-90 mph
            timestamp = new java.sql.Timestamp(System.currentTimeMillis()),
            status = vehicleStatuses(random.nextInt(vehicleStatuses.length))
        )
    }
}
