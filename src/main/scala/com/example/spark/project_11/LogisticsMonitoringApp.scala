package com.example.spark.project_11

import com.example.spark.project_11.models.{OrderEvent, VehicleGPS}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.logging.log4j.LogManager

object LogisticsMonitoringApp {
    private val logger = LogManager.getLogger(getClass)

    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "flink")
        // 初始化SparkSession和StreamingContext
        val spark = SparkSession.builder()
            .appName("LogisticsMonitoring")
            .master("local[*]") // 生产环境去掉这个配置，使用spark-submit指定
            .config("spark.sql.shuffle.partitions", "3")
            .config("spark.streaming.backpressure.enabled", "true")
            .config("spark.streaming.kafka.maxRatePerPartition", "100")
            .getOrCreate()

        val ssc = new StreamingContext(spark.sparkContext, Seconds(10)) // 10秒的批处理间隔

        import spark.implicits._

        // Kafka配置
        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> "master:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "logistics-monitoring-group",
            "auto.offset.reset" -> "latest",
            "enable.auto.commit" -> (false: java.lang.Boolean)
        )

        val topics = Array("order-events", "vehicle-gps")
        val streams = KafkaUtils.createDirectStream[String, String](
            ssc,
            PreferConsistent,
            Subscribe[String, String](topics, kafkaParams)
        )

        // 创建JSON解析器
        val mapper = new ObjectMapper()
        mapper.registerModule(DefaultScalaModule)

        // 处理订单事件流
        val orderEvents = streams.filter(_.topic() == "order-events").map(_.value())
            .flatMap(OrderEvent.fromJson)

        // 处理车辆GPS流
        val vehicleGPS = streams.filter(_.topic() == "vehicle-gps").map(_.value())
            .flatMap(VehicleGPS.fromJson)

        // 1. 实时订单状态统计 (输出到控制台和文件系统)
        orderEvents.foreachRDD { rdd =>
            if (!rdd.isEmpty()) {
                val ordersDF = rdd.toDF()

                // 按状态统计订单数
                val statusStats = ordersDF.groupBy("status").count()

                // 输出到控制台
                logger.info("Order Status Statistics:")
                statusStats.show()

                // 保存到文件系统 (Parquet格式)
                statusStats.write
                    .mode("append")
                    .parquet("hdfs://master:8020/logistics/order_status_stats")
            }
        }

        // 2. 车辆状态监控 (输出到控制台和文件系统)
        vehicleGPS.foreachRDD { rdd =>
            if (!rdd.isEmpty()) {
                val vehiclesDF = rdd.toDF()

                // 计算每辆车的平均速度和最新位置
                val vehicleStats = vehiclesDF.groupBy("vehicleId", "driverId", "status")
                    .agg(
                        avg("speed").as("avgSpeed"),
                        last("latitude").as("lastLatitude"),
                        last("longitude").as("lastLongitude"),
                        current_timestamp().as("updateTime")
                    )

                // 输出到控制台
                logger.info("Vehicle Status Statistics:")
                vehicleStats.show()

                // 保存到文件系统 (Parquet格式)
                vehicleStats.write
                    .mode("overwrite")
                    .parquet("hdfs://master:8020/logistics/vehicle_status")
            }
        }

        // 3. 区域配送时效分析 (输出到控制台和文件系统)
        orderEvents.map { order =>
            val deliveryTime = if (order.status == "DELIVERED") {
                Some((order.expectedDelivery.getTime - order.orderTime.getTime) / (1000 * 60 * 60)) // 小时
            } else None
            (order.destination, deliveryTime)
        }.foreachRDD { rdd =>
            if (!rdd.isEmpty()) {
                val statsDF = rdd.toDF("area", "deliveryTime")
                    .filter($"deliveryTime".isNotNull)
                    .groupBy("area")
                    .agg(
                        count("*").as("totalDeliveries"),
                        avg("deliveryTime").as("avgDeliveryTimeHours"),
                        current_timestamp().as("updateTime")
                    )

                // 输出到控制台
                logger.info("Area Delivery Statistics:")
                statsDF.show()

                // 保存到文件系统 (Parquet格式)
                statsDF.write
                    .mode("append")
                    .parquet("hdfs://master:8020/logistics/area_delivery_stats")
            }
        }

        // 4. 延迟订单预警 (输出到控制台和文件系统)
        orderEvents.filter(_.status == "DELAYED").foreachRDD { rdd =>
            if (!rdd.isEmpty()) {
                val delayedOrdersDF = rdd.toDF()
                    .select("orderId", "customerId", "expectedDelivery", "status")
                    .withColumn("alertTime", current_timestamp())

                // 输出到控制台
                logger.warn("Delayed Orders Alert:")
                delayedOrdersDF.show()

                // 保存到文件系统 (Parquet格式)
                delayedOrdersDF.write
                    .mode("append")
                    .parquet("hdfs://master:8020/logistics/delayed_orders_alert")
            }
        }

        ssc.start()
        ssc.awaitTermination()
    }
}
