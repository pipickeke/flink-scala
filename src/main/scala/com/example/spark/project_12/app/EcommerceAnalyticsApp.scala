package com.example.spark.project_12.app

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.functions._
import com.example.spark.project_12.models._
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object EcommerceAnalyticsApp {
    private val logger = LogManager.getLogger(getClass)

    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "flink")
        // 初始化Spark
        val spark = SparkSession.builder()
            .appName("EcommerceAnalytics")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "3")
            .config("spark.streaming.backpressure.enabled", "true")
            .config("spark.streaming.kafka.maxRatePerPartition", "100")
            .getOrCreate()

        val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

        import spark.implicits._

        // Kafka配置
        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> "master:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "ecommerce-analytics-group",
            "auto.offset.reset" -> "latest",
            "enable.auto.commit" -> (false: java.lang.Boolean)
        )

        val topics = Array("user-events", "order-events")
        val streams = KafkaUtils.createDirectStream[String, String](
            ssc,
            PreferConsistent,
            Subscribe[String, String](topics, kafkaParams)
        )

        // 1. 处理用户行为事件
        val userEvents = streams.filter(_.topic() == "user-events").map(_.value())
            .flatMap(UserEvent.fromJson)

        // 2. 处理订单事件
        val orderEvents = streams.filter(_.topic() == "order-events").map(_.value())
            .flatMap(OrderEvent.fromJson)

        // 3. 实时商品统计（输出到控制台和文件系统）
        // 3. 简化版实时商品统计
        userEvents.foreachRDD { rdd =>
            if (!rdd.isEmpty()) {
                // 直接转换为DataFrame并计算统计
                val stats = rdd.toDF()
                    .filter(col("productId").isNotNull)
                    .groupBy("productId", "eventType")
                    .count()
                    .groupBy("productId")
                    .pivot("eventType")
                    .sum("count")
                    .na.fill(0)
                    .withColumn("timestamp", current_timestamp())

                // 控制台输出
                println("商品实时统计:")
                stats.show(10, truncate = false)

                // 保存到文件系统
                stats.write
                    .mode("append")
                    .parquet("hdfs://master:8020/ecommerce/product_stats")
            }
        }

        // 4. 用户会话分析（保存到文件系统）
        userEvents.map(e => (e.sessionId, e.userId, e.eventTime.getTime))
            .foreachRDD { rdd =>
                if (!rdd.isEmpty()) {
                    val sessions = rdd.toDF("sessionId", "userId", "eventTime")
                        .groupBy("sessionId", "userId")
                        .agg(
                            min("eventTime").as("startTime"),
                            max("eventTime").as("endTime"),
                            count("*").as("eventCount")
                        )
                        .withColumn("duration", ($"endTime" - $"startTime") / 1000)

                    sessions.write
                        .mode("append")
                        .parquet("hdfs://master:8020/ecommerce/session_analysis")
                }
            }

        // 5. 异常交易检测（输出到控制台和文件系统）
        orderEvents.map(o => (o.userId, o.totalPrice, o.orderTime.getTime))
            .foreachRDD { rdd =>
                if (!rdd.isEmpty()) {
                    val orders = rdd.toDF("userId", "amount", "orderTime")

                    // 检测高价值交易
                    val highValue = orders.filter($"amount" > 1000)
                        .withColumn("orderId", lit(null).cast(StringType))
                        .withColumn("reason", lit("high_value"))
                        .select("orderId", "userId", "amount", "orderTime", "reason")

                    // 检测高频交易
                    val highFrequency = orders.groupBy("userId")
                        .agg(
                            count("*").as("orderCount"),
                            min("orderTime").as("firstOrderTime"),
                            max("orderTime").as("lastOrderTime")
                        )
                        .filter($"orderCount" > 3 && ($"lastOrderTime" - $"firstOrderTime") < 3600000)
                        .withColumn("orderId", lit(null).cast(StringType))
                        .withColumn("amount", lit(null).cast(DoubleType))
                        .withColumn("reason", lit("high_frequency"))
                        .select("orderId", "userId", "amount", "lastOrderTime", "reason")

                    // 检测异常时间交易
                    val unusualHour = orders
                        .withColumn("hour", hour(from_unixtime($"orderTime" / 1000)))
                        .filter($"hour" >= 2 && $"hour" <= 5)
                        .withColumn("orderId", lit(null).cast(StringType))
                        .withColumn("reason", lit("unusual_hour"))
                        .select("orderId", "userId", "amount", "orderTime", "reason")

                    // 合并所有异常交易
                    val fraudulentTransactions = highValue.union(highFrequency).union(unusualHour)

                    // 保存到文件系统
                    fraudulentTransactions.write
                        .mode("append")
                        .parquet("hdfs://master:8020/ecommerce/fraudulent_transactions")

                    // 输出到控制台报警
                    if (!fraudulentTransactions.isEmpty) {
                        logger.warn("Fraudulent transactions detected:")
                        fraudulentTransactions.show()
                    }
                }
            }

        // 6. 实时热门商品排名（保存到文件系统）
        userEvents.filter(e => e.eventType == "view" || e.eventType == "purchase")
            .map(e => (e.productId.getOrElse(""), 1))
            .reduceByKey(_ + _)
            .transform(rdd => rdd.sortBy(_._2, false))
            .foreachRDD { rdd =>
                if (!rdd.isEmpty()) {
                    val topProducts = rdd.toDF("productId", "count")

                    // 保存热门商品排名
                    topProducts.write
                        .mode("overwrite")
                        .parquet("hdfs://master:8020/ecommerce/top_products")

                    // 输出到控制台
                    logger.info("Top Products:")
                    topProducts.show(10)
                }
            }

        ssc.start()
        ssc.awaitTermination()
    }
}
