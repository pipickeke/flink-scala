package com.example.spark.utils

import com.example.spark.project_12.models.{OrderEvent, UserEvent}

import java.sql.Timestamp
import java.util.{Properties, Random, UUID}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write

object EcommerceDataGenerator {
    private val random = new Random()
    private implicit val formats: Formats = DefaultFormats

    // 模拟数据
    private val products = (1 to 100).map(i => s"P${"%03d".format(i)}").toArray
    private val categories = Array("Electronics", "Clothing", "Home", "Books", "Sports")
    private val users = (1 to 1000).map(i => s"U${"%04d".format(i)}").toArray
    private val paymentMethods = Array("Credit Card", "PayPal", "Alipay", "WeChat Pay")
    private val devices = Array("Mobile", "Desktop", "Tablet")
    private val eventTypes = Array("view", "click", "add_to_cart", "purchase", "search")

    def main(args: Array[String]): Unit = {
        val props = new Properties()
        props.put("bootstrap.servers", "master:9092")
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

        val userEventProducer = new KafkaProducer[String, String](props)
        val orderEventProducer = new KafkaProducer[String, String](props)

        // 持续生成数据
        while (true) {
            // 生成用户行为事件
            val userEvent = generateUserEvent()
            val userEventJson = write(userEvent)
            userEventProducer.send(new ProducerRecord[String, String]("user-events", userEventJson))

            // 有10%的概率生成订单事件
            if (random.nextInt(10) == 0) {
                val orderEvent = generateOrderEvent(userEvent.userId)
                val orderEventJson = write(orderEvent)
                orderEventProducer.send(new ProducerRecord[String, String]("order-events", orderEventJson))
            }

            Thread.sleep(random.nextInt(500) + 100) // 0.1-0.6秒间隔
        }
    }

    private def generateUserEvent(): UserEvent = {
        val eventType = eventTypes(random.nextInt(eventTypes.length))
        val hasProduct = eventType != "search"

        UserEvent(
            userId = users(random.nextInt(users.length)),
            sessionId = UUID.randomUUID().toString,
            eventType = eventType,
            productId = if (hasProduct) Some(products(random.nextInt(products.length))) else None,
            categoryId = if (hasProduct) Some(categories(random.nextInt(categories.length))) else None,
            price = if (hasProduct) Some(random.nextDouble() * 1000) else None,
            quantity = if (eventType == "add_to_cart") Some(random.nextInt(5) + 1) else None,
            searchQuery = if (eventType == "search") Some(generateSearchQuery()) else None,
            eventTime = new Timestamp(System.currentTimeMillis()),
            ipAddress = s"${random.nextInt(256)}.${random.nextInt(256)}.${random.nextInt(256)}.${random.nextInt(256)}",
            deviceType = devices(random.nextInt(devices.length)))
    }

    private def generateOrderEvent(userId: String): OrderEvent = {
        val productId = products(random.nextInt(products.length))
        val quantity = random.nextInt(5) + 1
        val unitPrice = random.nextDouble() * 1000

        OrderEvent(
            orderId = UUID.randomUUID().toString,
            userId = userId,
            productId = productId,
            quantity = quantity,
            unitPrice = unitPrice,
            totalPrice = unitPrice * quantity,
            paymentMethod = paymentMethods(random.nextInt(paymentMethods.length)),
            orderTime = new Timestamp(System.currentTimeMillis()),
            status = "created")
    }

    private def generateSearchQuery(): String = {
        val searchTerms = Array("laptop", "phone", "shirt", "book", "game", "camera", "shoes", "bag", "watch", "headphones")
        searchTerms(random.nextInt(searchTerms.length))
    }
}
