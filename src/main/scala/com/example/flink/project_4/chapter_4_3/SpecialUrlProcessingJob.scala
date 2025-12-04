package com.example.flink.project_4.chapter_4_3

import com.example.flink.project_4.common.View
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.util.Collector

object SpecialUrlProcessingJob {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        // 设置并行度
        env.setParallelism(1)

        // 模拟数据源 - 实际应用中可能是Kafka、文件等
        val viewStream: DataStream[View] = env.fromElements(
            View("user1", "https://example.com/home", System.currentTimeMillis()),
            View("user2", "https://example.com/admin/dashboard", System.currentTimeMillis()),
            View("user3", "https://example.com/products", System.currentTimeMillis()),
            View("user4", "https://example.com/login", System.currentTimeMillis()),
            View("user5", "https://example.com/secret/data", System.currentTimeMillis()),
            View("user6", "https://example.com/about", System.currentTimeMillis()),
            View("admin1", "https://example.com/administrator/settings", System.currentTimeMillis())
        )

        // 处理数据流，分离特殊URL
        val processedStream = processSpecialUrls(viewStream)

        // 获取侧输出流
        val specialUrlStream: DataStream[View] = processedStream.getSideOutput(SpecialUrlTags.specialUrlTag)
        val adminUrlStream: DataStream[View] = processedStream.getSideOutput(SpecialUrlTags.adminUrlTag)
        val sensitiveUrlStream: DataStream[View] = processedStream.getSideOutput(SpecialUrlTags.sensitiveUrlTag)

        // 主输出流处理 - 普通URL访问统计
        val normalUrlStream = processedStream.filter(_.url.contains("/admin") == false)
            .filter(_.url.contains("/secret") == false)
            .filter(_.url.contains("/login") == false)

        // 输出结果
        normalUrlStream.print("普通URL访问")
        specialUrlStream.print("特殊URL访问")
        adminUrlStream.print("管理员URL访问")
        sensitiveUrlStream.print("敏感URL访问")

        println("程序开始运行...")
        // 执行作业
        env.execute()
    }

    def processSpecialUrls(inputStream: DataStream[View]): DataStream[View] = {
        inputStream
            .process(new UrlProcessFunction)
    }

}


class UrlProcessFunction extends ProcessFunction[View, View] {

    // 定义特殊URL模式
    private val SPECIAL_URL_PATTERNS = List(
        "/admin", "/login", "/secret", "/confidential"
    )

    private val ADMIN_PATTERNS = List("/admin", "/administrator")
    private val SENSITIVE_PATTERNS = List("/secret", "/confidential", "/private")

    override def processElement(
                                   value: View,
                                   ctx: ProcessFunction[View, View]#Context,
                                   out: Collector[View]
                               ): Unit = {

        // 检查是否为特殊URL
        if (isSpecialUrl(value.url)) {
            // 发送到主输出流（可选）
            out.collect(value)

            // 根据URL类型发送到不同的侧输出流
            if (isAdminUrl(value.url)) {
                ctx.output(SpecialUrlTags.adminUrlTag, value)
            } else if (isSensitiveUrl(value.url)) {
                ctx.output(SpecialUrlTags.sensitiveUrlTag, value)
            }

            // 同时也发送到通用的特殊URL侧输出流
            ctx.output(SpecialUrlTags.specialUrlTag, value)
        } else {
            // 普通URL只发送到主输出流
            out.collect(value)
        }
    }

    private def isSpecialUrl(url: String): Boolean = {
        SPECIAL_URL_PATTERNS.exists(pattern => url.contains(pattern))
    }

    private def isAdminUrl(url: String): Boolean = {
        ADMIN_PATTERNS.exists(pattern => url.contains(pattern))
    }

    private def isSensitiveUrl(url: String): Boolean = {
        SENSITIVE_PATTERNS.exists(pattern => url.contains(pattern))
    }
}