package com.example.flink.project_6
import org.apache.flink.api.common.eventtime.{WatermarkStrategy, SerializableTimestampAssigner}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.pattern.conditions.SimpleCondition
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import java.time.Duration
import java.util

object LoginFailDetectionCEP {
    // 定义登录事件样例类
    case class LoginEvent(userId: String, ip: String, status: String, eventTime: Long)

    // 定义告警信息样例类
    case class LoginWarning(userId: String, firstFailTime: Long, lastFailTime: Long, warningMsg: String)

    def main(args: Array[String]): Unit = {
        // 1. 创建流执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 使用事件时间
        env.getConfig.setAutoWatermarkInterval(1000L)

        // 2. 创建模拟登录事件数据源
        val loginEventStream = env.fromElements(
            LoginEvent("user_1", "192.168.0.1", "fail", 1000L),
            LoginEvent("user_1", "192.168.0.2", "fail", 2000L),
            LoginEvent("user_2", "192.168.1.1", "success", 3000L),
            LoginEvent("user_1", "192.168.0.3", "fail", 4000L),  // user_1 第三次失败，应触发告警
            LoginEvent("user_2", "192.168.1.2", "fail", 5000L),
            LoginEvent("user_2", "192.168.1.3", "fail", 6000L),
            LoginEvent("user_2", "192.168.1.4", "fail", 7000L),  // user_2 第三次失败，应触发告警
            LoginEvent("user_1", "192.168.0.4", "success", 8000L)
        )
            // 分配时间戳和水位线
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner(new SerializableTimestampAssigner[LoginEvent] {
                        override def extractTimestamp(element: LoginEvent, recordTimestamp: Long): Long = element.eventTime
                    })
            )

        // 3. 定义CEP模式：2分钟内连续3次登录失败
        val loginFailPattern = Pattern
            .begin[LoginEvent]("first")  // 第一次失败
            .where(new SimpleCondition[LoginEvent] {
                override def filter(event: LoginEvent): Boolean = event.status == "fail"
            })
            .next("second")  // 紧接着第二次失败
            .where(new SimpleCondition[LoginEvent] {
                override def filter(event: LoginEvent): Boolean = event.status == "fail"
            })
            .next("third")  // 紧接着第三次失败
            .where(new SimpleCondition[LoginEvent] {
                override def filter(event: LoginEvent): Boolean = event.status == "fail"
            })
            .within(Time.minutes(2))  // 在2分钟内完成三次失败

        // 4. 将模式应用到数据流上，按用户ID分组
        val patternStream: PatternStream[LoginEvent] = CEP.pattern(
            loginEventStream.keyBy(_.userId),
            loginFailPattern
        )

        // 5. 检测匹配事件并输出告警
        val loginFailWarningStream = patternStream.select(
            (pattern: util.Map[String, util.List[LoginEvent]]) => {
                // 从模式中提取三次失败事件
                val firstFail = pattern.get("first").get(0)
                val secondFail = pattern.get("second").get(0)
                val thirdFail = pattern.get("third").get(0)

                LoginWarning(
                    thirdFail.userId,
                    firstFail.eventTime,
                    thirdFail.eventTime,
                    s"User ${thirdFail.userId} failed to login 3 times in 2 minutes. " +
                        s"IPs: ${firstFail.ip}, ${secondFail.ip}, ${thirdFail.ip}"
                )
            }
        )

        // 6. 打印结果
        loginFailWarningStream.print("Warning")

        // 7. 执行程序
        env.execute("Login Fail Detection with CEP")
    }
}