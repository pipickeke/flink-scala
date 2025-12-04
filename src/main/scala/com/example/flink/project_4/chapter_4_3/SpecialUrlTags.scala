package com.example.flink.project_4.chapter_4_3

import com.example.flink.project_4.common.View
import org.apache.flink.streaming.api.scala.{OutputTag, createTypeInformation}

object SpecialUrlTags {

    // 定义特殊URL的侧输出流标签
    val specialUrlTag: OutputTag[View] = OutputTag[View]("special-url-views")

    // 可以定义多个不同的特殊URL类型
    val adminUrlTag: OutputTag[View] = OutputTag[View]("admin-urls")
    val sensitiveUrlTag: OutputTag[View] = OutputTag[View]("sensitive-urls")

}
