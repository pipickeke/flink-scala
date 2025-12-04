package com.example.flink.project_4.common

case class UserBehaviorAnalysis(userId: String, windowStart: Long, windowEnd: Long,
                                visitCount: Int, pages: String, detailInfo: String)
