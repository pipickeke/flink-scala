package com.example.flink.model

import java.sql.Timestamp

// 用户行为数据模型
case class UserBehavior(
                           userId: Long,        // 用户ID
                           itemId: Long,        // 商品ID
                           categoryId: Int,     // 商品类别ID
                           behavior: String,    // 用户行为类型(pv:点击, buy:购买, cart:加购物车, fav:收藏)
                           timestamp: Long      // 行为发生的时间戳(秒)
                       ) {
    // 转换为Timestamp类型
    def eventTime: Timestamp = new Timestamp(timestamp * 1000)
}

// 商品点击量统计结果
case class ItemViewCount(
                            itemId: Long,        // 商品ID
                            windowEnd: Long,     // 窗口结束时间
                            count: Long          // 点击量
                        )