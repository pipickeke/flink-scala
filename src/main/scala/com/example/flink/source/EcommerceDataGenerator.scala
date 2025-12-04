package com.example.flink.source

import java.util.{Random, UUID}
import com.example.flink.model.UserBehavior
import org.apache.flink.streaming.api.functions.source.SourceFunction

class EcommerceDataGenerator extends SourceFunction[UserBehavior] {
    private var isRunning = true
    private val random = new Random()
    private val behaviorTypes = Seq("pv", "buy", "cart", "fav")

    override def run(ctx: SourceFunction.SourceContext[UserBehavior]): Unit = {
        val maxUserId = 1000
        val maxItemId = 10000
        val maxCategoryId = 20

        while (isRunning) {
            // 生成随机用户行为数据
            val userId = random.nextInt(maxUserId).toLong
            val itemId = random.nextInt(maxItemId).toLong
            val categoryId = random.nextInt(maxCategoryId)
            val behavior = behaviorTypes(random.nextInt(behaviorTypes.size))
            val timestamp = System.currentTimeMillis() / 1000

            ctx.collect(UserBehavior(userId, itemId, categoryId, behavior, timestamp))

            // 控制生成速度
            Thread.sleep(random.nextInt(10))
        }
    }

    override def cancel(): Unit = {
        isRunning = false
    }
}