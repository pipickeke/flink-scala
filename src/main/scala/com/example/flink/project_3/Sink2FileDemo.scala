package com.example.flink.project_3

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.connector.jdbc.JdbcExecutionOptions
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object Sink2FileDemo {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val dataStream: DataStream[View] = env.fromCollection(
            List(
                View("Tom", "/index.html", 1000L),
                View("Mike", "/home.html", 2000L),
                View("Bob", "/car.html", 4000L),
            )
        )

        writeToFileSystem(dataStream)

    }

    def writeToFileSystem(dataStream: DataStream[View]): Unit = {

        // 转为字符串输出
        val textStream = dataStream.map(view => s"User: ${view.user}, URL: ${view.url}, Time: ${view.timestamp}")

        val outputPath = "file:///tmp/flink-sink-output" // 本地路径

        textStream.writeAsText(outputPath.substring("file://".length))
            .setParallelism(1)
            .name("FileSystemSink")
    }

    import org.apache.flink.connector.jdbc.JdbcConnectionOptions
    import org.apache.flink.connector.jdbc.JdbcSink
    import java.sql.PreparedStatement

    def writeToMySQL(dataStream: DataStream[View]): Unit = {
        val jdbcUrl = "jdbc:mysql://localhost:3306/test_db?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true"
        val username = "root"
        val password = "xxxx"

        val sink = JdbcSink.sink[View](
            // SQL 插入语句
            "INSERT INTO views (user, url, ts) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE url=VALUES(url), ts=VALUES(ts)",
            (ps: PreparedStatement, view: View) => {
                ps.setString(1, view.user)
                ps.setString(2, view.url)
                ps.setLong(3, view.timestamp)
            },
            // JDBC 连接参数
            JdbcExecutionOptions.builder()
                .withBatchSize(1000)
                .withBatchIntervalMs(200)
                .withMaxRetries(3)
                .build(),
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(jdbcUrl)
                .withDriverName("com.mysql.cj.jdbc.Driver")
                .withUsername(username)
                .withPassword(password)
                .build()
        )
        dataStream.addSink(sink).name("MySQLSink")
        println("数据将写入 MySQL 数据库（views 表）")
    }
}
