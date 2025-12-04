package com.example.spark.project_11

import org.apache.spark.sql.SparkSession

object SparkHDFSWriter {
    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "flink") // 设置为有权限的用户，如你错误信息中的flink用户
        // 1. 创建SparkSession
        val spark = SparkSession.builder()
            .appName("Spark HDFS Writer")
            .master("local[*]") // 本地模式运行，生产环境应去掉这行
            .getOrCreate()

        // 2. 设置HDFS配置（如果core-site.xml和hdfs-site.xml不在classpath中）
        spark.sparkContext.hadoopConfiguration.set("fs.defaultFS", "hdfs://master:8020")

        // 3. 创建示例数据
        val data = Seq(
            ("John", 28, "New York"),
            ("Mary", 32, "Chicago"),
            ("Bob", 45, "Los Angeles")
        )

        // 4. 将数据写入HDFS
        try {
            // 方法1: 使用RDD API写入文本文件
            val rdd = spark.sparkContext.parallelize(data.map(_.toString()))
            rdd.saveAsTextFile("hdfs://master:8020/user/sparkstreaming/output/rdd_output")

            // 方法2: 使用DataFrame API写入多种格式
            import spark.implicits._
            val df = data.toDF("name", "age", "city")

            // 写入Parquet格式
            df.write.parquet("hdfs://master:8020/user/sparkstreaming/output/parquet_output")

            // 写入CSV格式
            df.write
                .option("header", "true")
                .csv("hdfs://master:8020/user/sparkstreaming/output/csv_output")

            println("数据成功写入HDFS")
        } catch {
            case e: Exception =>
                println(s"写入HDFS时出错: ${e.getMessage}")
                e.printStackTrace()
        } finally {
            // 5. 关闭SparkSession
            spark.stop()
        }
    }
}
