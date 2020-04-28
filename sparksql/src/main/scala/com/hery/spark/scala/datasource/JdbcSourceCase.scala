package com.hery.spark.scala.datasource

import java.util.Properties

import com.hery.spark.scala.SparkUtil

/**
 * @Date 2020/4/27 14:07
 * @Created by hery
 * @Description spark 操作jdbc
 */
object JdbcSourceCase {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.sparkSession()
    //加载数据
    val jdbcDF = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://master:3306/spark") // mysql url
      .option("dbtable", " test_spark") // mysql table
      .option("user", "root") // 用户名
      .option("password", "root").load() // 秘密

    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "root")
    val jdbcDF2 = spark.read
      .jdbc("jdbc:mysql://master:3306/spark", "test_spark", properties)

    // 保存数据到mysql
    jdbcDF.write
      .format("jdbc")
      .option("url", "jdbc:mysql://master:3306/spark")
      .option("dbtable", "test_spark2")
      .option("user", "root")
      .option("password", "root")
      .save()

    jdbcDF2.write
      .jdbc("jdbc:mysql://master:3306/mysql", "test_spark2", properties)
  }
}
