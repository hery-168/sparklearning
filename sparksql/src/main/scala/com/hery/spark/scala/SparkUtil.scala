package com.hery.spark.scala

import org.apache.spark.sql.SparkSession

/**
 * @Date 2020/4/26 17:18
 * @Created by hery
 * @Description
 */
object SparkUtil {
  def sparkSession(): SparkSession = {
    val spark = SparkSession.builder()
      .appName("Spark App")
      .master("local")
      .getOrCreate()
    spark
  }

}
