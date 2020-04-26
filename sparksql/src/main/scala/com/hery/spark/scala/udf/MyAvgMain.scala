package com.hery.spark.scala.udf

import com.hery.spark.scala.{Constants, SparkUtil}

/**
 * @Date 2020/4/26 17:34
 * @Created by hery
 * @Description 自定义函数
 */
object MyAvgMain {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.sparkSession()
    import spark.implicits._
    val df = spark.read.json(Constants.personJsonPath)
    // 注册函数
    spark.udf.register("myAverage", MyAvg)

    df.createOrReplaceTempView("person")
    df.show()
    val result = spark.sql("SELECT myAverage(age) as average_age FROM person")
    result.show()
  }
}
