package com.hery.spark.scala.datasource

import com.hery.spark.scala.{Constants, SparkUtil}

/**
 * @Date 2020/4/27 14:02
 * @Created by hery
 * @Description json 数据源
 */
object JsonSourceCaSE {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.sparkSession();
    import spark.implicits._
    val personDF = spark.read.json(Constants.personJsonPath)
    // 输出schema
    personDF.printSchema()
    personDF.createGlobalTempView("person")
    spark.sql("select * from person").show()
  }
}
