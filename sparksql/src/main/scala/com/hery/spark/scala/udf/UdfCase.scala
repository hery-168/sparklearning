package com.hery.spark.scala.udf

import com.hery.spark.scala.{Constants, SparkUtil}
import org.apache.spark.sql.SparkSession

/**
 * @Date 2020/4/26 16:40
 * @Created by hery
 * @Description 自定义函数
 */
object UdfCase {
  def main(args: Array[String]): Unit = {
   val  spark = SparkUtil.sparkSession()
    import spark.implicits._
    val df = spark.read.json(Constants.personJsonPath)
    df.show()
    // 注册自定义函数
    spark.udf.register("addName", (x: String) => "Name:" + x)

    df.createOrReplaceTempView("people")
    spark.sql("Select addName(name), age from people").show()
    spark.close()
  }
}
