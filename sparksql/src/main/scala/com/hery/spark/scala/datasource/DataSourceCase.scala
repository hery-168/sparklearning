package com.hery.spark.scala.datasource

import com.hery.spark.scala.SparkUtil
import org.apache.spark.sql.SaveMode

/**
 * @Date 2020/4/26 17:58
 * @Created by hery
 * @Description 操作各种数据源
 */
object DataSourceCase {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.sparkSession()
    import spark.implicits._
    //默认是parquet格式
    val df = spark.read.load("users.parquet")

    df.select("name", "age").write.save("namesAndAges.parquet")
    // 非parquet格式,json,parquet,jdbc,orc,csv,text
    val jsondf = spark.read.format("json").load("users.json")
    jsondf.select("name").write.mode(SaveMode.Append).format("json").save("jsondata.json")

  }
}
