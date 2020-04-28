package com.hery.spark.scala.datasource

import java.io.File

import com.hery.spark.scala.SparkUtil
import org.apache.spark.sql.{Row, SparkSession}

/**
 * @Date 2020/4/26 18:34
 * @Created by hery
 * @Description 通过spark操作hive
 */
object HiveCase {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark App")
      .master("local")
      //      .config("spark.sql.warehouse.dir","")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    // 创建表
    spark.sql("CREATE TABLE IF NOT EXISTS user (name STRING, age INT)")
    // 导入数据
    spark.sql("LOAD DATA LOCAL INPATH 'user.txt' INTO TABLE user")
    // 查询数据
    spark.sql("SELECT * FROM user").show()
    val resultDF = spark.sql("select name,age from user where age >18;")
    val resultDS = resultDF.map {
      case Row(name: String, age: Int) => s"Key: $name, Value: $age"
    }

    resultDS.show()
  }
}
