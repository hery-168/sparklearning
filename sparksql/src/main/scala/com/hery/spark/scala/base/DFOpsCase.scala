package com.hery.spark.scala.base

import org.apache.spark.sql.SparkSession

/**
 * @Date 2020/4/26 15:43
 * @Created by hery
 * @Description DataFrame的常用操作
 */
object DFOpsCase {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CreateDFCase")
      .master("local")
      .getOrCreate()
    //    dfOpsByDsl(spark)
    dfOpsBySql(spark)


  }

  def dfOpsByDsl(spark: SparkSession): Unit = {
    import spark.implicits._

    val df = spark.read.json("/data/spark/person.json")
    df.show()
    // 数仓schema
    df.printSchema()
    //输出某个字段
    df.select("name").show()
    // 字段上进行操作
    df.select($"name", $"age" + 1).show()
    // 过滤
    df.filter($"age" > 18).show()
    // 聚合操作
    df.groupBy("age").count().show()
  }

  def dfOpsBySql(spark: SparkSession): Unit = {
    import spark.implicits._
    val df = spark.read.json("/data/spark/person.json")
    df.show()
    df.createGlobalTempView("person")
    spark.sql("select name from person").show()
    spark.sql("select name,age+1 from person where age>18").show()
    spark.sql("select count(*) from person group by age").show()
  }
}
