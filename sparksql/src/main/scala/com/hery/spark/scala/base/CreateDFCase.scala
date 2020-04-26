package com.hery.spark.scala.base

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
 * @Date 2020/4/26 15:29
 * @Created by hery
 * @Description DataFrame 的创建方式
 */
object CreateDFCase {

  // 通过外部数据源创建
  def createBySource() = {
    val spark = SparkSession.builder()
      .appName("CreateDFCase")
      .master("local")
      .getOrCreate()
    import spark.implicits._
    val df = spark.read.json("/data/spark/person.json")
    df.show()
  }

  // 通过rdd创建
  def createByRDD(): Unit = {
    val spark = SparkSession.builder()
      .appName("CreateDFCase")
      .master("local")
      .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    val personRDD = sc.textFile("data/spark/person.txt")
    val personDF = personRDD.map(_.split(","))
      .map(line => (line(0), line(1).trim().toInt)).toDF("name", "age")
    personDF.show()
  }

  def main(args: Array[String]): Unit = {
    //    createBySource()
  }
}
