package com.hery.spark.scala.base

import com.hery.spark.scala.bean.Person
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * @Date 2020/4/26 16:11
 * @Created by hery
 * @Description rdd 和ds进行转换
 */
object RddAndDsTransform {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RddAndDsTransform")
      .master("local")
      .getOrCreate()
    reflected(spark) //

  }

  //  通过编程方式获取df
  def programme(spark: SparkSession): Unit = {
    import spark.implicits._
    // Create an RDD
    val peopleRDD = spark.sparkContext.textFile("/data/spark/people.txt")
    // The schema is encoded in a string,应该是动态通过程序生成的
    val schemaString = "name age"
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))

    val schema = StructType(fields)
    val rowRDD = peopleRDD
      .map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1).trim))

    val peopleDF = spark.createDataFrame(rowRDD, schema)
    peopleDF.show()
    val results = spark.sql("SELECT name FROM people")
    results.map(attributes => "Name: " + attributes(0)).show()
  }

  /**
   * 通过反射方式创建df,前提是需要知道反射的类型，也就是本例中Person类
   *
   * @param spark
   */
  def reflected(spark: SparkSession): Unit = {
    import spark.implicits._

    val peopleDF = spark.sparkContext
      .textFile("/data/spark/people.txt")
      .map(_.split(","))
      .map(line => Person(line(0), line(1).trim.toInt))
      .toDF()


    peopleDF.createOrReplaceTempView("people")
    val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")

    teenagersDF.map(teenager => "Name: " + teenager(0)).show()

    // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
    teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()
  }
}
