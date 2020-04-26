package com.hery.spark.scala.base

import com.hery.spark.scala.bean.Person
import org.apache.spark.sql.SparkSession

/**
 * @Date 2020/4/26 15:59
 * @Created by hery
 * @Description 创建ds
 *              ds是强类型的
 */
object CreateDSCase {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CreateDFCase")
      .master("local")
      .getOrCreate()
    import spark.implicits._

    val caseClassDS = Seq(Person("Andy", 32)).toDS()
    caseClassDS.show()

    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.map(_ + 1).collect() // Returns: Array(2, 3, 4)
    //读取文件
    val path = "data/spark/people.json"
    val peopleDS = spark.read.json(path).as[Person]
    peopleDS.show()
  }
}

