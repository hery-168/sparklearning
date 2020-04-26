package com.hery.spark.scala.udf

import com.hery.spark.scala.SparkUtil
import com.hery.spark.scala.bean.Employee

/**
 * @Date 2020/4/26 17:45
 * @Created by hery
 * @Description
 */
object MyAvgByAggregatorMain {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.sparkSession()
    import spark.implicits._

    val ds = spark.read.json("data/spark/employees.json").as[Employee]
    ds.show()
    val averageSalary = MyAvgByAggregator.toColumn.name("average_salary")
    val result = ds.select(averageSalary)
    result.show()
  }
}
