package com.hery.spark.scala.datasource

import com.hery.spark.scala.SparkUtil
import org.apache.spark.sql.SparkSession

/**
 * @Date 2020/4/26 18:15
 * @Created by hery
 * @Description Parquet 数据类型
 */
object ParquetType {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.sparkSession()
    import spark.implicits._
    // 从本地读取数据
    val peopleDF = spark.read.json("people.json")
    // 保存到hdfs上
    peopleDF.write.parquet("hdfs://master:9000/people.parquet")

    val parquetFileDF = spark.read.parquet("hdfs://master:9000/people.parquet")

    parquetFileDF.createOrReplaceTempView("people")
    val namesDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19")
    namesDF.map(attributes => "Name: " + attributes(0)).show()

  }

  def schameMearge(): Unit = {
    val spark = SparkUtil.sparkSession()
    import spark.implicits._
    val sc = spark.sparkContext
    // 创建一个简单的df,输出到一个partition中
    val numdf = sc.makeRDD(1 to 5).map(i => (i, i * 2)).toDF("single", "double")
    numdf.write.parquet("hdfs://master:9000/data/test_table/key=1")
    // 再创建一个df,输出到另外一个partition中
    val df2 = sc.makeRDD(6 to 10).map(i => (i, i * 3)).toDF("single", "triple")
    df2.write.parquet("hdfs://master:9000/data/test_table/key=2")
    // 读取分区数据
    val df3 = spark.read.option("mergeSchema", "true").parquet("hdfs://master:9000/data/test_table")
    df3.printSchema()
    // The final schema consists of all 3 columns in the Parquet files together
    // with the partitioning column appeared in the partition directory paths.
    // root
    // |-- single: int (nullable = true)
    // |-- double: int (nullable = true)
    // |-- triple: int (nullable = true)
    // |-- key : int (nullable = true)
  }
}
