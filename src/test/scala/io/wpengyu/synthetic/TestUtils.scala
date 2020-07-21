package io.wpengyu.synthetic

import java.math.BigInteger
import java.math.BigDecimal
import java.sql.Date
import java.text.SimpleDateFormat

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object TestUtils {

  var spark: SparkSession = null

  def createDate(dateStr:String): Date = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    new Date(sdf.parse(dateStr).getTime)
  }

  def initSpark(): Unit = {
    println("--- Init Spark ---")
    val conf = new SparkConf().setAppName("Spark 2.0 Test")
    spark = SparkSession.builder.config(conf).config("spark.driver.bindAddress", "localhost").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.sparkContext.hadoopConfiguration
      .set("mapreduce.fileoutputcomitter.marksuccessfuljobs", "false")
  }

  def terminateSpark(): Unit = {
    println("--- Terminate Spark ---")
    if (spark != null) {
      spark.stop()
    }
  }

}
