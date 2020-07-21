package io.wpengyu.synthetic.equity

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar
import java.math.{BigDecimal, RoundingMode}

import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable.ListBuffer

object MarketGenerator {

  val DAY_START_TIME_FORMAT = "09:30:00.000"
  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      println("Usage: MarketGenerator <date:YYYY-MM-DD> <seed file>")
      System.exit(1)
    }

    val procDate = args(0)
    val seedLocation = args(1)
    val outputLocation = args(2)
    val exchangeListStr = args(3)
    val unitVolume = args(4)

    val spark = SparkSession.builder
      .appName("MarketGenerator")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse/")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val seed = spark.read
      .option("header", "true")
      .csv(seedLocation)
      .as[Seed]

    val baseline = convertSeedToBaseline(spark, seed, exchangeListStr.split(","), unitVolume.toInt)
    val data = generate(spark, baseline, procDate)

//    data.write.format("com.databricks.spark.csv")
//      .option("delimiter", ",")
//      .mode("overwrite")
//      .save(outputLocation)

    data.write.mode("overwrite").parquet(outputLocation)

    // Write out the symbol metadata
    seed.selectExpr("symbol", "concat(symbol, ' Corporation'")
      .write.mode("overwrite").parquet(outputLocation + "_meta")

  }

  def convertSeedToBaseline(spark:SparkSession, seed:Dataset[Seed], exchangeList:Array[String], volume:Int):Dataset[Baseline] = {
    import spark.implicits._
    val baseline = seed.flatMap(v => {
      val baseList = new ListBuffer[Baseline]
      exchangeList.foreach(ex => {
        baseList.append(new Baseline(v.symbol, ex, volume, BigDecimal.valueOf(v.source_pr.toDouble)))
      })
      baseList.toList
    })
    baseline
  }

  def generate(spark:SparkSession, baseline:Dataset[Baseline], date:String):Dataset[Trade] = {
    import spark.implicits._

    val startTimeStr = "%s %s".format(date, DAY_START_TIME_FORMAT)
    println(startTimeStr)

    //val partitions = Math.min(baseline.count, 3000)
    val population = baseline.repartition(1).flatMap(v => {

      // Get the seed data
      val exchange = v.exchange
      val volume = v.volume
      val sourcePrice = v.source_pr
      val symbol = v.symbol

      // Prepare output buffer
      val quoteList = new ListBuffer[Trade]

      // Init calendar
      val calendar = Calendar.getInstance
      val sdfTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
      val sdfDate = new SimpleDateFormat("yyyy-MM-dd")
      val startTime = sdfTime.parse(startTimeStr)
      calendar.setTime(startTime)

      val step = 8 * 3600 * 1000 / volume

      for (i <- 1 to volume) {
        calendar.add(Calendar.MILLISECOND, step + (Math.random * step).toInt)
        val bidPrice = BigDecimal.valueOf(sourcePrice.doubleValue() + (Math.random - 0.5) * 5)
        val askPrice = BigDecimal.valueOf(bidPrice.doubleValue() + Math.random * 2)
        val bidSize = BigDecimal.valueOf(100)
        val askSize = BigDecimal.valueOf(100)
        val eventTime = new Timestamp(calendar.getTime.getTime)

        bidPrice.setScale(2, RoundingMode.CEILING)
//        quoteList.append(new Quote(date, symbol, eventTime, i, exchange, bidPrice, bidSize, askPrice, askSize))
        quoteList.append(new Trade(date, symbol, "EX-" + i, eventTime, i, exchange, bidPrice, BigDecimal.valueOf(Math.round(Math.random * 1000))))
      }
      quoteList.toList
    })
    population
  }
}
