package io.wpengyu.synthetic.equity

import java.math.BigInteger

import io.wpengyu.synthetic.TestUtils
import io.wpengyu.synthetic.equity.MarketGenerator.{convertSeedToBaseline, generate}
import org.apache.spark
import org.junit.{After, Assert, Before, Test}
import org.apache.spark.sql.types.{DataTypes, DateType, DecimalType, StringType, StructField, StructType}

import scala.collection.mutable.ListBuffer

class DatagenTest {

  @Test
  def test2(): Unit = {

    val ss = TestUtils.spark
    val procDate = "2020-06-26"
    val outputLocation = "target/datagen"
    val exchangeListStr = "NYSE"
    val unitVolume = 10000
    val seedLocation = "src/test/resources/synthetic/seed.txt"
    import ss.implicits._
    val seed = ss.read
      .option("header", "true")
      .csv(seedLocation)
      .as[Seed]

    val baseline = convertSeedToBaseline(ss, seed, exchangeListStr.split(","), unitVolume.toInt)
    val data = generate(ss, baseline, procDate)

    //    data.write.format("com.databricks.spark.csv")
    //      .option("delimiter", ",")
    //      .mode("overwrite")
    //      .save(outputLocation)

    data.show(100, false)
    data.write.mode("overwrite")
      .format("csv")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSSSSS")
      .save("target/datagen/data")

    val meta = seed.selectExpr("symbol", "symbol || 'Corporation'")
    meta.show(100, false)
    meta.write.mode("overwrite").csv("target/datagen/meta")
  }

  @Before
  def setup:Unit = {
    TestUtils.initSpark()
  }

  @After
  def clean(): Unit = {
    TestUtils.terminateSpark()
  }
}


case class Dummy(id: Int, data: Array[Int])
case class Dday(col: String)

object DatagenTest {

  @Before
  def setup:Unit = {
    TestUtils.initSpark()
  }

  @After
  def clean(): Unit = {
    TestUtils.terminateSpark()
  }
}