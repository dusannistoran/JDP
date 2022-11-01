package com.example

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions.{col, hour, split, to_timestamp}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.slf4j.LoggerFactory

import java.text.SimpleDateFormat
import java.time.{LocalDate, LocalTime}
import java.time.format.DateTimeFormatter
import java.util.Date

// ./spark/bin/spark-submit --jars /home/scala/target/scala-2.12/jdp.jar --class "com.example.ProductsFromLocalToHDFS" --master local[4] /home/scala/target/scala-2.12/jdp_2.12-0.1.0-SNAPSHOT.jar

class ProductsFromLocalToHDFS(localPath: String, hdfsPath: String) {

  val configSpark: Config = ConfigFactory.load().getConfig("application.spark")
  val configMisc: Config = ConfigFactory.load().getConfig("application.misc")
  val sparkCores: String = configSpark.getString("master")
  val checkpoint: String = configSpark.getString("checkpointLocation")
  val differenceInDays: Int = configMisc.getInt("differenceInDays")

  lazy val spark: SparkSession = SparkSession
    .builder()
    .config("spark.speculation", "false")
    .config("checkpointLocation", s"$checkpoint")
    .master(s"$sparkCores")
    .appName("send products to HDFS")
    .getOrCreate()

  LoggerFactory.getLogger(spark.getClass)
  spark.sparkContext.setLogLevel("WARN")

  val productsSchema: StructType = StructType(
    StructField("StockCode", StringType, nullable = true) ::
      StructField("Description", StringType, nullable = true) ::
      StructField("UnitPrice", StringType, nullable = true) ::
      StructField("InvoiceDate", StringType, nullable = true) :: Nil
  )

  // pick up invoices for single day at once
  def getDataframeFromLocalByGivenDate: DataFrame = {

    val today = LocalDate.now()
    val formatterDate = DateTimeFormatter.ofPattern("M/d/yyyy")
    //val todayFormattedString: String = today.format(formatterDate)
    val csvToday = today.minusDays(differenceInDays)
    val csvTodayFormattedString: String = csvToday.format(formatterDate)

    val dfWholeCsvFile = spark
      .read
      .format("csv")
      .option("header", "true")
      .schema(productsSchema)
      .load(localPath)

    val dfOnlyAkaToday = dfWholeCsvFile.filter(col("InvoiceDate").startsWith(csvTodayFormattedString))
    println("dfOnlyAkaToday:")
    dfOnlyAkaToday.show()
    println("dfOnlyAkaToday count: " + dfOnlyAkaToday.count())

    dfOnlyAkaToday
  }

  // pick up invoices hour by hour, according to current time
  def getDataframeFromLocalByGivenDateAndHour: DataFrame = {

    val today = LocalDate.now()
    val formatterDate = DateTimeFormatter.ofPattern("M/d/yyyy")
    val todayFormattedString: String = today.format(formatterDate)
    val csvToday = today.minusDays(4349)
    val csvTodayFormattedString: String = csvToday.format(formatterDate)

    val today2 = new SimpleDateFormat("M/d/yyyy").format(new Date())
    val nowTime = LocalTime.now()
    val formatterHours = DateTimeFormatter.ofPattern("HH")
    val nowHours: String = nowTime.format(formatterHours)
    val nowHoursInt: Int = nowHours.toInt
    println("today2: " + today2)
    println("nowTime: " + nowTime)
    println("todayFormattedString: " + todayFormattedString)
    println("csvTodayFormattedString: " + csvTodayFormattedString)
    println("nowHours: " + nowHours)

    val dfWholeCsvFile = spark
      .read
      .format("csv")
      .option("header", "true")
      .schema(productsSchema)
      .load(localPath)

    val dfOnlyAkaToday = dfWholeCsvFile.filter(col("InvoiceDate").startsWith(csvTodayFormattedString))
    println("dfOnlyAkaToday:")
    dfOnlyAkaToday.show()
    println("dfOnlyAkaToday count: " + dfOnlyAkaToday.count())

    val splitColInvoiceDate = split(dfOnlyAkaToday.col("InvoiceDate"), " ")
    val dfTimeOnly = dfOnlyAkaToday
      .withColumn("dateOnly", splitColInvoiceDate.getItem(0))
      .withColumn("timeOnly", splitColInvoiceDate.getItem(1))
      .withColumn("hours", hour(col("timeOnly")))
    println("dfTimeOnly:")
    dfTimeOnly.show()
    println("dfTimeOnly count: " + dfTimeOnly.count())

    val dfOnlyAkaTodayAndNowHoursUtc = dfTimeOnly.filter(col("hours").equalTo((nowHoursInt - 1) + ""))
    println("dfOnlyAkaTodayAndNowHoursUtc:")
    dfOnlyAkaTodayAndNowHoursUtc.show()
    println("dfOnlyAkaTodayAndNowHoursUtc count: " + dfOnlyAkaTodayAndNowHoursUtc.count())

    dfOnlyAkaTodayAndNowHoursUtc
  }

  def transformDataframeAndSaveToHDFS(dfFiltered: DataFrame): Unit = {

    val dfForHDFS = dfFiltered
    .withColumn("invoice_date", to_timestamp(col("InvoiceDate"), "M/d/yyyy H:mm"))
      .drop(col("InvoiceDate"))
      .withColumnRenamed("StockCode", "stock_code")
      .withColumnRenamed("Description", "product_description")
      .withColumnRenamed("UnitPrice", "unit_price")

    dfForHDFS.write
      .mode("overwrite")
      .option("header", "true")
      .csv(hdfsPath)
  }
}

object ProductsFromLocalToHDFS {

  def main(args: Array[String]): Unit = {

    val configSpark: Config = ConfigFactory.load().getConfig("application.spark")
    val configHDFS: Config = ConfigFactory.load().getConfig("application.hdfs")
    val csvProductsPath: String = configSpark.getString("localProductInfoPath")
    val hdfsProductsPath: String = configHDFS.getString("hdfsProductInfoPath")

    val today = new SimpleDateFormat("dd-MM-yyyy").format(new Date())
    //val hdfsWholePath: String = hdfsProductsPath + "/" + today + "/" + nowHours
    val hdfsWholePath: String = hdfsProductsPath + "/" + today
    println("csvProductsPath: " + csvProductsPath)
    println("hdfsWholePath: " + hdfsWholePath)

    val productsFromLocalToHDFS = new ProductsFromLocalToHDFS(csvProductsPath, hdfsWholePath)
    val dfFromCsv = productsFromLocalToHDFS.getDataframeFromLocalByGivenDate
    productsFromLocalToHDFS.transformDataframeAndSaveToHDFS(dfFromCsv)
  }
}
