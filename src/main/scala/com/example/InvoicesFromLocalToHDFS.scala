package com.example

import Utils.getNowHoursUTC

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions.{col, hour, split, to_timestamp}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.slf4j.LoggerFactory

import java.time.{LocalDate, LocalTime}
import java.time.format.DateTimeFormatter

// ./spark/bin/spark-submit --jars /home/scala/target/scala-2.12/jdp.jar --class "com.example.InvoicesFromLocalToHDFS" --master local[4] /home/scala/target/scala-2.12/jdp_2.12-0.1.0-SNAPSHOT.jar

class InvoicesFromLocalToHDFS(localPath: String, hdfsPath: String) {

  val configSpark: Config = ConfigFactory.load().getConfig("application.spark")
  val configMisc: Config = ConfigFactory.load().getConfig("application.misc")
  val differenceInDays: Int = configMisc.getInt("differenceInDays")
  val sparkCores: String = configSpark.getString("master")
  val checkpoint: String = configSpark.getString("checkpointLocation")

  //val nowTime: LocalTime = LocalTime.now()
  val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("HH_mm_ss")
  //val now: String = nowTime.format(formatter)

  lazy val spark: SparkSession = SparkSession
    .builder()
    .config("spark.speculation", "false")
    .config("checkpointLocation", s"$checkpoint")
    .master(s"$sparkCores")
    .appName("send invoices to HDFS")
    .getOrCreate()

  LoggerFactory.getLogger(spark.getClass)
  spark.sparkContext.setLogLevel("WARN")


  val invoicesSchema: StructType = StructType(
    StructField("InvoiceNo", StringType, nullable = true) ::
      StructField("StockCode", StringType, nullable = true) ::
      StructField("Quantity", StringType, nullable = true) ::
      StructField("InvoiceDate", StringType, nullable = true) ::
      StructField("CustomerID", StringType, nullable = true) ::
      StructField("Country", StringType, nullable = true) :: Nil
  )

  // pick up invoices hour by hour, according to current time
  def getDataframeFromLocalByGivenDateAndHour(nowHours: String): DataFrame = {

    try {

      val today: LocalDate = LocalDate.now()
      // US format
      val formatterDate = DateTimeFormatter.ofPattern("M/d/yyyy")
      val todayFormattedStr: String = today.format(formatterDate)
      val csvToday: LocalDate = today.minusDays(differenceInDays)
      val csvTodayFormattedStr: String = csvToday.format(formatterDate)
      println("today (String): " + todayFormattedStr)
      println("csvToday (String): " + csvTodayFormattedStr)

      // all invoices from whole csv file
      val dfWholeCsvFile = spark
        .read
        .format("csv")
        .option("header", "true")
        .schema(invoicesSchema)
        .load(localPath)

      // invoices filtered by date: today - differenceInDays
      val dfOnlyAkaToday = dfWholeCsvFile.filter(col("InvoiceDate").startsWith(csvTodayFormattedStr))
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

      // invoices filtered by current time, that is, current hour
      val nowHoursString: String = nowHours
      println("now hours UTC (String): " + nowHoursString)
      val nowHoursInt: Int = nowHoursString.toInt
      val hourAgoInt: Int = nowHoursInt - 1
      val hourAgoStr: String = hourAgoInt + ""
      //val dfNowHoursOnly = dfTimeOnly.filter(col("hours").equalTo(nowHoursString))
      //println("dfNowHoursOnly:")
      //dfNowHoursOnly.show()
      //println("dfNowHoursOnly count: " + dfNowHoursOnly.count())
      //dfNowHoursOnly
      val dfHourAgoOnly = dfTimeOnly.filter(col("hours").equalTo(hourAgoStr))
      println("dfHourAgoOnly:")
      dfHourAgoOnly.show()
      println("dfHourAgoOnly count: " + dfHourAgoOnly.count())
      dfHourAgoOnly
    } catch {
      case e: Exception => println("InvoicesFromLocalToHDFS, " +
        "def getDataframeFromLocalByGivenDateAndHour(nowHours: String): DataFrame, " +
        "error occurred: " + e)
        import org.apache.spark.sql.Row
        val emptyDf: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], invoicesSchema)
        emptyDf
    }

  }

  def transformDataframeAndSaveToHDFS(dfFiltered: DataFrame): Unit = {

    try {
      val dfForHDFS = dfFiltered
        .drop(col("timeOnly"))
        .drop(col("hours"))
        .drop(col("dateOnly"))
        .withColumn("invoice_date", to_timestamp(col("InvoiceDate"), "M/d/yyyy H:mm"))
        .withColumn("quantity_int", col("Quantity").cast(IntegerType))
        .drop("Quantity")
        .drop(col("InvoiceDate"))
        .withColumnRenamed("InvoiceNo", "invoice_no")
        .withColumnRenamed("StockCode", "stock_code")
        .withColumnRenamed("quantity_int", "quantity")
        .withColumnRenamed("CustomerID", "customer_id")
        .withColumnRenamed("Country", "country")

      val nowHours: String = getNowHoursUTC
      val hdfsAbsolutePath: String = hdfsPath + "/" + nowHours

      dfForHDFS.write
        .mode("overwrite")
        .option("header", "true")
        .csv(hdfsAbsolutePath)
    } catch {
      case e: Exception => println("InvoicesFromLocalToHDFS, " +
        "def transformDataframeAndSaveToHDFS(dfFiltered: DataFrame): Unit, " +
        "error occurred: " + e)
    }
  }
}

object InvoicesFromLocalToHDFS {

  def main(args: Array[String]): Unit = {

    try {
      val configSpark: Config = ConfigFactory.load().getConfig("application.spark")
      val configHDFS: Config = ConfigFactory.load().getConfig("application.hdfs")
      val csvInvoicesPath: String = configSpark.getString("localInvoicesPath")
      val hdfsInvoicesPath: String = configHDFS.getString("hdfsInvoicesPath")

      val invoicesFromLocalToHDFS = new InvoicesFromLocalToHDFS(csvInvoicesPath, hdfsInvoicesPath)
      val nowHours: String = getNowHoursUTC
      val dfFiltered = invoicesFromLocalToHDFS.getDataframeFromLocalByGivenDateAndHour(nowHours)
      invoicesFromLocalToHDFS.transformDataframeAndSaveToHDFS(dfFiltered)
    } catch {
      case e: Exception => println("InvoicesFromLocalToHDFS, " +
        "def main(args: Array[String]): Unit, " +
        "error occurred: " + e)
    }

  }
}
