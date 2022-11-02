package com.example

import Utils.extractCountryName

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions.{col, split}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.slf4j.LoggerFactory

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Date

// ./spark/bin/spark-submit --jars /home/scala/target/scala-2.12/jdp.jar --class "com.example.CountriesFromInvoicesCsvToHDFS" --master local[4] /home/scala/target/scala-2.12/jdp_2.12-0.1.0-SNAPSHOT.jar

class CountriesFromInvoicesCsvToHDFS(localPath: String, hdfsPath: String) {

  val configSpark: Config = ConfigFactory.load().getConfig("application.spark")
  val configMisc: Config = ConfigFactory.load().getConfig("application.misc")
  val differenceInDays: Int = configMisc.getInt("differenceInDays")
  val sparkCores: String = configSpark.getString("master")
  val checkpoint: String = configSpark.getString("checkpointLocation")

  lazy val spark: SparkSession = SparkSession
    .builder()
    .config("spark.speculation", "false")
    .config("checkpointLocation", s"$checkpoint")
    .master(s"$sparkCores")
    .appName("send countries from invoices csv to HDFS")
    .getOrCreate()

  LoggerFactory.getLogger(spark.getClass)
  spark.sparkContext.setLogLevel("WARN")

  val schema: StructType = StructType(
    StructField("InvoiceNo", StringType, nullable = true) ::
      StructField("StockCode", StringType, nullable = true) ::
      StructField("Quantity", StringType, nullable = true) ::
      StructField("InvoiceDate", StringType, nullable = true) ::
      StructField("CustomerID", StringType, nullable = true) ::
      StructField("Country", StringType, nullable = true) :: Nil
  )

  def getDataframeFromLocalByGivenDate: DataFrame = {

    val today = LocalDate.now()
    val formatterDate = DateTimeFormatter.ofPattern("M/d/yyyy")
    val csvToday = today.minusDays(differenceInDays)
    val csvTodayFormattedString: String = csvToday.format(formatterDate)

    // This is from invoices.csv
    val dfWholeCsvFile = spark
      .read
      .format("csv")
      .option("header", "true")
      .schema(schema)
      .load(localPath)

    // filtered by date: today - differenceInDays
    val dfOnlyAkaToday = dfWholeCsvFile.filter(col("InvoiceDate").startsWith(csvTodayFormattedString))
    println("dfOnlyAkaToday:")
    dfOnlyAkaToday.show()
    println("dfOnlyAkaToday count: " + dfOnlyAkaToday.count())

    dfOnlyAkaToday
  }

  def modifyDataframeAndSaveToHDFS(df: DataFrame): Unit = {

    // extracting country_id and country_name
    val splitColCountry = split(df.col("Country"), "-")
    val dfWithCountryIdCountryName = df
      .withColumn("country_id", splitColCountry.getItem(0))
      .withColumn("country_name", extractCountryName(col("country_id")))

    println("dfWithCountryIdCountryName:")
    dfWithCountryIdCountryName.show()
    println("dfWithCountryIdCountryName count: " + dfWithCountryIdCountryName.count())

    val dfCountryIdCountryName = dfWithCountryIdCountryName
      .drop("InvoiceNo", "StockCode", "Quantity")
      .drop("InvoiceDate", "CustomerID", "Country")

    println("dfCountryIdCountryName:")
    dfCountryIdCountryName.show()
    println("dfCountryIdCountryName count: " + dfCountryIdCountryName.count())

    // write countries to HDFS
    dfCountryIdCountryName.write
      .mode("overwrite")
      .option("header", "true")
      .csv(hdfsPath)
  }

}

object CountriesFromInvoicesCsvToHDFS {

  def main(args: Array[String]): Unit = {

    val configSpark: Config = ConfigFactory.load().getConfig("application.spark")
    val configHDFS: Config = ConfigFactory.load().getConfig("application.hdfs")
    val csvInvoicesPath: String = configSpark.getString("localInvoicesPath")
    val hdfsCountriesPath: String = configHDFS.getString("hdfsCountriesPath")

    val today = new SimpleDateFormat("dd-MM-yyyy").format(new Date())
    val hdfsWholePath: String = hdfsCountriesPath + "/" + today


    val countriesFromInvoicesCsvToHDFS =
      new CountriesFromInvoicesCsvToHDFS(csvInvoicesPath, hdfsWholePath)
    val dataframe = countriesFromInvoicesCsvToHDFS.getDataframeFromLocalByGivenDate
    //println("dataframe:")
    //dataframe.show()
    //println("dataframe count: " + dataframe.count())

    countriesFromInvoicesCsvToHDFS.modifyDataframeAndSaveToHDFS(dataframe)

  }
}
