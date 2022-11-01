package com.example

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.slf4j.LoggerFactory

import java.io.File
import java.net.URI
import java.text.SimpleDateFormat
import java.util.Date

// ./spark/bin/spark-submit --jars /home/scala/target/scala-2.12/jdp.jar --class "com.example.CountriesFromHDFSToHive" --master local[4] /home/scala/target/scala-2.12/jdp_2.12-0.1.0-SNAPSHOT.jar

class CountriesFromHDFSToHive(hdfsPath: String, hiveTablePath: String) {

  val configSpark: Config = ConfigFactory.load().getConfig("application.spark")
  val configHDFS: Config = ConfigFactory.load().getConfig("application.hdfs")
  val sparkCores: String = configSpark.getString("master")
  val checkpoint: String = configHDFS.getString("hdfsCheckpointDirectory")

  val warehouseLocation: String = new File("spark-warehouse").getAbsolutePath

  lazy val spark: SparkSession = SparkSession
    .builder()
    .config("spark.speculation", "false")
    .config("checkpointLocation", s"$checkpoint")
    .config("spark.sql.uris", "thrift://hive-metastore:9083")
    .config("hive.metastore.warehouse.dir", "file:///user/hive/warehouse")
    .config("spark.hadoop.hive.exec.dynamic.partition", "true")
    .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .master(s"$sparkCores")
    .appName("send countries from HDFS to Hive once a day")
    .enableHiveSupport()
    .getOrCreate()

  LoggerFactory.getLogger(spark.getClass)
  spark.sparkContext.setLogLevel("WARN")

  val schema: StructType = StructType(
    StructField("country_id", StringType, nullable = true) ::
      StructField("country_name", StringType, nullable = true) :: Nil
  )

  def readFromHDFSAndInsertNewCountriesToHiveTable(countryHiveTable: String): Unit = {

    val today = new SimpleDateFormat("dd-MM-yyyy").format(new Date())
    println("hdfsPath: " + hdfsPath)
    val hdfsWholePath = hdfsPath + "/" + today
    println("hdfsWholePath: " + hdfsWholePath)

    val dfCountriesFromHDFS = spark
      .read
      .format("csv")
      .option("header", "true")
      .schema(schema)
      .load(hdfsPath)

    val dfCountriesFromHDFSDistinct = dfCountriesFromHDFS.distinct()
      //.withColumnRenamed("CountryID", "country_id")
      //.withColumnRenamed("CountryName", "country_name")
    println("dfCountriesFromHDFSDistinct:")
    dfCountriesFromHDFSDistinct.show()
    println("dfCountriesFromHDFSDistinct count: " + dfCountriesFromHDFSDistinct.count())

    println("hiveTablePath: " + hiveTablePath)
    spark.catalog.refreshTable("country")
    val originalHiveCountriesTable = spark.read
      .format("parquet")
      .schema(schema)
      .load(hiveTablePath)

    println("originalHiveCountriesTable:")
    originalHiveCountriesTable.show()

    val originalRenamed = originalHiveCountriesTable
      //.withColumnRenamed("CountryID", "country_id")
      //.withColumnRenamed("CountryName", "country_name")

    val onlyNewCountries = dfCountriesFromHDFSDistinct
      .join(originalRenamed, Seq("country_id"), "leftanti")
    println("onlyNewCountries:")
    onlyNewCountries.show()

      //val onlyNewCountriesWithCurrentTime = onlyNewCountries
      //  .withColumn("current_time", expr("reflect('java.time.LocalDateTime', 'now')"))

    onlyNewCountries.write
      .mode(SaveMode.Append)
        //.partitionBy("InvoiceDate")
      .format("parquet")
        //.partitionBy("current_time")
      .saveAsTable(countryHiveTable)

  }
}

object CountriesFromHDFSToHive {

  def main(args: Array[String]): Unit = {

    val configHDFS: Config = ConfigFactory.load().getConfig("application.hdfs")
    val configHive: Config = ConfigFactory.load().getConfig("application.hive")
    val hdfsCountriesPath: String = configHDFS.getString("hdfsCountriesPath")
    val hiveTablesPathPrefix: String = configHive.getString("hiveTablesPathPrefix")
    val hiveCountriesTableName: String = configHive.getString("countriesTableName")
    val hiveCountriesTableWholePath = hiveTablesPathPrefix + hiveCountriesTableName

    val today = new SimpleDateFormat("dd-MM-yyyy").format(new Date())
    //val todayUnderscores = new SimpleDateFormat("dd_MM_yyyy").format(new Date())
    val hdfsWholePath: String = hdfsCountriesPath + "/" + today

    val countriesFromHDFSToHive = new CountriesFromHDFSToHive(hdfsWholePath, hiveCountriesTableWholePath)
    countriesFromHDFSToHive.readFromHDFSAndInsertNewCountriesToHiveTable(hiveCountriesTableName)
  }
}
