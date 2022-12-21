package com.example

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.slf4j.LoggerFactory

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

// ./spark/bin/spark-submit --jars /home/scala/target/scala-2.12/jdp.jar --class "com.example.CountriesFromHDFSToHive" --master local[4] /home/scala/target/scala-2.12/jdp_2.12-0.1.0-SNAPSHOT.jar

class CountriesFromHDFSToHive(hdfsPath: String, hiveTablePath: String) {

  val configSpark: Config = ConfigFactory.load().getConfig("application.spark")
  val configHDFS: Config = ConfigFactory.load().getConfig("application.hdfs")
  val sparkCores: String = configSpark.getString("master")
  val checkpoint: String = configHDFS.getString("hdfsCheckpointDirectory")

  val warehouseLocation: String = new File("spark-warehouse").getAbsolutePath

  val spark: SparkSession = SparkSession
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

    try {
      val today = new SimpleDateFormat("dd-MM-yyyy").format(new Date())
      println("hdfsPath: " + hdfsPath)
      val hdfsAbsolutePath = hdfsPath + "/" + today
      println("hdfsAbsolutePath: " + hdfsAbsolutePath)

      val dfCountriesFromHDFS = spark
        .read
        .format("csv")
        .option("header", "true")
        .schema(schema)
        .load(hdfsPath)

      val dfCountriesFromHDFSDistinct = dfCountriesFromHDFS.distinct()
      println("dfCountriesFromHDFSDistinct:")
      dfCountriesFromHDFSDistinct.show()
      println("dfCountriesFromHDFSDistinct count: " + dfCountriesFromHDFSDistinct.count())

      println("hiveTablePath: " + hiveTablePath)
      spark.catalog.refreshTable("country")

      // reading country Hive table
      val originalHiveCountriesTable = spark.read
        .format("parquet")
        .schema(schema)
        .load(hiveTablePath)

      println("originalHiveCountriesTable:")
      originalHiveCountriesTable.show()

      // getting the difference between new countries and countries already stored in country Hive table
      val onlyNewCountries = dfCountriesFromHDFSDistinct
        .join(originalHiveCountriesTable, Seq("country_id"), "leftanti")
      println("onlyNewCountries:")
      onlyNewCountries.show()

      // appending the difference into country Hive table
      onlyNewCountries.write
        .mode(SaveMode.Append)
        .format("parquet")
        .saveAsTable(countryHiveTable)

      val fileSystem: FileSystem = {
        val conf = new Configuration()
        conf.set("fs.defaultFS", hdfsPath)
        FileSystem.get(conf)
      }
      println("products hdfsPath: " + hdfsPath)
      val srcPath = new Path(hdfsPath)
      if (fileSystem.exists(srcPath)) fileSystem.delete(srcPath, true)
    } catch {
      case e: Exception => println("CountriesFromHDFSToHive, " +
        "def readFromHDFSAndInsertNewCountriesToHiveTable(countryHiveTable: String): Unit, " +
        "error occurred: " + e)
    }
  }
}

object CountriesFromHDFSToHive {

  def main(args: Array[String]): Unit = {

    try {
      val configHDFS: Config = ConfigFactory.load().getConfig("application.hdfs")
      val configHive: Config = ConfigFactory.load().getConfig("application.hive")
      val hdfsCountriesPath: String = configHDFS.getString("hdfsCountriesPath")
      val hiveTablesPathPrefix: String = configHive.getString("hiveTablesPathPrefix")
      val hiveCountriesTableName: String = configHive.getString("countriesTableName")
      val hiveCountriesTableWholePath = hiveTablesPathPrefix + hiveCountriesTableName

      val today = new SimpleDateFormat("dd-MM-yyyy").format(new Date())
      val hdfsAbsolutePath: String = hdfsCountriesPath + "/" + today

      val countriesFromHDFSToHive = new CountriesFromHDFSToHive(hdfsAbsolutePath, hiveCountriesTableWholePath)
      countriesFromHDFSToHive.readFromHDFSAndInsertNewCountriesToHiveTable(hiveCountriesTableName)
    } catch {
      case e: Exception => println("CountriesFromHDFSToHive, " +
        "def main(args: Array[String]): Unit, " +
        "error occurred: " + e)
    }
  }
}
