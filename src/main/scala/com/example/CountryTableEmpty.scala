package com.example

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.slf4j.LoggerFactory

import java.io.File

// ./spark/bin/spark-submit --jars /home/scala/target/scala-2.12/jdp.jar --class "com.example.CountryTableEmpty" --master local[4] /home/scala/target/scala-2.12/jdp_2.12-0.1.0-SNAPSHOT.jar

class CountryTableEmpty(hiveTablePath: String) {

  val configSpark: Config = ConfigFactory.load().getConfig("application.spark")
  val sparkCores: String = configSpark.getString("master")
  val checkpoint: String = configSpark.getString("checkpointLocation")

  val warehouseLocation: String = new File("spark-warehouse").getAbsolutePath

  lazy val spark: SparkSession = SparkSession
    .builder()
    .config("spark.speculation", "false")
    .config("checkpointLocation", s"$checkpoint")
    .config("spark.sql.uris", "thrift://hive-metastore:9083")
    .config("hive.metastore.warehouse.dir", "file:///user/hive/warehouse")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .master(s"$sparkCores")
    .appName("create empty countries table")
    .enableHiveSupport()
    .getOrCreate()

  LoggerFactory.getLogger(spark.getClass)
  spark.sparkContext.setLogLevel("WARN")

  def createHiveTable(hiveTableName: String): Unit = {

    val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val srcPath = new Path(hiveTablePath)
    if (fileSystem.exists(srcPath)) {
      println("srcPath exists; hiveTablePath: " + hiveTablePath)
      fileSystem.delete(srcPath, true)
    }
    else println("srcPath does NOT exist; hiveTablePath: " + hiveTablePath)

    val countriesSchema = StructType(
      StructField("country_id", StringType, nullable = true) ::
        StructField("country_name", StringType, nullable = true) :: Nil
    )
    import org.apache.spark.sql.Row
    val dfEmpty = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], countriesSchema)

    dfEmpty.write
      .mode(SaveMode.Overwrite)
      .saveAsTable(hiveTableName)
  }
}

object CountryTableEmpty {

  def main(args: Array[String]): Unit = {

    //val configHDFS: Config = ConfigFactory.load().getConfig("application.hdfs")
    val configHive: Config = ConfigFactory.load().getConfig("application.hive")
    //val hdfsCountriesPath: String = configHDFS.getString("hdfsCountriesPath")
    val hiveTablesPathPrefix: String = configHive.getString("hiveTablesPathPrefix")
    val hiveCountriesTableName: String = configHive.getString("countriesTableName")
    val hiveCountriesTableWholePath = hiveTablesPathPrefix + hiveCountriesTableName

    val countryTableEmpty = new CountryTableEmpty(hiveCountriesTableWholePath)
    countryTableEmpty.createHiveTable(hiveCountriesTableName)

  }
}
