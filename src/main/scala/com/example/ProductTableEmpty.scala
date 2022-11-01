package com.example

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DecimalType, StringType, StructField, StructType, TimestampType}
import org.slf4j.LoggerFactory

import java.io.File

// ./spark/bin/spark-submit --jars /home/scala/target/scala-2.12/jdp.jar --class "com.example.ProductTableEmpty" --master local[4] /home/scala/target/scala-2.12/jdp_2.12-0.1.0-SNAPSHOT.jar

class ProductTableEmpty(hiveTableName: String) {

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
    .appName("create empty product table")
    .enableHiveSupport()
    .getOrCreate()

  LoggerFactory.getLogger(spark.getClass)
  spark.sparkContext.setLogLevel("WARN")

  def createHiveTable(hdfsPath: String): Unit = {

    val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val srcPath = new Path(hdfsPath)
    if (fileSystem.exists(srcPath)) {
      println("srcPath exists; hdfsPath: " + hdfsPath)
      fileSystem.delete(srcPath, true)
    }
    else println("srcPath does NOT exist; hdfsPath: " + hdfsPath)

    val productSchema = StructType(
      StructField("stock_code", StringType, nullable = true) ::
        StructField("product_description", StringType, nullable = true) ::
        StructField("unit_price", DecimalType(8, 2), nullable = true) ::
        StructField("date", TimestampType, nullable = true) :: Nil
    )

    import org.apache.spark.sql.Row
    val dfEmpty = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], productSchema)
    //.withColumn("current_time", expr("reflect('java.time.LocalDateTime', 'now')"))

    dfEmpty.write
      .mode(SaveMode.Overwrite)
      .format("hive")
      .saveAsTable(hiveTableName)
  }

}

object ProductTableEmpty {

  def main(args: Array[String]): Unit = {

    val configHive: Config = ConfigFactory.load().getConfig("application.hive")
    val hdfsPathPrefix: String = configHive.getString("hiveTablesPathPrefix")
    val productTableName: String = configHive.getString("productTableName")

    val productTableEmpty = new ProductTableEmpty(productTableName)
    productTableEmpty.createHiveTable(hdfsPathPrefix + productTableName)
  }

}
