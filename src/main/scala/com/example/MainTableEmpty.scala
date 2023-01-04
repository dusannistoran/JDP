package com.example

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}
import org.slf4j.LoggerFactory

import java.io.File

// ./spark/bin/spark-submit --jars /home/scala/target/scala-2.12/jdp.jar --class "com.example.MainTableEmpty" --master local[4] /home/scala/target/scala-2.12/jdp_2.12-0.1.0-SNAPSHOT.jar

class MainTableEmpty(hiveTableName: String) {

  val configSpark: Config = ConfigFactory.load().getConfig("application.spark")
  val sparkCores: String = configSpark.getString("master")
  val checkpoint: String = configSpark.getString("checkpointLocation")

  val warehouseLocation: String = new File("spark-warehouse").getAbsolutePath

  val spark: SparkSession = SparkSession
    .builder()
    .config("spark.speculation", "false")
    .config("checkpointLocation", s"$checkpoint")
    .config("spark.sql.uris", "thrift://hive-metastore:9083")
    .config("hive.metastore.warehouse.dir", "file:///user/hive/warehouse")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .master(s"$sparkCores")
    .appName("create empty main table")
    .enableHiveSupport()
    .getOrCreate()

  LoggerFactory.getLogger(spark.getClass)
  spark.sparkContext.setLogLevel("WARN")

  def createHiveTable(hdfsPath: String): Unit = {

    // deleting any invoices on HDFS if they exist
    val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val srcPath = new Path(hdfsPath)
    if (fileSystem.exists(srcPath)) {
      println("srcPath exists; hdfsPath: " + hdfsPath)
      fileSystem.delete(srcPath, true)
    }
    else println("srcPath does NOT exist; hdfsPath: " + hdfsPath)

    val mainSchema = StructType(
      StructField("invoice_no", StringType, nullable = true) ::
        StructField("stock_code", StringType, nullable = true) ::
        StructField("customer_id", StringType, nullable = true) ::
        StructField("country", StringType, nullable = true) ::
        StructField("invoice_date", TimestampType, nullable = true) ::
        StructField("quantity", IntegerType, nullable = true) :: Nil
    )

    import org.apache.spark.sql.Row
    val dfEmpty = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], mainSchema)

    dfEmpty.write
      .mode(SaveMode.Overwrite)
      .saveAsTable(hiveTableName)
  }

}

object MainTableEmpty {

  def main(args: Array[String]): Unit = {

    val configHive: Config = ConfigFactory.load().getConfig("application.hive")
    val hdfsPathPrefix: String = configHive.getString("hiveTablesPathPrefix")
    val invoicesTableName: String = configHive.getString("invoicesTableName")

    val mainTableEmpty = new MainTableEmpty(invoicesTableName)
    mainTableEmpty.createHiveTable(hdfsPathPrefix + invoicesTableName)

  }
}
