package com.example

import Utils.getNowHoursUTC

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}
import org.slf4j.LoggerFactory

import java.io.File
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ListBuffer
import scala.language.postfixOps

// ./spark/bin/spark-submit --jars /home/scala/target/scala-2.12/jdp.jar --class "com.example.InvoicesFromHDFSToHive" --master local[4] /home/scala/target/scala-2.12/jdp_2.12-0.1.0-SNAPSHOT.jar

class InvoicesFromHDFSToHive(hdfsPath: String, hiveTableName: String) {

  val configSpark: Config = ConfigFactory.load().getConfig("application.spark")
  val sparkCores: String = configSpark.getString("master")
  val checkpoint: String = configSpark.getString("checkpointLocation")

  val nowTime: LocalTime = LocalTime.now()
  val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("HH_mm_ss")
  val now: String = nowTime.format(formatter)

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
    .appName("send invoices from HDFS to Hive")
    .enableHiveSupport()
    .getOrCreate()

  LoggerFactory.getLogger(spark.getClass)
  spark.sparkContext.setLogLevel("WARN")

  val invoicesSchema: StructType = StructType(
    StructField("invoice_no", StringType, nullable = true) ::
      StructField("stock_code", StringType, nullable = true) ::
      StructField("customer_id", StringType, nullable = true) ::
      StructField("country", StringType, nullable = true) ::
      StructField("invoice_date", TimestampType, nullable = true) ::
      StructField("quantity", IntegerType, nullable = true) :: Nil
  )

  def getDataframeFromHDFSByGivenHours(nowHours: String): Unit = {

    val nowHoursStr: String = getNowHoursUTC
    println("now hours (String): " + nowHoursStr)
    val nowHoursInt: Int = nowHoursStr.toInt
    println("now hours (Int): " + nowHoursInt)
    val threeHoursAgoInt: Int = nowHoursInt - 3
    val twoHoursAgoInt: Int = nowHoursInt - 2
    val hourAgoInt: Int = nowHoursInt - 1
    val threeHoursAgoStr: String = threeHoursAgoInt + ""
    val twoHoursAgoStr: String = twoHoursAgoInt + ""
    val hourAgoStr: String = hourAgoInt + ""

    val fileSystem: FileSystem = {
      val conf = new Configuration()
      conf.set("fs.defaultFS", hdfsPath)
      FileSystem.get(conf)
    }

    // Getting all files names on HDFS
    val files = fileSystem.listFiles(new Path(hdfsPath), true)
    val filenames = ListBuffer[String]()
    val csvFilenames = filenames.filter(filename => filename.endsWith(".csv"))
    while (files.hasNext) filenames += files.next().getPath.toString
    filenames.foreach(println(_))
    println("filenames size: " + filenames.size)
    println("csvFilenames size: " + csvFilenames.size)

    // Filtering only .csv files
    filenames.filter(filename => filename.endsWith(".csv")).foreach({ file =>

      // reading each .csv file
      val dfFromHDFS = spark
        .read
        .format("csv")
        .option("header", "true")
        .schema(invoicesSchema)
        .load(file)

      println("dfFromHDFS:")
      dfFromHDFS.show()

      println("dfFromHDFS count: " + dfFromHDFS.count())
      println("dfFromHDFS schema:")
      dfFromHDFS.printSchema()

      // and write it to main Hive table
      dfFromHDFS.write
        .mode(SaveMode.Append)
        .saveAsTable(hiveTableName)
    })

    println("hours strings:")
    val hoursStrings = filenames.map({filename =>
      val filenameStrings = filename.split("/")
      val filenameNumHoursStr = filenameStrings(6)
      filenameNumHoursStr
    })
    val hoursStringsSet = hoursStrings.toSet
    println("hoursStringsSet:")
    hoursStringsSet.foreach(println)

    // deleting all files on HDFS; they are represented by hour
    hoursStringsSet.foreach({ hour =>
      val hdfsAbsolutePath = hdfsPath + "/" + hour
      val srcPath = new Path(hdfsAbsolutePath)
      if (fileSystem.exists(srcPath)) fileSystem.delete(srcPath, true)
    })
  }
}

object InvoicesFromHDFSToHive {

  def main(args: Array[String]): Unit = {

    val configHDFS: Config = ConfigFactory.load().getConfig("application.hdfs")
    val configHive: Config = ConfigFactory.load().getConfig("application.hive")
    val hdfsInvoicesPath: String = configHDFS.getString("hdfsInvoicesPath")
    //val hiveTablesPathPrefix = configHive.getString("hiveTablesPathPrefix")
    val hiveMainTableName = configHive.getString("invoicesTableName")

    val invoicesFromHDFSToHive = new InvoicesFromHDFSToHive(hdfsInvoicesPath, hiveMainTableName)
    val nowHours: String = getNowHoursUTC
    invoicesFromHDFSToHive.getDataframeFromHDFSByGivenHours(nowHours)
    println("hdfsPath: " + hdfsInvoicesPath)
  }
}
