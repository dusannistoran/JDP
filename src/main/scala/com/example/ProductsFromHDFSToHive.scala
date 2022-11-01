package com.example

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DecimalType, StringType, StructField, StructType, TimestampType}
import org.slf4j.LoggerFactory

import java.io.File
import java.net.URI
import java.text.SimpleDateFormat
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import java.util.Date

// ./spark/bin/spark-submit --jars /home/scala/target/scala-2.12/jdp.jar --class "com.example.ProductsFromHDFSToHive" --master local[4] /home/scala/target/scala-2.12/jdp_2.12-0.1.0-SNAPSHOT.jar

class ProductsFromHDFSToHive(hdfsPath: String, hiveTableName: String) {

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
    .config("spark.hadoop.hive.exec.dynamic.partition", "true")
    .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .master(s"$sparkCores")
    .appName("send products from HDFS to Hive once a day")
    .enableHiveSupport()
    .getOrCreate()

  LoggerFactory.getLogger(spark.getClass)
  spark.sparkContext.setLogLevel("WARN")

  val productsSchema: StructType = StructType(
    StructField("stock_code", StringType, nullable = true) ::
      StructField("product_description", StringType, nullable = true) ::
      StructField("unit_price", DecimalType(8, 2), nullable = true) ::
      StructField("invoice_date", TimestampType, nullable = true) :: Nil
  )

  def getDataframeFromHDFSByGivenDate: DataFrame = {

    val today = new SimpleDateFormat("dd-MM-yyyy").format(new Date())
    println("hdfsPath: " + hdfsPath)
    val hdfsWholePath = hdfsPath + "/" + today
    println("hdfsWholePath: " + hdfsWholePath)

    if (FileSystem.get(new URI(hdfsWholePath), spark.sparkContext.hadoopConfiguration).exists(new Path(hdfsWholePath))) {
      val dfFromHDFS = spark
        .read
        .format("csv")
        .option("header", "true")
        .schema(productsSchema)
        //.option("inferSchema", "true")
        .load(hdfsWholePath)

      println("dfFromHDFS schema:")
      dfFromHDFS.printSchema()
      println("dfFromHDFS:")
      dfFromHDFS.show()

      val dfProductsWithDateOnly = dfFromHDFS
        .withColumn("dateOnly", to_date(dfFromHDFS.col("invoice_date"), "M/d/yyyy"))
        .drop("invoice_date")
        .withColumnRenamed("dateOnly", "date")

      // CAST unit_price to double
      val dfProductsCast = dfProductsWithDateOnly
        .withColumn("unit_price_decimal", dfProductsWithDateOnly.col("unit_price").cast(DecimalType(8, 2)))
        .drop(col("unit_price"))
        .withColumnRenamed("unit_price_decimal", "unit_price")

      //dfProductsCast
      //val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      //val srcPath = new Path(hdfsPath)
      //fileSystem.delete(srcPath, true)
      dfProductsWithDateOnly
    }
    else {
      println(hdfsWholePath + " does not exist!")
      val dfEmpty = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], productsSchema)
      dfEmpty
    }
  }

  def saveDataframeToHDFS(dfForHive: DataFrame): Unit = {
    dfForHive.write
      .mode(SaveMode.Append)
      .format("hive")
      .saveAsTable(hiveTableName)
  }

  def getDataframeFromHDFSByGivenDateAndHourAndSaveToHiveTable: Unit = {

    val nowTime = LocalTime.now()
    val formatter = DateTimeFormatter.ofPattern("H")
    val nowHours = nowTime.format(formatter)
    var hdfsWholePath = hdfsPath + "/" + nowHours
    println("hdfsWholePath: " + hdfsWholePath)

    val nowHoursInt = nowHours.toInt
    val previousHourInt = nowHoursInt - 1
    val twoHoursAgoInt = nowHoursInt - 2
    val threeHoursAgoInt = nowHoursInt - 3
    val hoursList: List[Int] = threeHoursAgoInt :: twoHoursAgoInt :: previousHourInt :: nowHoursInt :: Nil
    // OVO JE SAMO ZA OVAJ I PRETHODNI SAT, POSLE NAPRAVITI ZA OVAJ I PRETHODNA 3 SATA!!!

    val hiveTableName = "product"

    hoursList.foreach({ hourInt =>
      hdfsWholePath = hdfsPath + "/" + (hourInt + "")
      if (FileSystem.get(new URI(hdfsWholePath), spark.sparkContext.hadoopConfiguration).exists(new Path(hdfsWholePath))) {
        val dfFromHDFS = spark
          .read
          .format("csv")
          .option("header", "true")
          .schema(productsSchema)
          .load(hdfsWholePath)
        println("hdfsWholePath: " + hdfsWholePath)

        dfFromHDFS.show(15)
        dfFromHDFS.printSchema()

        val dfForHive = dfFromHDFS
          .withColumn("unit_price_decimal", dfFromHDFS.col("unit_price").cast(DecimalType(8, 2)))

        dfForHive.write
          .mode(SaveMode.Append)
          //.partitionBy("dateonly")
          .saveAsTable(hiveTableName)

        import org.apache.hadoop.conf.Configuration
        val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        val srcPath = new Path(hdfsPath)
        //if (fileSystem.exists(srcPath)) fileSystem.delete(srcPath, true)

      }
      else println(hdfsWholePath + " does not exist!")
    })
  }

}

object ProductsFromHDFSToHive {

  def main(args: Array[String]): Unit = {

    val configHDFS: Config = ConfigFactory.load().getConfig("application.hdfs")
    val configHive: Config = ConfigFactory.load().getConfig("application.hive")
    val hdfsProductsPath = configHDFS.getString("hdfsProductInfoPath")
    val hiveTablePathPrefix = configHive.getString("hiveTablesPathPrefix")
    val productHiveTableName = configHive.getString("productTableName")
    val wholeHiveTablePath = hiveTablePathPrefix + productHiveTableName

    val productsFromHDFSToHive = new ProductsFromHDFSToHive(hdfsProductsPath, productHiveTableName)
    val dfForHive = productsFromHDFSToHive.getDataframeFromHDFSByGivenDate
    println("dfForHive count: " + dfForHive.count())
    println("wholeHiveTablePath: " + wholeHiveTablePath)
    println("hiveTableName: " + productHiveTableName)
    productsFromHDFSToHive.saveDataframeToHDFS(dfForHive)
  }
}
