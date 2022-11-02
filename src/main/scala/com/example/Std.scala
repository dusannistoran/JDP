package com.example

import com.example.Utils.getNowHoursUTC
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions.{avg, col, hour, lit, stddev, stddev_pop, stddev_samp, when}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer
import org.slf4j.LoggerFactory

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.collection.mutable
import scala.compat.Platform.EOL

import java.io.File

// cadzilvsfztrrheg
// cadz ilvs fztr rheg

// ./spark/bin/spark-submit --packages org.postgresql:postgresql:42.2.5 --jars /home/scala/target/scala-2.12/jdp.jar,/home/scala/target/scala-2.12/Scala_Spark_Mail.jar --class "com.example.Std" --master local[4] /home/scala/target/scala-2.12/jdp_2.12-0.1.0-SNAPSHOT.jar

class Std {

  val configSpark: Config = ConfigFactory.load().getConfig("application.spark")
  val configHive: Config = ConfigFactory.load().getConfig("application.hive")
  val configMisc: Config = ConfigFactory.load().getConfig("application.misc")
  val sparkCores: String = configSpark.getString("master")
  val checkpoint: String = configSpark.getString("checkpointLocation")
  val hiveTablePathPrefix: String = configHive.getString("hiveTablesPathPrefix")
  val configPostgres: Config = ConfigFactory.load().getConfig("application.postgres")
  val postgresDriver: String = configPostgres.getString("driver")
  val postgresUrl: String = configPostgres.getString("url")
  val postgresUser: String = configPostgres.getString("user")
  val postgresPassword: String = configPostgres.getString("password")
  val differenceInDays: Int = configMisc.getInt("differenceInDays")

  val warehouseLocation: String = new File("spark-warehouse").getAbsolutePath

  lazy val spark: SparkSession = SparkSession
    .builder()
    .config("spark.speculation", "false")
    .config("checkpointLocation", s"$checkpoint")
    .config("spark.sql.uris", "thrift://hive-metastore:9083")
    .config("hive.metastore.warehouse.dir", "file:///user/hive/warehouse")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .config("spark.hadoop.hive.exec.dynamic.partition", "true")
    .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
    .master(s"$sparkCores")
    .appName("join tables")
    .enableHiveSupport()
    .getOrCreate()

  LoggerFactory.getLogger(spark.getClass)
  spark.sparkContext.setLogLevel("WARN")

  val today: LocalDate = LocalDate.now()
  val formatterDate: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val akaToday: LocalDate = today.minusDays(differenceInDays)
  val akaTodayFormattedString: String = akaToday.format(formatterDate)

  def getDataframeFromPostgres(): DataFrame = {

    // getting the whole table from Postgres
    val dfWholeTable = spark.read
      .format("jdbc")
      .option("driver", s"$postgresDriver")
      .option("url", s"$postgresUrl")
      .option("dbtable", "joined")
      .option("user", s"$postgresUser")
      .option("password", s"$postgresPassword")
      .load()

    val dfWholeTableWithDateString = dfWholeTable
      .withColumn("invoice_date_string", dfWholeTable.col("invoice_date").cast(StringType))
    val dfOnlyToday = dfWholeTableWithDateString
      .filter(col("invoice_date_string").startsWith(akaTodayFormattedString))
      .drop("invoice_date_string")

    val currentTimeHoursStr: String = getNowHoursUTC
    println("currentTimeHours: " + currentTimeHoursStr)
    val currentTimeHoursInt: Int = currentTimeHoursStr.toInt
    val threeHoursAgoInt: Int = currentTimeHoursInt - 3
    val twoHoursAgoInt: Int = currentTimeHoursInt - 2
    val hourAgoInt: Int = currentTimeHoursInt - 1
    val threeHoursAgoStr: String = threeHoursAgoInt + ""
    val twoHoursAgoStr: String = twoHoursAgoInt + ""
    val hourAgoStr: String = hourAgoInt + ""

    // filtering table by current time; previous hour and current hour;
    // later I'll modify this and involve 3 hours ago, 2 hours ago, hour ago and current hour invoices
    val dfFilteredByHour = dfOnlyToday
      .filter(hour(col("invoice_date")) === currentTimeHoursStr ||
              hour(col("invoice_date")) === hourAgoStr)
    println("dfFilteredByHour:")
    dfFilteredByHour.show()
    println("dfFilteredByHour count: " + dfFilteredByHour.count())
    println("dfFilteredByHour schema:")
    dfFilteredByHour.printSchema()

    dfFilteredByHour
  }

  def getDataframeWithQuantityMedian(dfFromPostgres: DataFrame): DataFrame = {

    val dfQuantityMedian = dfFromPostgres
      .withColumn("quantity_median", lit(dfFromPostgres.stat.approxQuantile("quantity", Array(0.5), 0.2)))
      .withColumn("quantity_median_int", col("quantity_median")(0))
    dfQuantityMedian
  }

  def getDataframeWithStandardDeviation(dfFromPostgres: DataFrame, dfQuantityMedian: DataFrame): DataFrame = {

    val dfStandardDeviationSample = dfFromPostgres.select(stddev("quantity"))
    println("dfStandardDeviationSample:")
    dfStandardDeviationSample.show()

    val valueStandardDeviationSample: Double = dfStandardDeviationSample.collect().head.getDouble(0)

    val dfWithStandardDeviation = dfQuantityMedian
      .drop("quantity_median")
      .withColumn("stddev(quantity)", lit(valueStandardDeviationSample))
      .withColumn("very_large_quantity",
        when(col("quantity") > (lit(2.0) * col("stddev(quantity)") + col("quantity_median_int")), lit(true))
          .otherwise(lit(false)))
    println("dfWithStandardDeviation:")
    dfWithStandardDeviation.show(151)
    println("dfWithStandardDeviation count: " + dfWithStandardDeviation.count())
    println("dfWithStandardDeviation schema:")
    dfWithStandardDeviation.printSchema()

    val dfFiltered = dfWithStandardDeviation.filter(col("very_large_quantity").equalTo("true"))

    println("dfFiltered:")
    dfFiltered.show()
    println("dfFiltered count: " + dfFiltered.count())

    val dfForEmail = dfFiltered
      .drop("date")
      .withColumnRenamed("quantity_median_int", "quantity_median")

    dfForEmail
  }

  def createEmailBody(df: DataFrame): String = {

    //val text: mutable.StringBuilder = new mutable.StringBuilder("")
    var text: String = ""
    val data = df.rdd
      .map(row => {
        row.mkString(" | ") + "\r\n"
      }).collect()
    data.foreach(line => text = text + line + "\r\n")


    println("text: ")
    println(text)

    text
  }
}

object Std {

  def main(args: Array[String]): Unit = {

    val std = new Std()

    val dfFromPostgres: DataFrame = std.getDataframeFromPostgres()
    println("dfFromPostgres:")
    dfFromPostgres.show()
    println("dfFromPostgres count: " + dfFromPostgres.count())
    println("dfFromPostgres schema:")
    dfFromPostgres.printSchema()

    //val dfQuantityMedian = dfFromPostgres
    //  .withColumn("quantity_median", lit(dfFromPostgres.stat.approxQuantile("quantity", Array(0.5), 0.2)))
    //  .withColumn("quantity_median_int", col("quantity_median")(0))

    val dfQuantityMedian: DataFrame = std.getDataframeWithQuantityMedian(dfFromPostgres)
    println("dfQuantityMedian:")
    dfQuantityMedian.show()
    println("dfQuantityMedian count: " + dfQuantityMedian.count())
    println("dfQuantityMedian schema:")
    dfQuantityMedian.printSchema()

    //val dfStandardDeviationSample = dfFromPostgres.select(stddev("quantity"))
    //println("dfStandardDeviationSample:")
    //dfStandardDeviationSample.show()

    //val valueStandardDeviationSample: Double = dfStandardDeviationSample.collect().head.getDouble(0)

    val dfWithStandardDeviation = std.getDataframeWithStandardDeviation(dfFromPostgres, dfQuantityMedian)
    //val dfWithStandardDeviation = dfQuantityMedian
    //  .drop("quantity_median")
    //  .withColumn("stddev(quantity)", lit(valueStandardDeviationSample))
    //  .withColumn("very_large_quantity",
    //    when(col("quantity") > (lit(2.0) * col("stddev(quantity)") + col("quantity_median_int")), lit(true))
    //      .otherwise(lit(false)))
    //println("dfWithStandardDeviation:")
    //dfWithStandardDeviation.show(151)
    //println("dfWithStandardDeviation count: " + dfWithStandardDeviation.count())
    //println("dfWithStandardDeviation schema:")
    //dfWithStandardDeviation.printSchema()

    //val dfFiltered = dfWithStandardDeviation.filter(col("very_large_quantity").equalTo("true"))

    //println("dfFiltered:")
    //dfFiltered.show()
    //println("dfFiltered count: " + dfFiltered.count())

    //val dfForEmail = dfFiltered
    //  .drop("date")
    //  .withColumnRenamed("quantity_median_int", "quantity_median")

    //val dfForEmail = std.getDataframeWithStandardDeviation(dfFromPostgres, dfQuantityMedian)


    val columnsSeq = Seq("country_id", "stock_code", "invoice_no", "customer_id", "country",
                     "invoice_date", "quantity", "unit_price", "product_description", "region_id",
                   "total_price", "country_name", "quantity_median", "stddev(quantity)", "very_large_quantity")

    val header = columnsSeq.map(c => c + " | ").mkString
    //val msg = header + "\n" + createEmailBody(dfForEmail)
    val msg = header + "\n" + std.createEmailBody(dfWithStandardDeviation)

    val obj = new Email("/home/scala/src/main/resources/application-mail.conf")
    val spark: SparkSession = SparkSession.builder().appName("Spark Mail Job").master("local[*]").getOrCreate()
    obj.sendMail(msg, spark.sparkContext.applicationId, "test", "R", "", "")

  }
}
