package com.example

import com.example.Utils.getNowHoursUTC
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.Partition
import org.apache.spark.sql.functions.{avg, col, hour, lit, stddev, stddev_pop, stddev_samp, when}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer
import org.slf4j.LoggerFactory

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.compat.Platform.EOL

//import com.spark.mail.Email

import javax.mail._
import javax.mail.internet._

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

    val dfWholeTable = spark.read
      .format("jdbc")
      .option("driver", s"$postgresDriver")
      .option("url", s"$postgresUrl")
      .option("dbtable", "joined")
      .option("user", s"$postgresUser")
      .option("password", s"$postgresPassword")
      .load()
    //dfWholeTable

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

    val dfQuantityMedian = dfFromPostgres
      .withColumn("quantity_median", lit(dfFromPostgres.stat.approxQuantile("quantity", Array(0.5), 0.2)))
      .withColumn("quantity_median_int", col("quantity_median")(0))

    println("dfQuantityMedian:")
    dfQuantityMedian.show()
    println("dfQuantityMedian count: " + dfQuantityMedian.count())
    println("dfQuantityMedian schema:")
    dfQuantityMedian.printSchema()

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
      //.withColumnRenamed("very_large_quantity", "large_qty")

    //val countryIds = dfFiltered.select("country_id").rdd.map(x => x.mkString).collect()
    //val countryId = countryIds(0)
    //println("countryId: " + countryId)
    //val stockCodes = dfFiltered.select("stock_code").rdd.map(x => x.mkString).collect()
    //val stockCode = stockCodes(0)
    //println("stockCode: " + stockCode)

    val columnsSeq = Seq("country_id", "stock_code", "invoice_no", "customer_id", "country",
                     "invoice_date", "quantity", "unit_price", "product_description", "region_id",
                   "total_price", "country_name", "quantity_median", "stddev(quantity)", "very_large_quantity")



    def createEmailBody(df: DataFrame): String = {


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


    /*
    def createHtmlEmailBody(df: DataFrame): String = {

      val columnNames = df.columns.map(x => "<th>" + x.trim + "</th>").mkString
      val data = df.collect().mkString
      val data1 = data.split(",").map(x => "<td>".concat(x).concat("</td>"))
      val data2 = data1.mkString.replaceAll("<td>\\[", "<tr><td>")
      val data3 = data2.mkString.replaceAll("]\\[", "</td></tr><td>").replaceAll("]", "")

      val msg =
        s"""<!DOCTYPE html>
           |<html>
           |   <head>
           |      <style>
           |         table {
           |            border: 1px solid black;
           |         }
           |         th {
           |          border: 1px solid black;
           |          background-color: #FFA;
           |          }
           |         td {
           |          border: 1px solid black;
           |          background-color: #FFF;
           |          }
           |      </style>
           |   </head>
           |
           |   <body>
           |      <h1>Report</h1>
           |
           |         <br> $columnNames </br> $data3
           |
           |   </body>
           |</html>""".stripMargin

      msg
    }
     */

    /*
    def getPathOfCSVFiles(dir: String): List[String] = {
      val d = new File(dir)
      var path = new ListBuffer[String]()
      if (d.exists) {
        val a = d.listFiles
        a.foreach { x =>
          val CSV: Boolean = x.toString.split("/").last.contains(".csv")
          val CRC: Boolean = x.toString.split("/").last.contains(".crc")

          if (CSV && !CRC) {
            path += x.toString.trim
          }
        }
      } else {
        println("Path " + d + " does not exists!!!!!!!!!!!!")
      }
      path.toList
    }
     */

    //val OutputPath = "~/scalaProjects/JDP/src/main/resources"
    //val OutputPath = "./home/scala/src/main/resources"
    //val ListFiles = getPathOfCSVFiles(OutputPath).mkString(";")
    //val msg = createHtmlEmailBody(dfForEmail)
    val header = columnsSeq.map(c => c + " | ").mkString
    val msg = header + "\r\n" + createEmailBody(dfForEmail)

    val obj = new Email("/home/scala/src/main/resources/application-mail.conf")
    val spark: SparkSession = SparkSession.builder().appName("Spark Mail Job").master("local[*]").getOrCreate()
    obj.sendMail(msg, spark.sparkContext.applicationId, "test", "R", "", "")

  }
}
