package com.example

import Utils.getNowHoursUTC
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions.{col, first, hour, split, to_date}
import org.apache.spark.sql.types.{DecimalType, StringType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import java.io.File
import java.time.LocalDate
import java.time.format.DateTimeFormatter

// ./spark/bin/spark-submit --jars /home/scala/target/scala-2.12/jdp.jar --class "com.example.Join" --master local[4] /home/scala/target/scala-2.12/jdp_2.12-0.1.0-SNAPSHOT.jar
// ./spark/bin/spark-submit --packages org.postgresql:postgresql:42.2.5 --jars /home/scala/target/scala-2.12/jdp.jar --class "com.example.Join" --master local[4] /home/scala/target/scala-2.12/jdp_2.12-0.1.0-SNAPSHOT.jar

class Join(hivePath: String) {

  val configSpark: Config = ConfigFactory.load().getConfig("application.spark")
  val configHive: Config = ConfigFactory.load().getConfig("application.hive")
  val configMisc: Config = ConfigFactory.load().getConfig("application.misc")
  val differenceInDays: Int = configMisc.getInt("differenceInDays")
  val sparkCores: String = configSpark.getString("master")
  val checkpoint: String = configSpark.getString("checkpointLocation")
  val hiveTablePathPrefix: String = configHive.getString("hiveTablesPathPrefix")
  val configPostgres: Config = ConfigFactory.load().getConfig("application.postgres")
  val postgresDriver: String = configPostgres.getString("driver")
  val postgresUrl: String = configPostgres.getString("url")
  val postgresUser: String = configPostgres.getString("user")
  val postgresPassword: String = configPostgres.getString("password")

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

  val hiveToday: LocalDate = today.minusDays(differenceInDays)
  //val hiveYesterday: LocalDate = today.minusDays(4346)
  val hiveTodayFormattedString: String = hiveToday.format(formatterDate)
  //val hiveYesterdayFormattedString: String = hiveYesterday.format(formatterDate)
  println("hiveTodayFormattedString: " + hiveTodayFormattedString)
  //println("hiveYesterdayFormattedString: " + hiveYesterdayFormattedString)

  def getDataframeInvoices(invoicesHiveTableName: String): DataFrame = {

    // getting all invoices from main Hive table
    val dfAllInvoicesFromHive = spark.read
      .parquet(hiveTablePathPrefix + invoicesHiveTableName)
    val dfAllInvoicesFromHiveWithDateString = dfAllInvoicesFromHive
      .withColumn("invoice_date_string", dfAllInvoicesFromHive.col("invoice_date").cast(StringType))

    // filtering invoices by date: today - differenceInDays
    val dfInvoicesForOnlyToday = dfAllInvoicesFromHiveWithDateString
      .filter(col("invoice_date_string").startsWith(hiveTodayFormattedString))
      .drop("invoice_date_string")
    println(s"dfInvoicesForOnlyToday ($hiveTodayFormattedString):")
    dfInvoicesForOnlyToday.show()
    println(s"dfInvoicesForOnlyToday ($hiveTodayFormattedString) count: " + dfInvoicesForOnlyToday.count())
    println(s"dfInvoicesForOnlyToday ($hiveTodayFormattedString) schema:")
    dfInvoicesForOnlyToday.printSchema()

    val currentTimeHoursStr: String = getNowHoursUTC
    println("currentTimeHours: " + currentTimeHoursStr)
    val currentTimeHoursInt: Int = currentTimeHoursStr.toInt
    val threeHoursAgoInt: Int = currentTimeHoursInt - 3
    val twoHoursAgoInt: Int = currentTimeHoursInt - 2
    val hourAgoInt: Int = currentTimeHoursInt - 1
    val threeHoursAgoStr: String = threeHoursAgoInt + ""
    val twoHoursAgoStr: String = twoHoursAgoInt + ""
    val hourAgoStr: String = hourAgoInt + ""

    // filtering invoices by current time; previous hour and current hour;
    // later I'll modify this and involve 3 hours ago, 2 hours ago, hour ago and current hour invoices
    val dfFilteredByHour = dfInvoicesForOnlyToday
      .filter(hour(col("invoice_date")) === currentTimeHoursStr ||
              hour(col("invoice_date")) === hourAgoStr)
    println("dfFilteredByHour:")
    dfFilteredByHour.show()
    println("dfFilteredByHour count: " + dfFilteredByHour.count())
    println("dfFilteredByHour schema:")
    dfFilteredByHour.printSchema()

    dfFilteredByHour
  }

  def getAndTransformDataframeProducts(productHiveTableName: String): DataFrame = {

    // getting all products from product Hive table
    val dfAllProductsFromHive = spark.sql(s"select * from $productHiveTableName")
    val dfAllProductsFromHiveWithDateString = dfAllProductsFromHive
      .withColumn("date_string", dfAllProductsFromHive.col("date").cast(StringType))

    // filtering invoices by date: today - differenceInDays
    val dfProductsForOnlyToday = dfAllProductsFromHiveWithDateString
      .filter(col("date_string").startsWith(hiveTodayFormattedString))
      .drop("date_string")
    println(s"dfProductsForOnlyToday ($hiveTodayFormattedString):")
    dfProductsForOnlyToday.show()
    println(s"dfProductsForOnlyToday ($hiveTodayFormattedString) count: " + dfProductsForOnlyToday.count())
    println(s"dfProductsForOnlyToday ($hiveTodayFormattedString) schema:")
    dfProductsForOnlyToday.printSchema()

    // I have to aggregate products, because there are products with different unit_price within a date;
    // This way, I take first only product unit_price
    val dfProductsAggregated = dfProductsForOnlyToday
      .groupBy("stock_code", "date")
      .agg(
        first("unit_price") alias "first_unit_price",
        first("product_description") alias "first_product_description")


    val dfProductsAggregatedRenamed = dfProductsAggregated
      .withColumnRenamed("first_unit_price", "unit_price")
      .withColumnRenamed("first_product_description", "product_description")
    println("dfProductsAggregatedRenamed:")
    dfProductsAggregatedRenamed.show()
    println("dfProductsAggregatedRenamed count: " + dfProductsAggregatedRenamed.count())
    println("dfProductsAggregatedRenamed schema:")
    dfProductsAggregatedRenamed.printSchema()

    dfProductsAggregatedRenamed
  }

  def getDataframeCountries(countriesHiveTableName: String): DataFrame = {

    // getting all countries from country Hive table
    val dfAllCountriesFromHive = spark.sql(s"select * from $countriesHiveTableName")
    println("dfAllCountriesFromHive:")
    dfAllCountriesFromHive.show()
    println("dfAllCountriesFromHive count: " + dfAllCountriesFromHive.count())
    println("dfAllCountriesFromHive schema:")
    dfAllCountriesFromHive.printSchema()

    dfAllCountriesFromHive
  }

  def joinAllDataframes(dfInvoices: DataFrame, dfProducts: DataFrame, dfCountries: DataFrame): DataFrame = {

    // Building the column with only date (as opposed to date and time)
    val dfInvoicesWithDate = dfInvoices
      .withColumn("date", to_date(dfInvoices.col("invoice_date"), "M/d/yyyy"))

    // Join invoices and products, by stock_code and date
    val joinedInvoicesAndProducts = dfInvoicesWithDate.join(
      dfProducts, Seq("stock_code", "date"), "leftouter"
    )
    println("joinedInvoicesAndProducts count: " + joinedInvoicesAndProducts.count())

    // Building the columns: country_id, region_id and total_price (quantity * unit_price)
    val splitColCountry = split(joinedInvoicesAndProducts.col("country"), "-")
    val joinedInvoicesAndProductsCountryIdRegionId = joinedInvoicesAndProducts
      .withColumn("country_id", splitColCountry.getItem(0))
      .withColumn("region_id", splitColCountry.getItem(1))
      .withColumn("total_price", (col("quantity") * col("unit_price")).cast(DecimalType(8, 2)))

    // Join previous joined table and countries table, by country_id
    val joinedAll = joinedInvoicesAndProductsCountryIdRegionId.join(
      dfCountries, Seq("country_id"), "inner"
    )
    joinedAll
  }

  def writeDataframeToPostgres(dataframe: DataFrame): Unit = {

    dataframe.write
      .format("jdbc")
      .option("driver", s"$postgresDriver")
      .option("url", s"$postgresUrl")
      .option("dbtable", "joined")
      .option("user", s"$postgresUser")
      .option("password", s"$postgresPassword")
      .mode(SaveMode.Append)
      .save()
  }
}

object Join {

  def main(args: Array[String]): Unit = {

    val configHive: Config = ConfigFactory.load().getConfig("application.hive")
    val hivePathPrefix: String = configHive.getString("hiveTablesPathPrefix")

    val mainHiveTableName = "main"
    val productsHiveTableName = "product"
    val countriesHiveTableName = "country"

    val join = new Join(hivePathPrefix)

    val dfInvoices = join.getDataframeInvoices(mainHiveTableName)
    println("\n******************************************************************************************")

    val dfProducts = join.getAndTransformDataframeProducts(productsHiveTableName)
    println("\n******************************************************************************************")

    val dfCountries = join.getDataframeCountries(countriesHiveTableName)
    println("\n******************************************************************************************")
    println("******************************************************************************************")
    println("******************************************************************************************")

    val joinedAll: DataFrame = join.joinAllDataframes(dfInvoices, dfProducts, dfCountries)
    println("joinedAll:")
    joinedAll.show(50)
    println("joinedAll count: " + joinedAll.count())
    println("joinedAll schema:")
    joinedAll.printSchema()

    println("\n******************************************************************************************")
    println("******************************************************************************************")
    println("******************************************************************************************")

    println("SAVING TO POSTGRES")
    join.writeDataframeToPostgres(joinedAll)
  }
}
