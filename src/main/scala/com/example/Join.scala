package com.example

import Utils.{extractRegionName, getNowHoursUTC}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions.{col, collect_list, count, first, hour, lit, sort_array, split, sqrt, sum, to_date, udf, unix_timestamp}
import org.apache.spark.sql.types.{DecimalType, DoubleType, IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import java.io.File
import java.time.{LocalDate, LocalTime}
import java.time.format.DateTimeFormatter

// ./spark/bin/spark-submit --jars /home/scala/target/scala-2.12/jdp.jar --class "com.example.Join" --master local[4] /home/scala/target/scala-2.12/jdp_2.12-0.1.0-SNAPSHOT.jar
// ./spark/bin/spark-submit --packages org.postgresql:postgresql:42.2.5 --jars /home/scala/target/scala-2.12/jdp.jar --class "com.example.Join" --master local[4] /home/scala/target/scala-2.12/jdp_2.12-0.1.0-SNAPSHOT.jar
// ./spark/bin/spark-submit --packages org.postgresql:postgresql:42.2.5 --jars /home/scala/target/scala-2.12/jdp.jar,/home/scala/target/scala-2.12/Scala_Spark_Mail.jar --class "com.example.Join" --master local[2] /home/scala/target/scala-2.12/jdp_2.12-0.1.0-SNAPSHOT.jar

class Join(hivePath: String) {

  val configSpark: Config = ConfigFactory.load().getConfig("application.spark")
  val configHive: Config = ConfigFactory.load().getConfig("application.hive")
  val configMisc: Config = ConfigFactory.load().getConfig("application.misc")
  //val configEmail: Config = ConfigFactory.load("/home/scala/src/main/resources/application-mail.conf").getConfig("application-mail")
  val differenceInDays: Int = configMisc.getInt("differenceInDays")
  val sparkCores: String = configSpark.getString("master")
  val checkpoint: String = configSpark.getString("checkpointLocation")
  val hiveTablePathPrefix: String = configHive.getString("hiveTablesPathPrefix")
  val configPostgres: Config = ConfigFactory.load().getConfig("application.postgres")
  val postgresDriver: String = configPostgres.getString("driver")
  val postgresUrl: String = configPostgres.getString("url")
  val postgresUser: String = configPostgres.getString("user")
  val postgresPassword: String = configPostgres.getString("password")
  val postgresTableName: String = configPostgres.getString("dbtable")

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
  val hiveYesterday: LocalDate = today.minusDays(differenceInDays + 1)
  val hiveTodayFormattedString: String = hiveToday.format(formatterDate)
  val hiveYesterdayFormattedString: String = hiveYesterday.format(formatterDate)
  println("hiveTodayFormattedString: " + hiveTodayFormattedString)
  println("hiveYesterdayFormattedString: " + hiveYesterdayFormattedString)

  def getDataframeInvoices(invoicesHiveTableName: String): DataFrame = {

    try {
      val currentTimeHoursStr: String = getNowHoursUTC
      println("currentTimeHours: " + currentTimeHoursStr)
      val currentTimeHoursInt: Int = LocalTime.now().getHour

      // getting all invoices from main Hive table
      val dfAllInvoicesFromHive = spark.read
        .parquet(hiveTablePathPrefix + invoicesHiveTableName)
      val dfAllInvoicesFromHiveWithDateString = dfAllInvoicesFromHive
        .withColumn("invoice_date_string", dfAllInvoicesFromHive.col("invoice_date").cast(StringType))

      // filtering invoices by date: today - differenceInDays
      var todayOrYesterday = ""
      //val nulaSati = 0
      if (currentTimeHoursInt == 0) todayOrYesterday += hiveYesterdayFormattedString
      else todayOrYesterday += hiveTodayFormattedString
      val dfInvoicesForOnlyToday = dfAllInvoicesFromHiveWithDateString
        //.filter(col("invoice_date_string").startsWith(hiveTodayFormattedString))
        .filter(col("invoice_date_string").startsWith(todayOrYesterday))
        .drop("invoice_date_string")


      println(s"dfInvoicesForOnlyToday ($hiveTodayFormattedString):")
      dfInvoicesForOnlyToday.show()
      println(s"dfInvoicesForOnlyToday ($hiveTodayFormattedString) count: " + dfInvoicesForOnlyToday.count())
      println(s"dfInvoicesForOnlyToday ($hiveTodayFormattedString) schema:")
      dfInvoicesForOnlyToday.printSchema()


      //val currentTimeHoursInt: Int = currentTimeHoursStr.toInt
      //val fourHoursAgoInt: Int = currentTimeHoursInt - 4
      //val threeHoursAgoInt: Int = currentTimeHoursInt - 3
      //val twoHoursAgoInt: Int = currentTimeHoursInt - 2
      //val hourAgoInt: Int = currentTimeHoursInt - 1
      //val fourHoursAgoStr: String = fourHoursAgoInt + ""
      //val threeHoursAgoStr: String = threeHoursAgoInt + ""
      //val twoHoursAgoStr: String = twoHoursAgoInt + ""
      //val hourAgoStr: String = hourAgoInt + ""

      // filtering invoices by current time;
      // I involve 4 hours ago, 3 hours ago, 2 hours ago, and hour ago invoices
      /*
      val dfFilteredByHour = dfInvoicesForOnlyToday
        .filter(
          // hour(col("invoice_date")) === currentTimeHoursStr
          //    ||
          hour(col("invoice_date")) === hourAgoStr
            ||
            hour(col("invoice_date")) === twoHoursAgoStr
            ||
            hour(col("invoice_date")) === threeHoursAgoStr
            ||
            hour(col("invoice_date")) === fourHoursAgoStr
        )
       */

      val currentTimeUnixHours = System.currentTimeMillis() / (1000 * 3600)
      println("Current UTC unix hours: " + currentTimeUnixHours)

      val currentTimeWithDifferenceInDaysUnixHours = currentTimeUnixHours - differenceInDays * 24
      println("Current UTC time with 4372 difference in days unix hours: " + currentTimeWithDifferenceInDaysUnixHours)

      val dfWithUnix = dfInvoicesForOnlyToday
        .withColumn("unix_hours", (unix_timestamp(col("invoice_date"), "yyyy-M-d HH:mm:ss") / 3600).cast(LongType))

      println("dfWithUnix:")
      dfWithUnix.show()
      println("dfWithUnix count: " + dfWithUnix.count())
      println("dfWithUnix schema:")
      dfWithUnix.printSchema()

      //println("1 hour before Current time Unix hours: " + (currentTimeUnixHours - 1) + "")

      val dfFilteredByHourWithUnix = dfWithUnix
        .filter(
          col("unix_hours") === (currentTimeWithDifferenceInDaysUnixHours - 1) + ""
          ||
            col("unix_hours") === (currentTimeWithDifferenceInDaysUnixHours - 2) + ""
          ||
            col("unix_hours") === (currentTimeWithDifferenceInDaysUnixHours - 3) + ""
          ||
            col("unix_hours") === (currentTimeWithDifferenceInDaysUnixHours - 4) + ""
          //||
          //    col("unix_hours") === (currentTimeWithDifferenceInDaysUnixHours - 5) + ""
          //col("unix_hours") === (currentTimeWithDifferenceInDaysUnixHours - 12) + ""
        )

      println("dfFilteredByHourWithUnix:")
      dfFilteredByHourWithUnix.show()
      println("dfFilteredByHourWithUnix count: " + dfFilteredByHourWithUnix.count())
      println("dfFilteredByHourWithUnix schema:")
      dfFilteredByHourWithUnix.printSchema()

      val dfFilteredByHour = dfFilteredByHourWithUnix
        .drop("unix_hours")

      dfFilteredByHour
    } catch {
      case e: Exception => println("Join, " +
        "def getDataframeInvoices(invoicesHiveTableName: String): DataFrame, " +
        "error occurred: " + e)
        import org.apache.spark.sql.Row
        val emptyDf: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], StructType(Seq()))
        emptyDf
    }
  }

  def getAndTransformDataframeProducts(productHiveTableName: String): DataFrame = {

    try {
      val currentTimeHoursInt: Int = LocalTime.now().getHour

      // getting all products from product Hive table
      val dfAllProductsFromHive = spark.sql(s"select * from $productHiveTableName")
      val dfAllProductsFromHiveWithDateString = dfAllProductsFromHive
        .withColumn("date_string", dfAllProductsFromHive.col("date").cast(StringType))

      // filtering invoices by date: today - differenceInDays
      var todayOrYesterday = ""
      //val nulaSati = 0
      if (currentTimeHoursInt == 0) todayOrYesterday += hiveYesterdayFormattedString
      else todayOrYesterday += hiveTodayFormattedString
      val dfProductsForOnlyToday = dfAllProductsFromHiveWithDateString
        //.filter(col("date_string").startsWith(hiveTodayFormattedString))
        .filter(col("date_string").startsWith(todayOrYesterday))
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
    } catch {
      case e: Exception => println("Join, " +
        "def getAndTransformDataframeProducts(productHiveTableName: String): DataFrame, " +
        "error occurred: " + e)
        import org.apache.spark.sql.Row
        val emptyDf: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], StructType(Seq()))
        emptyDf
    }
  }

  def getDataframeCountries(countriesHiveTableName: String): DataFrame = {

    try {
      // getting all countries from country Hive table
      val dfAllCountriesFromHive = spark.sql(s"select * from $countriesHiveTableName")
      println("dfAllCountriesFromHive:")
      dfAllCountriesFromHive.show()
      println("dfAllCountriesFromHive count: " + dfAllCountriesFromHive.count())
      println("dfAllCountriesFromHive schema:")
      dfAllCountriesFromHive.printSchema()

      dfAllCountriesFromHive
    } catch {
      case e: Exception => println("Join, " +
        "def getDataframeCountries(countriesHiveTableName: String): DataFrame, " +
        "error occurred: " + e)
        import org.apache.spark.sql.Row
        val emptyDf: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], StructType(Seq()))
        emptyDf
    }
  }

  def joinAllDataframes(dfInvoices: DataFrame, dfProducts: DataFrame, dfCountries: DataFrame): DataFrame = {

    try {
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
        .withColumn("region", extractRegionName(col("region_id")))

      // Join previous joined table and countries table, by country_id
      val joinedAll = joinedInvoicesAndProductsCountryIdRegionId.join(
        dfCountries, Seq("country_id"), "inner"
      )
      joinedAll
    } catch {
      case e: Exception => println("Join, " +
        "def joinAllDataframes(dfInvoices: DataFrame, dfProducts: DataFrame, dfCountries: DataFrame): DataFrame, " +
        "error occurred: " + e)
        import org.apache.spark.sql.Row
        val emptyDf: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], StructType(Seq()))
        emptyDf
    }
  }

  /*
  def extractInvoicesForEmailMean(joinedAllDf: DataFrame): DataFrame = {

    // first, extract whole "old" data from Postgres
    val fromPostgresDf = spark.read
      .format("jdbc")
      .option("driver", s"$postgresDriver")
      .option("url", s"$postgresUrl")
      .option("dbtable", "joined")
      .option("user", s"$postgresUser")
      .option("password", s"$postgresPassword")
      .load()
    println("fromPostgresDf:")
    fromPostgresDf.show()
    println("fromPostgresDf count: " + fromPostgresDf.count())

    // then, calculate mean and standard deviation per stock_code for old Postgres data
    val withMeanDf: DataFrame = fromPostgresDf
      .select("stock_code", "quantity")
      .groupBy("stock_code")
      .agg(avg(col("quantity")) alias "qty_avg")
      //.withColumnRenamed("qty_avg", "quantity")
    println("withMeanDf:")
    withMeanDf.show()
    println("withMeanDf count: " + withMeanDf.count())

    val joined: DataFrame = fromPostgresDf.join(
      withMeanDf, Seq("stock_code"), "inner"
    )
    println("joined:")
    joined.show()
    println("joined1 count: " + joined.count())

    val withStandardDeviation: DataFrame = joined
      .select("stock_code", "quantity", "qty_avg")
      //.select("stock_code", "quantity")
      .groupBy("stock_code")
      .agg(
        sqrt(sum((col("quantity") - col("qty_avg")) * (col("quantity") - col("qty_avg"))) / count("stock_code"))
          alias "standard_deviation"
      , avg("quantity") alias "quantity_avg")
    println("withStandardDeviation:")
    withStandardDeviation.show(46)
    println("withStandardDeviation count: " + withStandardDeviation.count())

    // join new data with withStandardDeviation dataframe
    val newAndOldDf: DataFrame = withStandardDeviation.join(
      joinedAllDf, Seq("stock_code"), "inner"
    )
    println("newAndOldDf:")
    println("newAndOldDf count: " + newAndOldDf.count())
    newAndOldDf.show(71)

    // and filter newAndOldDf, so that only remain rows for email
    val forEmailDf = newAndOldDf
      .filter(col("quantity").cast(DoubleType) > lit(2.0) * col("standard_deviation") + col("quantity_avg"))
    println("forEmailDf:")
    forEmailDf.show()
    println("forEmailDf count: " + forEmailDf.count())

    forEmailDf
  }
   */

  def extractInvoicesForEmailMedian(joinedAllDf: DataFrame): DataFrame = {

    try {
      // first, extract whole "old" data from Postgres
      val fromPostgresDf = spark.read
        .format("jdbc")
        .option("driver", s"$postgresDriver")
        .option("url", s"$postgresUrl")
        .option("dbtable", s"$postgresTableName")
        .option("user", s"$postgresUser")
        .option("password", s"$postgresPassword")
        .load()
      println("fromPostgresDf:")
      fromPostgresDf.show()
      println("fromPostgresDf count: " + fromPostgresDf.count())

      // then, calculate median and standard deviation per stock_code for old Postgres data
      val withMedianArrayDf: DataFrame = fromPostgresDf
        .select("stock_code", "quantity")
        .groupBy("stock_code")
        .agg(sort_array(collect_list(col("quantity"))) alias "median_array")

      println("withMedianArrayDf:")
      withMedianArrayDf.show()
      println("withMedianArrayDf schema:")
      withMedianArrayDf.printSchema()

      val arrayToInt = (arr: scala.collection.mutable.Seq[Int]) => {
        if (arr.length % 2 == 1) arr(arr.length / 2)
        else (arr(arr.length / 2 - 1) + arr(arr.length / 2)) / 2
      }

      val arrayToIntUDF = udf(arrayToInt)

      val withMedianIntDf: DataFrame = withMedianArrayDf
        .withColumn("median_int", arrayToIntUDF(withMedianArrayDf.col("median_array")))
      println("withMedianIntDf:")
      withMedianIntDf.show()
      println("withMedianIntDf count: " + withMedianIntDf.count())

      val joined: DataFrame = fromPostgresDf.join(
        withMedianIntDf, Seq("stock_code"), "inner"
      )
      println("joined:")
      joined.show()
      println("joined count: " + joined.count())

      val withStandardDeviation: DataFrame = joined
        .select("stock_code", "quantity", "median_array", "median_int")
        .groupBy("stock_code")
        .agg(
          sqrt(sum((col("quantity") - col("median_int")) * (col("quantity") - col("median_int"))) / count("stock_code"))
            alias "standard_deviation"
          , first(col("median_int")) alias "quantity_median")
      println("withStandardDeviation:")
      withStandardDeviation.show(46)
      println("withStandardDeviation count: " + withStandardDeviation.count())

      // join new data with withStandardDeviation dataframe
      val newAndOldDf: DataFrame = withStandardDeviation.join(
        joinedAllDf, Seq("stock_code"), "inner"
      )
      println("newAndOldDf:")
      println("newAndOldDf count: " + newAndOldDf.count())
      newAndOldDf.show(71)

      // and filter newAndOldDf, so that only remain rows for email
      val forEmailDf = newAndOldDf
        .filter(col("quantity").cast(DoubleType) > lit(2.0) * col("standard_deviation") + col("quantity_median"))
      println("forEmailDf:")
      forEmailDf.show()
      println("forEmailDf count: " + forEmailDf.count())

      forEmailDf
    } catch {
      case e: Exception => println("Join, " +
        "def extractInvoicesForEmailMedian(joinedAllDf: DataFrame): DataFrame, " +
        "error occurred: " + e)
        import org.apache.spark.sql.Row
        val emptyDf: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], StructType(Seq()))
        emptyDf
    }
  }


  def sendEmail(forEmailDf: DataFrame): Unit = {

    try {
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

      val columnsSeq = Seq("stock_code", "standard_deviation", "quantity_median", "country_id", "date", "invoice_no",
        "customer_id", "country", "invoice_date", "quantity", "unit_price", "product_description", "region_id",
        "total_price", "country_name")

      val header = columnsSeq.map(c => c + " | ").mkString

      val msg = header + "\n" + createEmailBody(forEmailDf)

      val obj = new Email("/home/scala/src/main/resources/application-mail.conf")
      //val obj = new Email(s"$configEmail")
      val spark: SparkSession = SparkSession.builder().appName("Spark Mail Job").master("local[4]").getOrCreate()
      obj.sendMail(msg, spark.sparkContext.applicationId, "test", "R", "", "")
    } catch {
      case e: Exception =>
        println("Join, " +
          "def sendEmail(forEmailDf: DataFrame): Unit, " +
          "error occurred: " + e)
    }
  }

  def writeDataframeToPostgres(dataframe: DataFrame): Unit = {

    try {
      // I don't want to save rows with unit_price 0.0 or customer_id null
      val filteredDf = dataframe.filter(col("total_price") =!= 0.0 &&
        col("customer_id").isNotNull &&
        col("quantity").gt(0))
      println("filteredDf:")
      filteredDf.show()
      println("filteredDf count: " + filteredDf.count())

      filteredDf.write
        .format("jdbc")
        .option("driver", s"$postgresDriver")
        .option("url", s"$postgresUrl")
        .option("dbtable", s"$postgresTableName")
        .option("user", s"$postgresUser")
        .option("password", s"$postgresPassword")
        .mode(SaveMode.Append)
        .save()
    } catch {
      case e: Exception =>
        println("Join, " +
          "def writeDataframeToPostgres(dataframe: DataFrame): Unit, " +
          "error occurred: " + e)
    }
  }
}

object Join {

  def main(args: Array[String]): Unit = {

    try {
      val configHive: Config = ConfigFactory.load().getConfig("application.hive")
      val hivePathPrefix: String = configHive.getString("hiveTablesPathPrefix")

      val mainHiveTableName = "main"
      val productsHiveTableName = "product"
      val countriesHiveTableName = "country"

      val join = new Join(hivePathPrefix)

      val dfInvoices = join.getDataframeInvoices(mainHiveTableName)
      //println("\n******************************************************************************************")

      val dfProducts = join.getAndTransformDataframeProducts(productsHiveTableName)
      //println("\n******************************************************************************************")

      val dfCountries = join.getDataframeCountries(countriesHiveTableName)
      //println("\n******************************************************************************************")
      //println("******************************************************************************************")
      //println("******************************************************************************************")

      val joinedAll: DataFrame = join.joinAllDataframes(dfInvoices, dfProducts, dfCountries)
      println("joinedAll:")
      joinedAll.show(50)
      println("joinedAll count: " + joinedAll.count())
      println("joinedAll schema:")
      joinedAll.printSchema()

      //println("\n******************************************************************************************")
      //println("******************************************************************************************")
      //println("******************************************************************************************")


      println("CALCULATING MEDIAN AND STANDARD DEVIATION ")
      //val forEmailMeanDf = join.extractInvoicesForEmailMean(joinedAll)
      val forEmailMedianDf = join.extractInvoicesForEmailMedian(joinedAll)
      println("\nSENDING EMAIL")

      //if (!forEmailMeanDf.isEmpty) join.sendEmail(forEmailMeanDf)
      //else println("Dataframe mean for email is empty")

      if (!forEmailMedianDf.isEmpty) join.sendEmail(forEmailMedianDf)
      else println("Dataframe median for email is empty")

      println("\nSAVING TO POSTGRES")
      join.writeDataframeToPostgres(joinedAll)


    } catch {
      case e: Exception => println("Join, " +
        "def main(args: Array[String]): Unit, " +
        "error occurred: " + e)
    }
  }
}
