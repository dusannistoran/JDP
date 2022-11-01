package com.example

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions.{asc, col, count, countDistinct, desc, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import java.io.File

// ./spark/bin/spark-submit --packages org.postgresql:postgresql:42.2.5 --jars /home/scala/target/scala-2.12/jdp.jar --class "com.example.Queries" --master local[4] /home/scala/target/scala-2.12/jdp_2.12-0.1.0-SNAPSHOT.jar

class Queries {

  val configSpark: Config = ConfigFactory.load().getConfig("application.spark")
  val configHive: Config = ConfigFactory.load().getConfig("application.hive")
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

  def getDataframeFromPostgres(): DataFrame = {

    val df = spark.read
      .format("jdbc")
      .option("driver", s"$postgresDriver")
      .option("url", s"$postgresUrl")
      .option("dbtable", "joined")
      .option("user", s"$postgresUser")
      .option("password", s"$postgresPassword")
      .load()
    df
  }
}

object Queries {

  def main(args: Array[String]): Unit = {

    val queries = new Queries()

    val dfFromPostgres: DataFrame = queries.getDataframeFromPostgres()
    println("dfFromPostgres:")
    dfFromPostgres.show()
    println("dfFromPostgres count: " + dfFromPostgres.count())
    println("dfFromPostgres schema:")
    dfFromPostgres.printSchema()

    // TOP BUYERS PER USER
    val topBuyersPerUser = dfFromPostgres
      .filter(col("customer_id").isNotNull)
      .groupBy("customer_id")
      .agg(sum("total_price") as "sum")
      .orderBy(desc("sum"))
    println("TOP BUYERS PER USER:")
    topBuyersPerUser.show()
    println("topBuyersPerUser count: " + topBuyersPerUser.count())


    // TOP BUYERS PER COUNTRY REGION
    val topBuyersPerCountryRegion = dfFromPostgres
      .filter(col("customer_id").isNotNull)
      .groupBy("customer_id", "country_id", "country_name", "region_id")
      .agg(sum("total_price") as "sum")
      .orderBy(desc("sum"))
    println("TOP BUYERS PER COUNTRY REGION:")
    topBuyersPerCountryRegion.show()
    println("topBuyersPerCountryRegion count: " + topBuyersPerCountryRegion.count())

    // USERS WHO PLACE ORDERS FROM DIFFERENT REGIONS OF THE SAME COUNTRY
    val aggregatingCols = Seq("customer_id", "country_id", "country_name")
    val usersWhoPlaceOrdersFromDifferentRegionsOfTheSameCountry = dfFromPostgres
      .filter(col("customer_id").isNotNull)
      .groupBy(aggregatingCols.head, aggregatingCols.tail:_*)
      .agg(countDistinct("region_id").as("distinct_regions"))
      .select("distinct_regions", aggregatingCols:_*)
      .join(dfFromPostgres, usingColumns = aggregatingCols)
      .orderBy(desc("distinct_regions"))
      .where(col("distinct_regions").gt(1))

    println("USERS WHO PLACE ORDERS FROM DIFFERENT REGIONS OF THE SAME COUNTRY:")
    usersWhoPlaceOrdersFromDifferentRegionsOfTheSameCountry.show(151)
    println("usersWhoPlaceOrdersFromDifferentRegionsOfTheSameCountry count: " +
      usersWhoPlaceOrdersFromDifferentRegionsOfTheSameCountry.count())

    // USERS WHO ORDER FROM MORE THAN ONE COUNTRY
    val usersWhoOrderFromMoreThanOneCountry = dfFromPostgres
      .filter(col("customer_id").isNotNull)
      .groupBy("customer_id")
      .agg(countDistinct("country_name").as("distinct_countries"))
      .select("distinct_countries", "customer_id")
      .join(dfFromPostgres, usingColumn = "customer_id")
      .orderBy(desc("distinct_countries"))
      .where(col("distinct_countries").gt(1))

    println("USERS WHO ORDER FROM MORE THAN ONE COUNTRY:")
    usersWhoOrderFromMoreThanOneCountry.show()
    println("usersWhoOrderFromMoreThanOneCountry count: " + usersWhoOrderFromMoreThanOneCountry.count())

    // HOW MANY USERS HAVE PLACED ONE ORDER
    val usersWhoHavePlacedOneOrder = dfFromPostgres
      .filter(col("customer_id").isNotNull)
      .groupBy("customer_id")
      .agg(count("*").as("cnt"))
      .orderBy(asc("customer_id"))
      .where(col("cnt").equalTo(1))
      //.withColumn("num_of_customers", count("*"))

    println("HOW MANY USERS HAVE PLACED ONE ORDER:")
    usersWhoHavePlacedOneOrder.show()
    println("usersWhoHavePlacedOneOrder count: " + usersWhoHavePlacedOneOrder.count())

    // HOW MANY USERS HAVE PLACED MORE THAN ONE ORDER
    val usersWhoHavePlacedMoreThanOneOrder = dfFromPostgres
      .filter(col("customer_id").isNotNull)
      .groupBy("customer_id")
      .agg(count("*").as("cnt"))
      .orderBy(desc("cnt"), asc("customer_id"))
      .where(col("cnt").gt(1))

    println("HOW MANY USERS HAVE PLACED MORE THAN ONE ORDER:")
    usersWhoHavePlacedMoreThanOneOrder.show()
    println("usersWhoHavePlacedMoreThanOneOrder count: " + usersWhoHavePlacedMoreThanOneOrder.count())

  }
}
