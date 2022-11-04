package com.example

import org.apache.spark.sql.expressions.UserDefinedFunction

import java.time.LocalTime
import java.time.format.DateTimeFormatter

object Utils {

  val countriesMap: Map[String, String] = Map(
    "1000" -> "France",
    "1001" -> "United Kingdom",
    "1002" -> "Netherlands",
    "1102" -> "Austria",
    "1104" -> "Belgium",
    "1108" -> "Cyprus",
    "1109" -> "Czech Republic",
    "1110" -> "Denmark",
    "1112" -> "Finland",
    "1113" -> "Germany",
    "1114" -> "Greece",
    "1116" -> "Iceland",
    "1117" -> "Ireland",
    "1118" -> "Italy",
    "1121" -> "Lithuania",
    "1123" -> "Malta",
    "1128" -> "Norway",
    "1129" -> "Poland",
    "1130" -> "Portugal",
    "1136" -> "Spain",
    "1137" -> "Sweden",
    "1138" -> "Switzerland",
    "1141" -> "European Community",
    "2245" -> "RSA",
    "3303" -> "Bahrain",
    "3308" -> "China",
    "3314" -> "Israel",
    "3315" -> "Japan",
    "3320" -> "Lebanon",
    "3332" -> "Saudi Arabia",
    "3333" -> "Singapore",
    "3342" -> "United Arab Emirates",
    "4400" -> "Australia",
    "5504" -> "Canada",
    "5522" -> "USA",
    "6602" -> "Brazil",
    "7777" -> "Unspecified"
  )

  val regionsMap: Map[String, String] = Map(
    "1" -> "North",
    "2" -> "South",
    "3" -> "East",
    "4" -> "West",
    "5" -> "Center",
    "6" -> "Islands or peripheral territories"
  )

  import org.apache.spark.sql.functions.udf

  def extractCountryName: UserDefinedFunction = {
    udf((key: String) => countriesMap.get(key))
  }

  def extractRegionName: UserDefinedFunction = {
    udf((key: String) => regionsMap.get(key))
  }

  def getNowHoursUTC: String = {

    val now: LocalTime = LocalTime.now()
    val formatterTime = DateTimeFormatter.ofPattern("H:mm")
    val nowFormattedStr: String = now.format(formatterTime)
    val nowFormattedStrings: Array[String] = nowFormattedStr.split(":")
    val nowHoursStr: String = nowFormattedStrings(0)
    nowHoursStr
  }
}
