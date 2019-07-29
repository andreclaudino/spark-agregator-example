package com.aula.spark.base

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

class SparkApp(appName:String) extends App {

    // Cria uma sessão
    val spark =
        SparkSession
            .builder()
            .master("local[*]")
            .appName(appName)
            .getOrCreate()

    import spark.implicits._
}