package com.aula.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import com.aula.spark.base.SparkApp

object Particionador extends App {

    // Cria uma sessão
    val spark =
        SparkSession
            .builder()
            .master("local[*]")
            .appName("Contagem de formas de pagamento")
            .getOrCreate()

    import spark.implicits._

    spark
        .read
        .format("csv")
        .option("header", "true")
        .load("../olist/olist_order_payments_dataset.csv")
        .write
            .mode(SaveMode.Overwrite)
            .partitionBy("payment_type")
            .json("../output/meio_de_pagamento_particionado")

    spark.close
}
