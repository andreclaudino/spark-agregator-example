package com.aula.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import com.aula.spark.base.SparkApp

object ContaPaymentType extends SparkApp("Conta payment type") {

    val meio_de_pagamento = spark.read.json("../output/meio_de_pagamento_particionado")

    meio_de_pagamento
        .groupBy("payment_type")
        .count()
    .write
        .mode(SaveMode.Overwrite)
        .json("../output/contagem_por_meio_de_pagamento")

    spark.close
}