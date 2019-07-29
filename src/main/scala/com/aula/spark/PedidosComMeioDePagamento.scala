package com.aula.spark

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

import com.aula.spark.base.SparkApp

object PedidosComMeioDePagamento 
        extends SparkApp("Pedidos com meio de pagamento") {
    
    val pedido =
        spark
            .read
            .format("csv")
            .option("header", "true")
            .load("../olist/olist_orders_dataset.csv")
            .withColumnRenamed("order_purchase_timestamp", "data")
            .withColumnRenamed("order_approved_at", "data_aprovacao")
            .withColumnRenamed("order_Status", "status_pedido")
            .withColumnRenamed("customer_id", "id_cliente")
            .select("order_id", "id_cliente", "status_pedido", "data", "data_aprovacao")
            .withColumn("data", to_date(col("data")))
            .withColumn("data_aprovacao", to_date(col("data_aprovacao")))
            .repartition(4)
    
    val meio_de_pagamento =
            spark
                .read
                .json("../output/meio_de_pagamento_particionado")
                .withColumnRenamed("payment_type", "meio_de_pagamento")
                .select("order_id", "meio_de_pagamento")

    val pedido_com_meio_de_pagamento =
            pedido
                .join(meio_de_pagamento, "order_id")
                .withColumnRenamed("order_id", "id_pedido")

    pedido_com_meio_de_pagamento
            .write
            .mode(SaveMode.Overwrite)
            .partitionBy("data", "status_pedido", "meio_de_pagamento")
            .json("../output/pedido_com_meio_de_pagamento")
    
    spark.close
}