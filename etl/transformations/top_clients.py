from pyspark.sql import functions as F, Window

def melhores_dias_20_clientes(sales_df, logger=None):
    sales_df = sales_df.withColumn(
        "DATA_VENDA",
        F.to_date(F.col("NUM_ANOMESDIA").cast("string"), "yyyyMMdd")
    )
    top20 = (
        sales_df.groupBy("COD_ID_CLIENTE")
        .agg(F.count("*").alias("TOTAL_COMPRAS"))
        .orderBy(F.desc("TOTAL_COMPRAS"))
        .limit(20)
    )

    vendas_top20 = sales_df.join(top20, on="COD_ID_CLIENTE", how="inner")

    vendas_top20 = vendas_top20.withColumn(
        "DIA_SEMANA",
        F.date_format(F.col("DATA_VENDA"), "u").cast("int")
    )

    compras_por_dia = (
        vendas_top20.groupBy("COD_ID_CLIENTE", "DIA_SEMANA")
        .agg(F.count("*").alias("QTD_COMPRAS"))
    )

    w = Window.partitionBy("COD_ID_CLIENTE").orderBy(F.desc("QTD_COMPRAS"))
    melhores_dias = (
        compras_por_dia.withColumn("RANK", F.row_number().over(w))
        .filter(F.col("RANK") == 1)
        .select("COD_ID_CLIENTE", "DIA_SEMANA", "QTD_COMPRAS")
        .orderBy(F.desc("QTD_COMPRAS"))
    )

    return melhores_dias
