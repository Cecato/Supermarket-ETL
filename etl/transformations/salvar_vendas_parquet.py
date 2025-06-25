from pyspark.sql import functions as F

def salvar_vendas_com_nomes_parquet(sales, products, customers, logger, output_dir="output/vendas_parquet"):
  vendas_prod = sales.join(
    products.select("COD_ID_PRODUTO", "DES_PRODUTO"),
    on="COD_ID_PRODUTO", how="left"
  )
  vendas_completo = vendas_prod.join(
    customers.select("COD_ID_CLIENTE", "NOM_NOME"),
    on="COD_ID_CLIENTE", how="left"
  )

  vendas_completo = vendas_completo.withColumn(
    "ANO", F.expr("int(NUM_ANOMESDIA / 10000)")
  ).withColumn(
    "MES", F.expr("int((NUM_ANOMESDIA % 10000) / 100)")
  )

  logger.info("Salvando vendas com nome do produto e cliente em formato Parquet particionado por ano e mês.")
  vendas_completo.write.mode("overwrite").partitionBy("ANO", "MES").parquet(output_dir)
  logger.info(f"Arquivo Parquet salvo em {output_dir}")
