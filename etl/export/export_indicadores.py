from etl.loaders.sql.postgres_loader import PostgresLoader
from etl.loaders.sql.queries.vendas import (
  quantidade_vendas_por_dia,
  produtos_distintos_por_dia)
from etl.loaders.sql.queries.produtos import (
  produtos_mais_vendidos,
  produtos_maior_desconto)
from etl.loaders.sql.queries.clientes import clientes_com_mais_compras
import os
import shutil
import glob
from pyspark.sql import functions as F

def exportar_indicadores_unico_csv(spark, logger):
  pg_loader = PostgresLoader(spark, logger)

  queries = [
    ("PRODUTOS_MAIS_VENDIDOS", produtos_mais_vendidos()),
    ("CLIENTES_COM_MAIS_COMPRAS", clientes_com_mais_compras()),
    ("QUANTIDADE_VENDAS_POR_DIA", quantidade_vendas_por_dia()),
    ("PRODUTOS_DISTINTOS_POR_DIA", produtos_distintos_por_dia()),
    ("PRODUTOS_MAIOR_DESCONTO", produtos_maior_desconto()),
  ]

  dfs = []
  all_columns = set()
  temp_dfs = []
  for nome, sql in queries:
    logger.info(f"Executando indicador: {nome}")
    df = pg_loader.read_query(sql)
    df = df.withColumn("INDICADOR", F.lit(nome))
    all_columns.update(df.columns)
    temp_dfs.append(df)

  all_columns = list(all_columns)
  dfs = []
  for df in temp_dfs:
    missing_cols = set(all_columns) - set(df.columns)
    for col in missing_cols:
      df = df.withColumn(col, F.lit(None))
    df = df.select(*all_columns)
    dfs.append(df)

  df_final = dfs[0]
  for df in dfs[1:]:
    df_final = df_final.unionByName(df)

  temp_dir = "output/indicadores_temp"
  output_file = "output/indicadores_unico.csv"

  if not os.path.exists('output'):
    os.makedirs('output')

  df_final.coalesce(1).write.mode("overwrite").csv(temp_dir, header=True)

  csv_files = glob.glob(os.path.join(temp_dir, 'part-*'))
  if csv_files:
    shutil.move(csv_files[0], output_file)

  shutil.rmtree(temp_dir)
  logger.info(f"Todos os indicadores exportados em: {output_file}")
