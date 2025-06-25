from etl.extraction.vendas.loader import SalesLoader
from etl.extraction.produtos.loader import ProductsLoader
from etl.extraction.clientes.loader import CustomersLoader
from etl.extraction.generic_extractor import GenericExtractor
from etl.transformations.sales_id_correction import fix_sale_unique_id

def run_pipeline(spark, logger):
  extractor = GenericExtractor(spark)

  sales_loader = SalesLoader(spark, logger, extractor)
  products_loader = ProductsLoader(spark, logger, extractor)
  customers_loader = CustomersLoader(spark, logger, extractor)

  sales = sales_loader.load_sales()
  products = products_loader.load_products()
  customers = customers_loader.load_customers()

  logger.info("Validando e corrigindo COD_ID_VENDA_UNICO...")
  sales_fixed, bad_rows = fix_sale_unique_id(sales)
  num_corrigidas = bad_rows.count()
  logger.info(f"Identificadores de venda corrigidos: {num_corrigidas:,}")

  if num_corrigidas > 0:
    bad_rows_path = "log/bad_sales_ids.parquet"
    logger.warning(f"Salvando {num_corrigidas:,} registros corrigidos em {bad_rows_path}")
    bad_rows.write.mode("overwrite").parquet(bad_rows_path)
  sales = sales_fixed