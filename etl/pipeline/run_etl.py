from etl.extraction.vendas.loader import SalesLoader
from etl.extraction.produtos.loader import ProductsLoader
from etl.extraction.clientes.loader import CustomersLoader
from etl.extraction.generic_extractor import GenericExtractor
from etl.transformations.sales_id_correction import fix_sale_unique_id
from etl.loaders.sql.postgres_loader import PostgresLoader
from etl.transformations.top_clients import melhores_dias_20_clientes
from etl.transformations.validar_vendas import validar_vendas_vs_clientes_produtos
from etl.transformations.salvar_vendas_parquet import salvar_vendas_com_nomes_parquet
from etl.export.export_indicadores import exportar_indicadores_unico_csv
from etl.loaders.nosql.mongo_loader import replicar_clientes_para_mongodb

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
        
    sales = sales_fixed
    melhores_dias_20_clientes(sales, logger)
    validar_vendas_vs_clientes_produtos(
        sales, customers, products, logger, output_dir="output"
    )
    
    salvar_vendas_com_nomes_parquet(sales, products, customers, logger)
    logger.info("salvar_vendas_com_nomes_parquet sucesso.")
    
    pg_loader = PostgresLoader(spark, logger)
    pg_loader.save(sales, "sales")
    pg_loader.save(products, "products")
    pg_loader.save(customers, "customers")
    logger.info("Todos os dados salvos no PostgreSQL.")
    
    exportar_indicadores_unico_csv(spark, logger)
    
    replicar_clientes_para_mongodb(customers, logger)
    