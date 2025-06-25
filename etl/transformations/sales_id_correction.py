from pyspark.sql.functions import col, split, concat_ws, monotonically_increasing_id

def fix_sale_unique_id(sales_df):

    sales_df = sales_df.withColumn("_ROW_ID", monotonically_increasing_id())
    
    sales_df = sales_df.withColumn("LOJA_SPLIT", split(col("COD_ID_VENDA_UNICO"), "\|").getItem(0)) \
                       .withColumn("CUPOM_SPLIT", split(col("COD_ID_VENDA_UNICO"), "\|").getItem(1).cast("string"))
    
    sales_df = sales_df.withColumn(
        "COD_ID_VENDA_UNICO_NOVA",
        concat_ws("|", col("COD_ID_LOJA").cast("string"), col("CUPOM_SPLIT"))
    )
    
    sales_df = sales_df.withColumn(
        "CORRIGIDO",
        col("LOJA_SPLIT") != col("COD_ID_LOJA").cast("string")
    )
    
    bad_rows = sales_df.filter(col("CORRIGIDO") == True).select("_ROW_ID", "COD_ID_VENDA_UNICO", "COD_ID_VENDA_UNICO_NOVA")

    sales_df = sales_df.withColumn(
        "COD_ID_VENDA_UNICO",
        col("COD_ID_VENDA_UNICO_NOVA")
    )
    
    sales_df = sales_df.drop("_ROW_ID", "LOJA_SPLIT", "CUPOM_SPLIT", "COD_ID_VENDA_UNICO_NOVA", "CORRIGIDO")
    
    return sales_df, bad_rows
