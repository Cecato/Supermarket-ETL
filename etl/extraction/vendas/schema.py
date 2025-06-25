from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

schema_vendas = StructType([
    StructField("DUMMY", StringType(), True),
    StructField("COD_ID_LOJA", IntegerType(), True),
    StructField("NUM_ANOMESDIA", IntegerType(), True),
    StructField("COD_ID_CLIENTE", IntegerType(), True),
    StructField("DES_TIPO_CLIENTE", StringType(), True),
    StructField("DES_SEXO_CLIENTE", StringType(), True),
    StructField("COD_ID_VENDA_UNICO", StringType(), True),
    StructField("COD_ID_PRODUTO", IntegerType(), True),
    StructField("VAL_VALOR_SEM_DESC", DoubleType(), True),
    StructField("VAL_VALOR_DESCONTO", DoubleType(), True),
    StructField("VAL_VALOR_COM_DESC", DoubleType(), True),
    StructField("VAL_QUANTIDADE_KG", DoubleType(), True),
])
