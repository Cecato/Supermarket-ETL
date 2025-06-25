from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema_produtos = StructType([
    StructField("COD_ID_PRODUTO", IntegerType(), True),
    StructField("COD_ID_CATEGORIA_PRODUTO", IntegerType(), True),
    StructField("ARR_CATEGORIAS_PRODUTO", StringType(), True),
    StructField("DES_PRODUTO", StringType(), True),
    StructField("DES_UNIDADE", StringType(), True),
    StructField("COD_CODIGO_BARRAS", StringType(), True),
])
