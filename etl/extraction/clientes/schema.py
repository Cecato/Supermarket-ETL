from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema_clientes = StructType([
    StructField("COD_ID_CLIENTE", IntegerType(), True),
    StructField("DES_TIPO_CLIENTE", StringType(), True),
    StructField("NOM_NOME", StringType(), True),
    StructField("DES_SEXO_CLIENTE", StringType(), True),
    StructField("DAT_DATA_NASCIMENTO", StringType(), True),
])
