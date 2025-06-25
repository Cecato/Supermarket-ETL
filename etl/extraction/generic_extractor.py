class GenericExtractor:
    def __init__(self, spark):
        self.spark = spark

    def extract(self, path, schema, file_type="csv", **kwargs):
        if file_type == "csv":
            return self.spark.read.schema(schema).csv(path, **kwargs)
        elif file_type == "json":
            return self.spark.read.schema(schema).json(path, **kwargs)
        else:
            raise ValueError(f"Tipo de arquivo não suportado: {file_type}")
