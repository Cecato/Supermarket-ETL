class BaseLoader:
  def __init__(self, spark, logger, extractor):
    self.spark = spark
    self.logger = logger
    self.extractor = extractor

  def load(self, path, schema, file_type, **kwargs):
    df = self.extractor.extract(
      path,
      schema,
      file_type=file_type,
      **kwargs
    )
    self.logger.info(f"{self.__class__.__name__}: Loaded {df.count():,} rows from {path}")
    return df
