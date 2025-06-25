class BaseSQLLoader:
  def __init__(self, spark, logger, url, user, password, driver):
    self.spark = spark
    self.logger = logger
    self.url = url
    self.user = user
    self.password = password
    self.driver = driver

  def save(self, df, table_name, mode="overwrite"):
    self.logger.info(f"Salvando DataFrame na tabela '{table_name}' do SQL...")
    df.write.format("jdbc") \
      .option("url", self.url) \
      .option("dbtable", table_name) \
      .option("user", self.user) \
      .option("password", self.password) \
      .option("driver", self.driver) \
      .mode(mode) \
      .save()
    self.logger.info(f"Salvo em '{table_name}' com sucesso.")

  def read_query(self, query: str):  
    self.logger.info("Executando leitura da query SQL...")
    df = self.spark.read.format("jdbc") \
      .option("url", self.url) \
      .option("query", query) \
      .option("user", self.user) \
      .option("password", self.password) \
      .option("driver", self.driver) \
      .load()
    self.logger.info("Leitura da query concluída com sucesso.")
    return df

