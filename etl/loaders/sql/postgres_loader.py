from etl.loaders.sql.base_sql_loader import BaseSQLLoader
from etl.loaders.sql.postgres_config import POSTGRES_URL, POSTGRES_USER, POSTGRES_PASS, POSTGRES_DRIVER

class PostgresLoader(BaseSQLLoader):
    def __init__(self, spark, logger):
        super().__init__(
            spark,
            logger,
            POSTGRES_URL,
            POSTGRES_USER,
            POSTGRES_PASS,
            POSTGRES_DRIVER
        )