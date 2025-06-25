from etl.spark_session import get_spark_session
from etl.utils.logger import get_logger
from etl.pipeline.run_etl import run_pipeline

def main():
    logger = get_logger("main")
    logger.info("== Starting ETL pipeline ==")
    spark = get_spark_session("Supermarket-ETL")
    run_pipeline(spark, logger)
    logger.info("Pipeline finished successfully.")
    spark.stop()

if __name__ == "__main__":
    main()
