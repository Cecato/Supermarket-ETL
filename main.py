# main.py
from etl.spark_session import get_spark_session

def main():
    spark = get_spark_session("ETL_Pipeline_PySpark")
    print("Spark iniciado com sucesso.")
    spark.stop()

if __name__ == "__main__":
    main()
