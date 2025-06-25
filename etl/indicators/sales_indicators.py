import pyspark.sql.functions as F

class SalesIndicators:
	def __init__(self, sales_df):
		self.sales = sales_df

	def top_selling_products(self):
		return self.sales.groupBy("COD_ID_PRODUTO").count().orderBy("count", ascending=False)

	def top_customers(self):
		return self.sales.groupBy("COD_ID_CLIENTE").count().orderBy("count", ascending=False)

	def sales_per_day(self):
		return self.sales.groupBy("NUM_ANOMESDIA").count().orderBy("NUM_ANOMESDIA")

	def distinct_products_per_day(self):
		return self.sales.groupBy("NUM_ANOMESDIA") \
			.agg(F.countDistinct("COD_ID_PRODUTO").alias("distinct_products")) \
			.orderBy("NUM_ANOMESDIA")

	def products_highest_discount(self):
		return self.sales.groupBy("COD_ID_PRODUTO") \
			.agg(F.sum("VAL_VALOR_DESCONTO").alias("total_discount")) \
			.orderBy("total_discount", ascending=False)
