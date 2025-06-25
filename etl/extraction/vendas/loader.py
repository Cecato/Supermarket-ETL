from etl.extraction.base_loader import BaseLoader
from etl.extraction.vendas.schema import schema_vendas

class SalesLoader(BaseLoader):
  def load_sales(self):
    sales = self.load(
      "data/vendas.csv",
      schema_vendas,
      file_type="csv",
      header=True,
      sep=";",
      encoding="utf-8"
    )
    if "DUMMY" in sales.columns:
      sales = sales.drop("DUMMY")
      self.logger.info("SalesLoader: Removed blank first column ('DUMMY').")
    return sales
