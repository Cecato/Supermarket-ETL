from etl.extraction.base_loader import BaseLoader
from etl.extraction.produtos.schema import schema_produtos

class ProductsLoader(BaseLoader):
  def load_products(self):
    products = self.load(
      "data/produtos.csv",
      schema_produtos,
      file_type="csv",
      header=True,
      sep=";",
      encoding="utf-8"
    )
    return products
