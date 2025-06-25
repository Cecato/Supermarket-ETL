from etl.extraction.base_loader import BaseLoader
from etl.extraction.clientes.schema import schema_clientes

class CustomersLoader(BaseLoader):
  def load_customers(self):
    customers = self.load(
      "data/clientes.json",
      schema_clientes,
      file_type="json",
      encoding="utf-8"
    )
    return customers
