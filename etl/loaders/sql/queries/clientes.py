def clientes_com_mais_compras():
    return """
    SELECT
      s."COD_ID_CLIENTE",
      COUNT(*) AS TOTAL_COMPRAS
    FROM sales s
    GROUP BY s."COD_ID_CLIENTE"
    ORDER BY TOTAL_COMPRAS DESC
    """
