def quantidade_vendas_por_dia():
    return """
    SELECT
      s."NUM_ANOMESDIA",
      COUNT(DISTINCT s."COD_ID_VENDA_UNICO") AS QTD_VENDAS
    FROM sales s
    GROUP BY s."NUM_ANOMESDIA"
    ORDER BY s."NUM_ANOMESDIA"
    """

def produtos_distintos_por_dia():
    return """
    SELECT
      s."NUM_ANOMESDIA",
      COUNT(DISTINCT s."COD_ID_PRODUTO") AS PRODUTOS_DISTINTOS
    FROM sales s
    GROUP BY s."NUM_ANOMESDIA"
    ORDER BY s."NUM_ANOMESDIA"
    """
