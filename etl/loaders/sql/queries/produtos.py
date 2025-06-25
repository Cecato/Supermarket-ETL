def produtos_mais_vendidos():
    return """
    SELECT
      s."COD_ID_PRODUTO",
      COUNT(*) AS TOTAL_VENDAS
    FROM sales s
    GROUP BY s."COD_ID_PRODUTO"
    ORDER BY TOTAL_VENDAS DESC
    """

def produtos_maior_desconto():
    return """
    SELECT
      s."COD_ID_PRODUTO",
      p."DES_PRODUTO",
      SUM(s."VAL_VALOR_DESCONTO") AS TOTAL_DESCONTO
    FROM sales s
    JOIN products p ON s."COD_ID_PRODUTO" = p."COD_ID_PRODUTO"
    GROUP BY s."COD_ID_PRODUTO", p."DES_PRODUTO"
    ORDER BY TOTAL_DESCONTO DESC
    """
