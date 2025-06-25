import os
import shutil

def salvar_unico_csv(df, caminho_final):
    tmp_dir = caminho_final + "_tmp"
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(tmp_dir)
    for filename in os.listdir(tmp_dir):
        if filename.endswith(".csv"):
            shutil.move(os.path.join(tmp_dir, filename), caminho_final)
            break
    shutil.rmtree(tmp_dir)

def validar_vendas_vs_clientes_produtos(sales_df, customers_df, products_df, logger=None, output_dir="output"):
    clientes_ausentes = (
        sales_df.select("COD_ID_CLIENTE").distinct()
        .join(customers_df.select("COD_ID_CLIENTE").distinct(), on="COD_ID_CLIENTE", how="left_anti")
    )

    produtos_ausentes = (
        sales_df.select("COD_ID_PRODUTO").distinct()
        .join(products_df.select("COD_ID_PRODUTO").distinct(), on="COD_ID_PRODUTO", how="left_anti")
    )

    clientes_path = f"{output_dir}/clientes_nao_encontrados.csv"
    produtos_path = f"{output_dir}/produtos_nao_encontrados.csv"

    salvar_unico_csv(clientes_ausentes, clientes_path)
    salvar_unico_csv(produtos_ausentes, produtos_path)

    if logger:
        logger.info(f"Clientes nas vendas e não encontrados em clientes: {clientes_path}")
        logger.info(f"Produtos nas vendas e não encontrados em produtos: {produtos_path}")

    return clientes_ausentes, produtos_ausentes
