from pymongo import MongoClient

def replicar_clientes_para_mongodb(df_clientes, logger=None, mongo_uri="mongodb://mongo:27017", database="replica_db", collection="clientes"):
    clientes_list = [row.asDict() for row in df_clientes.collect()]

    client = MongoClient(mongo_uri)
    db = client[database]
    col = db[collection]

    col.delete_many({})

    if clientes_list:
        col.insert_many(clientes_list)
        if logger:
            logger.info(f"{len(clientes_list)} clientes replicados para MongoDB: {database}.{collection}")
    else:
        if logger:
            logger.warning("Nenhum cliente a replicar para o MongoDB.")

    client.close()
