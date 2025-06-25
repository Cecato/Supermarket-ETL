# setup
1. Coloque os arquivos Brutos ( produtos.csv, vendas.csv, clientes.json) na pasta Data/

# Docker
1. docker-compose build

2. docker-compose up -d (vai rodar automaticamente)

DESAFIO TÉCNICO – DEV PYTHON PL
Contexto: Uma integração com um cliente onde recebemos dados históricos via arquivos
compactados de vendas, clientes e produtos. Pontos não definidos ficam a critério do
candidato tomar a decisão.
1. Construa uma pipeline com Python para extrair dados dos arquivos e inserir em um
banco de dados. Pense na escalabilidade e performance dessa pipeline.
1. Crie os seguintes indicadores via query a ser executada no banco e exportada em
planilha CSV:
a. Produtos mais vendidos.
b. Clientes com mais compras.
c. Quantidade de vendas por dia.
d. Quantidade de produtos distintos vendidos por dia.
e. Produtos que concederam maior desconto.
1. Na pipeline extraia a informação do melhor dia da semana de compra para cada um
dos 20 melhores clientes.
1. Na mesma pipeline implemente uma validação para identificar se há clientes ou
produtos que estejam na base de vendas e que não estejam nas outras. Esses casos
devem ser exportados em uma nova planilha.
1. Ainda na pipeline, escreva os dados de vendas porém incluindo nome do produto e
nome do cliente em arquivos do tipo parquet utilizando ano e mês como partições.
1. A coluna COD_ID_VENDA_UNICO é um identificador único de cada venda e é
composto por COD_ID_LOJA + COD_CUPOM. Verifique se todos os registros de vendas
possuem código de loja correto no campo COD_ID_VENDA_UNICO com base na coluna
COD_ID_LOJA presente no arquivo de vendas. Em caso de divergência, atualize no
COD_ID_VENDA_UNICO com o valor correto presente na coluna COD_ID_LOJA.
1. Caso o banco principal utilizado seja relacional (SQL), implemente uma etapa adicional
de replicação dos dados de clientes em um banco NoSQL, como o MongoDB.
Sugestões:
1- Utilize Docker para empacotar sua solução. Isso facilita a execução do projeto e
garante consistência no ambiente de desenvolvimento.
Caso opte por usar Docker, inclua no seu repositório:
● Um Dockerfile ou docker-compose.yml funcional
● Um passo-a-passo claro no README.md explicando como:
● Construir a imagem
● Rodar o container
● Executar/testar a aplicação
2 – Suba o código para o Git e nos envie o link com a resolução.