from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType

# Inicializar a sessão do Spark
spark = SparkSession.builder \
    .appName("NetDescartaveisDB") \
    .getOrCreate()

# Esquema para a tabela de colaboradores
schema_colaboradores = StructType([
    StructField("id", IntegerType(), True),
    StructField("nome", StringType(), True),
    StructField("cargo", StringType(), True),
    StructField("email", StringType(), True),
    StructField("telefone", StringType(), True)
])

# Dados iniciais dos colaboradores
data_colaboradores = [
    (1, "Alice Silva", "Analista de Sistemas", "alice.silva@netdescartaveis.com", "1234-5678"),
    (2, "Bruno Pereira", "Gerente de TI", "bruno.pereira@netdescartaveis.com", "1234-5679"),
    (3, "Carlos Souza", "Desenvolvedor de Software", "carlos.souza@netdescartaveis.com", "1234-5680"),
    (4, "Diana Oliveira", "Especialista em Treinamento", "diana.oliveira@netdescartaveis.com", "1234-5681"),
    (5, "Eduardo Lima", "Gerente de Operações", "eduardo.lima@netdescartaveis.com", "1234-5682"),
    (6, "Fernanda Alves", "Coordenadora de Logística", "fernanda.alves@netdescartaveis.com", "1234-5683")
]

# Criação do DataFrame de colaboradores
df_colaboradores = spark.createDataFrame(data_colaboradores, schema_colaboradores)

# Mostra os dados dos colaboradores
df_colaboradores.show()

# Esquema para a tabela de produtos
schema_produtos = StructType([
    StructField("id", IntegerType(), True),
    StructField("nome", StringType(), True),
    StructField("descricao", StringType(), True),
    StructField("preco", FloatType(), True),
    StructField("quantidade_estoque", IntegerType(), True)
])

# Dados iniciais dos produtos
data_produtos = [
    (1, "Copo Descartável", "Copo descartável de 200ml", 0.10, 10000),
    (2, "Prato Descartável", "Prato descartável de 18cm", 0.20, 5000),
    (3, "Talher Descartável", "Kit de talheres descartáveis (garfo e faca)", 0.15, 7000)
]

# Criação do DataFrame de produtos
df_produtos = spark.createDataFrame(data_produtos, schema_produtos)

# Mostra os dados dos produtos
df_produtos.show()

# Esquema para a tabela de movimentacoes_estoque
schema_movimentacoes_estoque = StructType([
    StructField("id", IntegerType(), True),
    StructField("produto_id", IntegerType(), True),
    StructField("quantidade", IntegerType(), True),
    StructField("tipo_movimentacao", StringType(), True),
    StructField("data_movimentacao", TimestampType(), True)
])

# Dados iniciais de movimentações de estoque
data_movimentacoes_estoque = [
    (1, 1, 500, "entrada", "2024-08-01 10:00:00"),
    (2, 2, 300, "saida", "2024-08-02 14:00:00"),
    (3, 3, 1000, "entrada", "2024-08-03 09:00:00")
]

# Criação do DataFrame de movimentações de estoque
df_movimentacoes_estoque = spark.createDataFrame(data_movimentacoes_estoque, schema_movimentacoes_estoque)

# Mostra os dados de movimentações de estoque
df_movimentacoes_estoque.show()

# Esquema para a tabela de requisicoes
schema_requisicoes = StructType([
    StructField("id", IntegerType(), True),
    StructField("colaborador_id", IntegerType(), True),
    StructField("descricao", StringType(), True),
    StructField("data_requisicao", TimestampType(), True),
    StructField("status", StringType(), True)
])

# Dados iniciais de requisições
data_requisicoes = [
    (1, 1, "Necessidade de reposição de copos descartáveis", "2024-07-01 08:00:00", "pendente"),
    (2, 2, "Solicitação de novos equipamentos de TI", "2024-07-02 09:30:00", "pendente")
]

# Criação do DataFrame de requisições
df_requisicoes = spark.createDataFrame(data_requisicoes, schema_requisicoes)

# Mostra os dados das requisições
df_requisicoes.show()

# Salva os DataFrames como tabelas Hive (ou em outro formato se preferir)
df_colaboradores.write.mode("overwrite").saveAsTable("colaboradores")
df_produtos.write.mode("overwrite").saveAsTable("produtos")
df_movimentacoes_estoque.write.mode("overwrite").saveAsTable("movimentacoes_estoque")
df_requisicoes.write.mode("overwrite").saveAsTable("requisicoes")

# Parar a sessão do Spark
spark.stop()
