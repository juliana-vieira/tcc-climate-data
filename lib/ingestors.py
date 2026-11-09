import delta
import db

# Classe para ingestão Full Load em batch
class IngestaoFullBronze:
    def __init__(self, table, idField, spark):
        self.table = table
        self.tableName = f'bronze.climapetropolis.{table}' #Definição do nome da tabela dentro do catalog.database
        self.path = f"/Volumes/raw/raw-climapetropolis/full_load/" # Caminho dos arquivos raw
        self.idField = idField # Coluna de IDs
        self.query = db.import_query(f"{table}.sql") # Importação da query do arquivo imoveis.sql utilizando o método import_query do módulo db.py
        self.query = self.query.format(table=table) # Format da query para incluir o nome da tabela
        self.spark = spark

    def read(self):
        # Leitura dos arquivos
        df = self.spark.read.json(self.path)
        return df
    
    def transform(self, df):
        # Método que executa as transformações necessárias no dataframe para preparar para ingestão
        
        # Criação da VIEW temporária para transformação com SQL
        df.createOrReplaceTempView(self.table)

        # Transformação básica (adição da coluna de timestamp da ingestão em UTC-3) utilizando a query formatada
        df_transform = self.spark.sql(self.query)
        return df_transform
    
    def load(self, df):

        # Método que salva o dataframe em delta, adicionando os novos registros abaixo, e dando merge no Schema em bronze.climapetropolis.registros

        (df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(self.tableName))
        
    def auto(self):
        df = self.read()
        df_transform = self.transform(df)
        self.load(df_transform)

# Classe de ingestão Full Load com streaming que herda da classe IngestaoFullBronze

class IngestaoFullBronzeStreaming(IngestaoFullBronze):
    def __init__(self, table, idField, spark):
        super().__init__(table, idField, spark)

        # Definição do checkpoint_path, onde serão a armazenadas as informações utilizadas pela streams
        # Por que usar streaming? -> Evita reprocessamento de arquivos. Se o bucket possuir 1000 arquivos, a stream não vai processar todos a cada execução, e sim os arquivos que não estão presentes nos metadados do checkpoint
        # Essas informações são necessárias para que a stream saiba em qual posição parou a última execução, ou seja, somente arquivos novos serão processados
        self.checkpoint_path = "/Volumes/raw/raw-climapetropolis/full_load/checkpoints/" 
       
    def read(self):
        return (self.spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", f"{self.checkpoint_path}/schema")
            .option("cloudFiles.inferColumnTypes", "false")
            .load(self.path))     

    def transform(self, df, epoch_id):
        # Remove a coluna de rescued_data caso apareça para evitar erro de schema no append
        if "_rescued_data" in df.columns:
            df = df.drop("_rescued_data")
            
        df = self.spark.sql(self.query.format(table = self.table))
    
        # Faz o append puro das novas consultas da API na tabela Delta
        df.write.format("delta").mode("append").saveAsTable(self.tableName)

    def save(self, df):
        (
            df.writeStream
            .foreachBatch(self.transform)
            .option("checkpointLocation", f"{self.checkpoint_path}/bronze_{self.table}")
            .trigger(availableNow=True)
            .start()
        )

    def auto(self):

        df = self.read()
        self.save(df)