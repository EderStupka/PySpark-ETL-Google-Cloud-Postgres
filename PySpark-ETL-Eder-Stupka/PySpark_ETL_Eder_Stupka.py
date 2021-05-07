
import psycopg2
import sys
import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, LongType, TimestampType, ShortType, DateType
from pyspark.sql.functions import date_add, col, date_format, to_timestamp, to_date, year, month, sum

def main(): 

    #1 - Realizar a importação dos dados dos 3 arquivos em uma tabela criada por você no banco de dados de sua escolha; 

    # establish a connection to the GOOGLE CLOUD DATABASE
    conn = psycopg2.connect(
        host = "34.95.193.234",
        database = "postgres",
        user = "postgres",
        password = "siul1991")

    print("Connection to PostgreSQL created", "\n")

    cur = conn.cursor()

    spark = initialize_Spark()

    df = loadDFWithSchema(spark, "Base_2017_1.csv,Base_2018_2.csv,Base_2019_3.csv,")

    create_vendas_table(cur)

    insert_query, venda_seq = write_vendas_postgresql(df)

    cur.execute(insert_query, venda_seq)

    print("Data inserted into PostgreSQL", "\n")

    # 2- Com os dados importados, modelar 4 novas tabelas e implementar processos que façam as transformações necessárias e insiram as seguintes visões nas tabelas: 
    #a. Tabela1: Consolidado de vendas por ano e mês; 

    cur = conn.cursor()

    table1 = table1_data(df)

    create_table1(cur)

    insert_query, venda_seq = write_table1_postgresql(table1)

    #b. Tabela2: Consolidado de vendas por marca e linha; 

    table2 = table2_data(df)

    #c. Tabela3: Consolidado de vendas por marca, ano e mês;  


    #d. Tabela4: Consolidado de vendas por linha, ano e mês

    

    #create_table2(cur)
    #insert_query, venda_seq = write_table1_postgresql(table1)


    print("Commiting changes to database", "\n")

    conn.commit()


    print("Closing connection", "\n")

    # close the connection
    cur.close()
    conn.close()
    
    print("Done!", "\n")


def initialize_Spark(): #metodo de inicialização do spark
    
    os.environ['HADOOP_HOME'] = "D:/hadoop/hadoop-3.2.1"
    sys.path.append("D:/hadoop/hadoop-3.2.1/bin")
    
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Simple etl job") \
        .getOrCreate()

    print("Spark Initialized", "\n")

    return spark

def loadDFWithSchema(spark, file):

    schema = StructType([
        StructField("ID_MARCA", IntegerType(), True),
        StructField("MARCA", StringType(), True),
        StructField("ID_LINHA", IntegerType(), False),
        StructField("LINHA", StringType(), True),
        StructField("DATA_VENDA", DateType(), True),
        StructField("QTD_VENDA", IntegerType(), True),
    ])

    splits = file.split(",")
    x = 0
    df1 = spark.read.format("csv").schema(schema).option("header", "true").load(splits[0])
    df2 = spark.read.format("csv").schema(schema).option("header", "true").load(splits[1])
    df3 = spark.read.format("csv").schema(schema).option("header", "true").load(splits[2])
    
    df1_df2 = df1.union(df2)
    result = df1_df2.union(df3)
    

    print("Data loaded into PySpark", "\n")

    print("Printing data ...")
    result.show()
    result.printSchema()
   
    return result

def table1_data(df):

    df_dropped = df.drop("ID_MARCA","MARCA","ID_LINHA","LINHA")
    df_mes_ano = df_dropped.select(date_format("DATA_VENDA", "MM/yyyy").alias("MES/ANO"), "QTD_VENDA")
    gr = df_mes_ano.groupby("MES/ANO")
    df_grouped = gr.agg(sum(col('QTD_VENDA')).alias('SUM_QTD_VENDA'))
    df_sort = df_grouped.sort("MES/ANO")

    print("Data transformed", "\n")

    df_sort.show()
    df_sort.printSchema()
   
    return df_sort

def table2_data(df):

    df_dropped_marca = df.drop("ID_LINHA","LINHA", "DATA_VENDA", "QTD_VENDA")

    df_marca = df_dropped_marca.select('ID_MARCA','MARCA').distinct()

    df_dropped_linha = df.drop("ID_MARCA","MARCA","DATA_VENDA", "QTD_VENDA")

    df_linha = df_dropped_linha.select('ID_LINHA','LINHA').distinct()
    
    
    #select id_linha,sum(qtd_venda) as marca_1 from vendas where id_marca = 1 group by id_linha;


    df_select = df.select('ID_LINHA','QTD_VENDA').filter('ID_MARCA = 1')
    gr = df_select.groupby("ID_LINHA")
    df_grouped = gr.agg(sum(col('QTD_VENDA')).alias('MARCA 1 '))

    #gr = df_select
    
    df_grouped.show()

    #marca_seq = [tuple(x) for x in df_marca.collect()]

    #print (marca_seq)

    #records_list_template = ','.join(['%s'] * len(marca_seq))


    #print ("INSERT INTO VENDAS (id_marca, marca, id_linha, linha, data_venda, qtd_venda \
    #                       ) VALUES {}".format(records_list_template))



    #insert_query = "INSERT INTO VENDAS (id_marca, marca, id_linha, linha, data_venda, qtd_venda \
    #                       ) VALUES {}".format(records_list_template)
    
    #print("Inserting data into PostgreSQL...", "\n")

    #return insert_query, venda_seq




    print("Data transformed", "\n")

    df_marca.show()
    df_marca.printSchema()
   
    df_linha.show()
    df_linha.printSchema()
    return df_linha

def create_vendas_table(cursor):

    try:
        cursor.execute("CREATE TABLE IF NOT EXISTS VENDAS ( \
        ID_MARCA int, \
        MARCA varchar(100), \
        ID_LINHA int, \
        LINHA varchar(100), \
        DATA_VENDA date, \
        QTD_VENDA int \
        );")

        print("Created table in PostgreSQL", "\n")
    except:
        print("Something went wrong when creating the table", "\n")

def create_table1(cursor):

    try:
        cursor.execute("CREATE TABLE IF NOT EXISTS TABELA1 (MES_ANO varchar(30), SUM_QTD_VENDA int)")

        print("Created TABELA1 in PostgreSQL", "\n")
    except:
        print("Something went wrong when creating the table", "\n")

def create_table2(cursor):

    try:
        cursor.execute("CREATE TABLE IF NOT EXISTS VENDAS ( \
        ID_MARCA int, \
        MARCA varchar(100), \
        ID_LINHA int, \
        LINHA varchar(100), \
        DATA_VENDA date, \
        QTD_VENDA int \
        );")

        print("Created table in PostgreSQL", "\n")
    except:
        print("Something went wrong when creating the table", "\n")

def create_table3(cursor):

    try:
        cursor.execute("CREATE TABLE IF NOT EXISTS VENDAS ( \
        ID_MARCA int, \
        MARCA varchar(100), \
        ID_LINHA int, \
        LINHA varchar(100), \
        DATA_VENDA date, \
        QTD_VENDA int \
        );")

        print("Created table in PostgreSQL", "\n")
    except:
        print("Something went wrong when creating the table", "\n")
        
def create_table4(cursor):

    try:
        cursor.execute("CREATE TABLE IF NOT EXISTS VENDAS ( \
        ID_MARCA int, \
        MARCA varchar(100), \
        ID_LINHA int, \
        LINHA varchar(100), \
        DATA_VENDA date, \
        QTD_VENDA int \
        );")

        print("Created table in PostgreSQL", "\n")
    except:
        print("Something went wrong when creating the table", "\n")

def write_vendas_postgresql(df):

    venda_seq = [tuple(x) for x in df.collect()]

    records_list_template = ','.join(['%s'] * len(venda_seq))

    insert_query = "INSERT INTO VENDAS (id_marca, marca, id_linha, linha, data_venda, qtd_venda \
                           ) VALUES {}".format(records_list_template)
    
    print("Inserting data into PostgreSQL...", "\n")

    return insert_query, venda_seq

def write_table1_postgresql(df):

    venda_seq = [tuple(x) for x in df.collect()]

    records_list_template = ','.join(['%s'] * len(venda_seq))

    insert_query = "INSERT INTO TABELA1 (MES_ANO, SUM_QTD_VENDAS \
                           ) VALUES {}".format(records_list_template)
    
    print("Inserting data into PostgreSQL...", "\n")

    return insert_query, venda_seq


if __name__ == '__main__':
    main()