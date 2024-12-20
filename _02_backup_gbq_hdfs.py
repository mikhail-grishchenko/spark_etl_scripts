import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import pyarrow as pa
import pyarrow.parquet as pq
from pandas_gbq import gbq
import tqdm
from datetime import datetime, timedelta
import sys, os
import subprocess
from io import BytesIO
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col, lit, hex, concat
from pyspark.sql.functions import max as spark_max
import gc
import math
import time

import fcntl
import time
import pwd
import functools
import signal


# блокировка параллельных запусков 
def prevent_duplicates(lock_file):
    # Попытка открыть файл с блокировкой
    try:
        file_descriptor = os.open(lock_file, os.O_CREAT | os.O_RDWR)
        fcntl.flock(file_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        print("Скрипт запущен")

        #Получение PID текущего процесса
        pid = os.getpid()
        # Получение информации о текущем пользователе, запускающем процесс
        user = pwd.getpwuid(os.geteuid())[0]
        print(f"{datetime.today().strftime('%Y-%m-%d %H:%M:%S')}.PID, USER текущего процесса:{pid}, {user}")
    except OSError as e:
        print(f"{e}")
        if e.errno == 11:
            pid = os.getpid()
            user = pwd.getpwuid(os.geteuid())[0]
            print("Дубликат скрипта уже запущен")
            sys.exit(0)
            os._exit(1)  # Добавляем os._exit(1) для немедленного завершения скрипта
        else:
            raise



#ФУНКЦИЯ ПОЛУЧЕНИЯ И ПРОВЕРКИ БИЛЕТА KERBEROS
def kinit(principal, password):
    try:
        # Execute kinit command with password
        subprocess.run(['kinit', principal], input=password.encode(), check=True)
        print("Kerberos ticket obtained successfully!\n")
    except subprocess.CalledProcessError as e:
        print("Failed to obtain Kerberos ticket:", e)
        
    try:
        print("Kerberos ticket details:")
        subprocess.run(['klist'], check=True)
        print('\n')
    except subprocess.CalledProcessError as e:
        print("Failed to check Kerberos ticket:", e)


    
if __name__ == "__main__":  
    
    # initial_path = '/data/composes/'
    initial_path = '/opt/'

    path_lockfiles = f'{initial_path}airflow/dags/lockfiles/'
    lock_file = f'{path_lockfiles}gbq_hdfs_backup.lock'
    prevent_duplicates(lock_file)
    
    
    hdfs_path = "/data/gbq_backup/installs/"
    
    #ПУТИ К КОНФИГАМ HADOOP, БИНАРНИКАМ APACHE HADOOP, БИНАРНИКАМ APACHE SPARK, JAVA OPENJDK, КРЕДАМ
    #ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ    
    os.environ['HADOOP_CONF_DIR'] = f'{initial_path}airflow/dags/git/config/HADOOP_CONF_DIR/sdpprod/'
    os.environ['HADOOP_HOME'] = f'{initial_path}airflow/hadoop-3.4.0/'
    os.environ['PATH'] = os.environ.get('PATH', '') + ':' + os.environ.get('HADOOP_HOME', '') + '/bin'
    os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-11-openjdk-amd64'
    os.environ['PATH'] = os.environ.get('PATH', '') + ':' + os.environ.get('JAVA_HOME', '') + '/bin'
    
    sys.path.append(f'{initial_path}airflow/spark-3.5.1-bin-hadoop3/python/lib/py4j-0.10.9.7-src.zip')
    sys.path.append(f'{initial_path}airflow/spark-3.5.1-bin-hadoop3/python')
    os.environ["SPARK_HOME"] = f'{initial_path}airflow/spark-3.5.1-bin-hadoop3'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f'{initial_path}airflow/credentials/cred_gbq.json'
    
    #НАТИВНЫЕ БИБЛИОТЕКИ HADOOP
    native_lib_path = f'{initial_path}airflow/hadoop-3.4.0/lib/native'
    os.environ['LD_LIBRARY_PATH'] = f"{native_lib_path}:{os.environ.get('LD_LIBRARY_PATH', '')}"
        

    #ПОЛУЧАЕМ И ПРОВЕРЯЕМ БИЛЕТ KERBEROS
    cred = pd.read_json(f'{initial_path}airflow/credentials/g_cred.json', typ='series') 
    principal = f'{cred[0]}@CORP.DOMEN.RU'
    password = cred[1]
    kinit(principal, password)
    

    spark = SparkSession.builder \
        .appName("BigQueryToHDFS") \
        .master("local[10]") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://sdpprod") \
        .config("spark.jars", f"{initial_path}airflow/spark_connectors/spark-3.5-bigquery-0.40.0.jar") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile", f"{initial_path}airflow/dags/credentials/cred_gbq.json") \
        .config("spark.executorEnv.GOOGLE_APPLICATION_CREDENTIALS", f"{initial_path}airflow/dags/credentials/cred_gbq.json") \
        .config("spark.sql.debug.maxToStringFields", "500") \
        .config("spark.executor.memory", "32g") \
        .config("spark.driver.memory", "32g") \
        .config("spark.executor.cores", "8") \
        .config("spark.driver.cores", "8") \
        .config("spark.local.dir", "/tmp/spark") \
        .config("spark.executorEnv.HADOOP_CONF_DIR", f"{initial_path}airflow/config/HADOOP_CONF_DIR/sdpprod/") \
        .config("spark.executorEnv.JAVA_TOOL_OPTIONS", "-Xmx64m") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel('WARN')
    
    spark.sql("select 1+1").show()
    print("Spark session successfully started and test query executed.")
    
    
    
    # ШАГ 1: ПОЛУЧАЕМ МАКСИМАЛЬНУЮ ДАТУ ИЗ HDFS
    try:
        # Читаем данные из HDFS
        df_hdfs = spark.read.parquet(f"hdfs://sdpprod{hdfs_path}")
        
        # Получаем максимальную дату install_date
        max_install_date = df_hdfs.select(spark_max(col("install_date"))).collect()[0][0]
        print(f"Maximum install_date in HDFS: {max_install_date}")
    except Exception as e:
        print(f"Failed to read from HDFS or find max install_date: {e}")
        spark.stop()
        sys.exit(1)
    
    # ШАГ 2: ФИЛЬТРАЦИЯ ДАННЫХ ИЗ BIGQUERY
    try:
        # Определяем текущее время
        yesterday_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        print(f'yesterday_date - {yesterday_date}')
        
        # Читаем данные из BigQuery, фильтруя по диапазону дат
        df_increment = spark.read \
            .format("bigquery") \
            .option("filter", f"install_date > '{max_install_date}' AND install_date < '{yesterday_date}'") \
            .load("project.dataset.installs")

        print(f"Number of rows read from BigQuery for the period: {df_increment.count()}")
    except Exception as e:
        print(f"Failed to load data from BigQuery: {e}")
        spark.stop()
        sys.exit(1)
    
    # ШАГ 3: ЗАПИСЬ ОТФИЛЬТРОВАННЫХ ДАННЫХ В HDFS
    try:
        if df_increment.count() > 0:
            df_increment.write \
                .mode("append") \
                .partitionBy("install_date") \
                .parquet(f"hdfs://sdpprod{hdfs_path}")

            print(f"Data for the period successfully written to HDFS.")
        else:
            print('Данных для загрузки нет.')
    except Exception as e:
        print(f"Failed to write data to HDFS: {e}")
        
        
    # Закрытие SparkSession
    spark.stop()
    gc.collect()
