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


#ФУНКЦИЯ ВЫПОЛНЕНИЯ КОММАНД HDFS ЧЕРЕЗ SUBPROCESS       
def run_hdfs_command(command):
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Failed to run command {' '.join(command)}:", e)
        

    
if __name__ == "__main__":  
    
    # initial_path = '/data/composes/'
    initial_path = '/opt/'
    
    hdfs_path = "/data/exchange/table_gp/"
    
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
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f'{initial_path}airflow/dags/credentials/cred_gbq.json'
    
    #НАТИВНЫЕ БИБЛИОТЕКИ HADOOP
    native_lib_path = f'{initial_path}airflow/hadoop-3.4.0/lib/native'
    os.environ['LD_LIBRARY_PATH'] = f"{native_lib_path}:{os.environ.get('LD_LIBRARY_PATH', '')}"
        
    credentials = service_account.Credentials.from_service_account_file(f'{initial_path}airflow/credentials/cred_gbq.json')
    project_id = 'project'
    clientBQ = bigquery.Client(credentials=credentials,project=project_id)

    #ПОЛУЧАЕМ И ПРОВЕРЯЕМ БИЛЕТ KERBEROS
    cred = pd.read_json(f'{initial_path}airflow/credentials/cred.json', typ='series') 
    principal = f'{cred[0]}@CORP.DOMEN.RU'
    password = cred[1]
    kinit(principal, password)
    

    req_gbq='''
    SELECT MAX(mt_update_dt) FROM `project.dwh_input.table` 
    '''
    max_date_gbq = gbq.read_gbq(req_gbq, project_id=project_id, credentials=credentials, dialect='standard')
    max_date_gbq = max_date_gbq.iloc[0,0]
    date_yesterday = datetime.now().date() - timedelta(days=1)
    print (f'max_date_gbq - {max_date_gbq}')
    print (f'date_yesterday - {date_yesterday}')

    
    spark = SparkSession.builder \
        .appName("MySparkApp") \
        .master("local[10]") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://sdpprod") \
        .config("spark.jars", f"{initial_path}airflow/spark_connectors/spark-3.5-bigquery-0.40.0.jar") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile", f"{initial_path}airflow/credentials/cred_gbq.json") \
        .config("spark.executorEnv.GOOGLE_APPLICATION_CREDENTIALS", f"{initial_path}airflow/credentials/cred_gbq.json") \
        .config("spark.sql.debug.maxToStringFields", "500") \
        .config("spark.executor.memory", "32g") \
        .config("spark.driver.memory", "32g") \
        .config("spark.executor.cores", "8") \
        .config("spark.driver.cores", "8") \
        .config("spark.local.dir", "/tmp/spark") \
        .config("spark.executorEnv.HADOOP_CONF_DIR", f"{initial_path}airflow/dags/git/config/HADOOP_CONF_DIR/sdpprod/") \
        .config("spark.executorEnv.JAVA_TOOL_OPTIONS", "-Xmx64m") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel('WARN')
    spark.sql("select 1+1").show()
    print("Spark session successfully started and test query executed.")
    
    df = spark.read.parquet(f"hdfs://sdpprod{hdfs_path}") \
        .filter(col("mt_update_dt") > lit(max_date_gbq))
    print(f"df.count(): {df.count()}")
    
    
    # Переименовывание и изменение типов
    df_transformed = df \
        .withColumnRenamed("mrch_key", "mrch_key") \
        .withColumnRenamed("mrch_id", "mrchID") \
        .withColumnRenamed("mrch_sid", "mrch_sid") \
        .withColumnRenamed("mrch_name", "mrchName") \
        .withColumnRenamed("mrch_full_name", "mrchFullName") \
        .withColumnRenamed("mrch_name_id", "mrch_name_id") \
        .withColumnRenamed("mrch_url", "mrch_url") \
        .withColumnRenamed("mrch_create_dt", "mrch_create_dt") \
        .withColumnRenamed("mrch_is_active", "mrch_is_active") \
        .withColumnRenamed("mrch_is_deleted", "mrch_is_deleted") \
        .withColumnRenamed("mrch_is_test", "mrch_is_test_mrch") \
        .withColumnRenamed("mrch_inn", "mrch_inn") \
        .withColumnRenamed("mrch_is_exclude_from_blocking", "mrch_is_exclude_from_blocking") \
        .withColumnRenamed("mrch_rating_final", "mrch_rating_final") \
        .withColumnRenamed("mrch_rating_static", "mrch_rating_static") \
        .withColumnRenamed("mrch_rating_auto", "mrch_rating_auto") \
        .withColumnRenamed("mrch_use_auto_rating", "mrch_use_auto_rating") \
        .withColumnRenamed("mrch_order_daily_limit", "mrch_order_daily_limit") \
        .withColumnRenamed("mrch_confirmation_duration", "mrch_confirmation_duration") \
        .withColumnRenamed("mrch_activation_date", "mrch_activation_date").withColumn("mrch_activation_date", col("mrch_activation_date").cast("DATE")) \
        .withColumnRenamed("mrch_forgiveness_dt", "mrch_forgiveness_date") \
        .withColumnRenamed("mrch_shipment_day", "mrch_shipment_day") \
        .withColumnRenamed("mrch_shipment_time_to", "mrch_shipment_time_to") \
        .withColumnRenamed("mrch_contract_id", "mrch_contract_id") \
        .withColumnRenamed("mrch_stock_source", "mrch_stock_source") \
        .withColumnRenamed("mrch_is_cnc", "mrch_is_cnc") \
        .withColumnRenamed("mrch_is_bu_matching_forbidden", "mrch_is_bu_matching_forbidden") \
        .withColumnRenamed("mrch_primary_category_name", "mrch_primary_category_name") \
        .withColumnRenamed("mrch_is_sbl_active", "mrch_is_sbl_active") \
        .withColumnRenamed("mrch_show_pdp", "mrch_show_pdp").withColumn("mrch_show_pdp", col("mrch_show_pdp").cast("BOOLEAN")) \
        .withColumnRenamed("mrch_show_cc", "mrch_show_cc").withColumn("mrch_show_cc", col("mrch_show_cc").cast("BOOLEAN")) \
        .withColumnRenamed("mrch_pdp_block_type_key", "mrch_pdp_block_type_key") \
        .withColumnRenamed("mrch_pdp_block_type_name", "mrch_pdp_block_type_name") \
        .withColumnRenamed("mrch_cnc_block_type_key", "mrch_cnc_block_type_key") \
        .withColumnRenamed("mrch_cnc_block_type_name", "mrch_cnc_block_type_name") \
        .withColumnRenamed("mrch_delivery_order_section_enabled", "mrch_is_delivery_order_section_enabled") \
        .withColumnRenamed("mrch_show_dbm", "mrch_show_dbm") \
        .withColumnRenamed("mrch_is_self_registration", "mrch_is_self_registration") \
        .withColumnRenamed("mrch_lead_source", "mrch_lead_source") \
        .withColumnRenamed("mrch_start_rate", "mrch_start_rate") \
        .withColumnRenamed("mrch_sber_hash_org_id", "mrch_sber_hash_org_id") \
        .withColumnRenamed("mrch_is_technical_partner_exist", "mrch_is_technical_partner_exist") \
        .withColumnRenamed("mrch_is_part_buyout", "mrch_is_part_buyout") \
        .withColumnRenamed("mrch_last_feed_dt", "mrch_last_feed_date") \
        .withColumnRenamed("mrch_kpp", "mrch_kpp") \
        .withColumnRenamed("mrch_ogrn", "mrch_ogrn") \
        .withColumnRenamed("mrch_is_warehouse_active", "mrch_is_warehouse_active") \
        .withColumnRenamed("mrch_test_order_status", "mrch_test_order_status") \
        .withColumnRenamed("mrch_contract_signing_dt", "mrch_contract_signing_dt") \
        .withColumnRenamed("mrch_test_period_start_dt", "mrch_test_period_start_dt") \
        .withColumnRenamed("mrch_test_period_start_cc_dt", "mrch_test_period_start_cc_dt") \
        .withColumnRenamed("mrch_test_period_start_dbm_dt", "mrch_test_period_start_dbm_dt") \
        .withColumnRenamed("mrch_test_period_end_dt", "mrch_test_period_end_dt") \
        .withColumnRenamed("mrch_test_period_end_cc_dt", "mrch_test_period_end_cc_dt") \
        .withColumnRenamed("mrch_test_period_end_dbm_dt", "mrch_test_period_end_dbm_dt") \
        .withColumnRenamed("mrch_is_model_1p", "mrch_is_model_1p") \
        .withColumnRenamed("mrch_business_form", "mrch_business_form") \
        .withColumnRenamed("mrch_account_manager_user_id", "mrch_account_manager_user_id") \
        .withColumnRenamed("mrch_account_manager_name", "mrch_account_manager_name") \
        .withColumnRenamed("holding_id", "holding_id") \
        .withColumnRenamed("holding_name", "holding_name") \
        .withColumnRenamed("mrch_partner_id", "mrch_partner_id") \
        .withColumnRenamed("mrch_accounting_counterparty_approval", "mrch_accounting_counterparty_approval") \
        .withColumnRenamed("mrch_is_enabled_cancellation_fee", "mrch_is_enabled_cancellation_fee") \
        .withColumnRenamed("mrch_auto_confirm_suggest_user_id", "mrch_auto_confirm_suggest_user_id") \
        .withColumnRenamed("mrch_auto_confirm_suggest", "mrch_auto_confirm_suggest") \
        .withColumnRenamed("mrch_business_group_name", "mrch_business_group_name") \
        .withColumnRenamed("mrch_integration_is_active", "mrch_integration_is_active").withColumn("mrch_integration_is_active", col("mrch_integration_is_active").cast("BIGINT")) \
        .withColumnRenamed("mrch_first_order_date", "mrch_first_order_date") \
        .withColumnRenamed("mrch_segment_id", "mrch_segment_id") \
        .withColumnRenamed("mrch_segment_name", "mrch_segment_name") \
        .withColumnRenamed("mrch_rating", "mrch_rating") \
        .withColumnRenamed("mrch_main_category_key", "mrch_main_category_key").withColumn("mrch_main_category_key", col("mrch_main_category_key").cast("BIGINT")) \
        .withColumnRenamed("mrch_main_category_name", "mrch_main_category_name") \
        .withColumnRenamed("mrch_sales_manager_key", "sales_manager_key").withColumn("sales_manager_key", col("sales_manager_key").cast("BIGINT")) \
        .withColumnRenamed("mrch_sales_manager_name", "sales_manager_name") \
        .withColumnRenamed("mrch_is_fulfillment", "mrch_is_fulfillment") \
        .withColumnRenamed("mrch_activation_month_id", "mrch_activation_month_id") \
        .withColumnRenamed("mrch_activation_month_name", "mrch_activation_month_name") \
        .withColumnRenamed("mrch_activation_quarter_id", "mrch_activation_quarter_id") \
        .withColumnRenamed("mrch_activation_quarter_name", "mrch_activation_quarter_name") \
        .withColumnRenamed("mrch_activation_year_id", "mrch_activation_year_id") \
        .withColumnRenamed("mrch_activation_year_name", "mrch_activation_year_name") \
        .withColumnRenamed("mrch_first_authorization_dt", "mrch_first_authorization_dt") \
        .withColumnRenamed("mrch_email", "mrch_email") \
        .withColumnRenamed("mrch_phone", "mrch_phone") \
        .withColumnRenamed("mrch_traffic_light_color", "mrch_traffic_light_color") \
        .withColumnRenamed("mrch_flow", "mrch_flow") \
        .withColumnRenamed("mrch_name_and_last_name", "mrch_name_and_last_name")


    # Преобразование столбца mrch_sid в шестнадцатеричную строку с префиксом '0x'
    df_transformed = df_transformed.withColumn("mrch_sid", concat(lit("0x"), hex(col("mrch_sid"))))
    
    
    print(f"df_transformed.count(): {df_transformed.count()}")

    if df_transformed.count() > 0:
        # Определите параметры BigQuery
        project_id = 'project'
        dataset_id = 'dwh_input'
        table_id = 'table'

        
        start_time = time.time()

        # Извлечение даты из столбца mt_update_dt и добавление в новый столбец
        df_transformed = df_transformed.withColumn("update_date", col("mt_update_dt").cast("date"))

        df_transformed.write \
        .format("bigquery") \
        .option("writeDisposition", "WRITE_APPEND") \
        .option("writeMethod", "direct") \
        .mode("append") \
        .save(f"{project_id}:{dataset_id}.{table_id}")

        execution_time = time.time() - start_time
        print(f'df.write - OK, execution time: {execution_time:.2f} seconds')

        print(f"Successfully processed")


        query_drop_duplicates =   '''
CREATE OR REPLACE TABLE project.dwh_input.table
AS
SELECT *  
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY mrchID ORDER BY mt_update_dt DESC) AS rn
  FROM project.dwh_input.table
  WHERE mrchID IS NOT NULL
) t
WHERE rn = 1;

ALTER TABLE project.dwh_input.table
DROP COLUMN rn;
                            '''

        clientBQ.query(query_drop_duplicates)
        print('query_drop_duplicates')

        

    # Закрытие SparkSession
    spark.stop()
    gc.collect()
