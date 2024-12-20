from datetime import timedelta, datetime

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from google.oauth2 import service_account
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email

from dependencies.utils import gen_spark_sumbit
import pandas as pd
import json
import requests
from datetime import datetime


HDFS_PATH = "/data/snap/hist_gp"
credentials = service_account.Credentials.from_service_account_file('/opt/airflow/security/gbq/gbq.json')


GBQ_PROJECT_ID = 'project'
GBQ_DATASET = None
GBQ_TABLE = 'input.fos_temp_GP'
GBQ_TABLE_TARGET = 'input.fos_GP'
GBQ_MODE = 'overwrite'
HDFS_PARTITION = 'snapshot_date'


args = {
    'owner': 'Airflow',
    'start_date': datetime(2022, 6, 27, 0),
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

def check_count_temp_table():
    count_gbq_temp_query = f""" SELECT count(snapshot_date), max(snapshot_date), min(snapshot_date) FROM {GBQ_TABLE} """
    count_gbq_temp = pd.read_gbq(count_gbq_temp_query,
                                     project_id=GBQ_PROJECT_ID, credentials=credentials, dialect='standard')
    return count_gbq_temp['f0_'][0],count_gbq_temp['f1_'][0],count_gbq_temp['f2_'][0]


def load_from_temp_gbq_fos():
    row_count, max_date, min_date = check_count_temp_table()

    if int(row_count) != 0 :
        pd.read_gbq(f'''
        insert {GBQ_TABLE_TARGET}
        (
            SnapshotDate,
            SnapshotDateKey,
            MerchantID,
            ArticleNumber,
            Price,
            OfferID,
            IsFavorite,
            IsOffer,
            DeliveryDays,
            delivery_date
        )
        select 
            cast(os.snapshot_date as date),									
            cast(replace(cast(os.snapshot_date as string),'-','') as INT64),
            cast(os.merchant_id	as INT64),									
            cast(os.item_id	as string),											
            cast(os.offer_price	as FLOAT64),									
            cast(os.offer_id  as INT64),									
            cast(os.offer_is_favorite as INT64),							
            cast(1 as INT64),					
            cast(DATE_DIFF(os.snapshot_date, os.offer_delivery_date, DAY) as INT64),           									
            cast(os.offer_delivery_date as date)																				
        from {GBQ_TABLE} os ;
    SELECT 1''',
                project_id=GBQ_PROJECT_ID, credentials=credentials, dialect='standard')
        print(f"Insert data to {GBQ_TABLE_TARGET} - OK")
    else:
        print("No fresh data")
    return True
def drop_temp_table_gbq():
    try:
        pd.read_gbq(f'''DROP TABLE IF EXISTS {GBQ_TABLE}; SELECT 1''',
                    project_id=GBQ_PROJECT_ID, credentials=credentials, dialect='standard')
        print(f"DROP {GBQ_TABLE} - OK")
    except Exception as e:
        print(f" ERROR drop table {GBQ_TABLE}, {e}")

with DAG(
        dag_id='hdfs_to_gbq_fos',
        max_active_runs=1,
        default_args=args,
        schedule_interval='0 10 * * *',
        tags=['greenplum', 'gbq'],
        catchup=False
) as dag:

    arguments = f" --hdfs_path='{HDFS_PATH}'" \
                f" --gbq_project_id='{GBQ_PROJECT_ID}'" \
                f" --hdfs_partition='{HDFS_PARTITION}'" \
                f" --gbq_dataset='{GBQ_DATASET}'" \
                f" --gbq_temp_table='{GBQ_TABLE}'" \
                f" --gbq_mode='{GBQ_MODE}'"

    task = BashOperator(
        task_id="gp_to_gbq",
        bash_command=gen_spark_sumbit(exec="spark-submit", conf_filename="dpspark/configs/base_sdp.json,dpspark/configs/basic/pyspark_gbq.json",
                                          job_filename="/opt/airflow/dpspark/jobs/dp/hdfs_to_gbq.py",
                                          job_extra=arguments
                                          ),
        env={"HADOOP_CONF_DIR": '/opt/airflow/HADOOP_CONF_DIR/sdpprod/',
             "JAVA_TOOL_OPTIONS": '-Xmx64m'},
        append_env=True
    )

    from_temp_gbq = PythonOperator(
        task_id='load_from_tmp_to_target',
        provide_context=True,
        python_callable= load_from_temp_gbq_fos
    )

    drop_temp_table = PythonOperator(
        task_id='drop_temp_table_from_gbq',
        python_callable=drop_temp_table_gbq
    )

    drop_temp_table >> task >> from_temp_gbq
