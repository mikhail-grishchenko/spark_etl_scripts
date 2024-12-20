import fire
from dateutil.parser import parse
from loguru import logger
from pyspark.sql import SparkSession
from dependencies.datacatalog import getUpdateDt, setUpdateDt
from pyspark.sql.functions import *


GBQ_URL = 'jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443'

def gp_to_gbq(hdfs_path: str, gbq_project_id: str, gbq_temp_table: str,
              hdfs_partition: str, gbq_dataset: str = None, gbq_mode: str = "overwrite"):
    """
    Args:
        secret_path_gp: vault secret path

    Returns:
        None
    """
    logger.info("create SparkSession")
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    gbq_table_dc = f"gbq.{gbq_project_id}.{gbq_temp_table}"

    # prevous increment column value
    update_dt = parse(getUpdateDt(gbq_table_dc))
    logger.info(f'Data catalog timestamp: {update_dt}')
    if update_dt is None or update_dt == "2000-01-01T00:00:00Z":
        raise Exception('There is no mt_update_dt from smmdc! Need to code here to get maximum value from '
                        'DESTINATION table or load first bucket')


    # HDFS
    logger.info("read data from HDFS")
    df = spark.read.parquet(f'{hdfs_path}')
    df = df.filter(df[hdfs_partition] > update_dt)


    df = df.cache()
    logger.info(f'Loaded columns: {df.columns}')
    df_count = df.count()
    logger.info(f'Loaded {df_count} rows')


    # GBQ
    logger.info("write data to GBQ - START")
    if df_count > 0:

        mt_update_dt_max = (df.select(to_timestamp(hdfs_partition, "yyyy-MM-dd'T'HH:mm:ss").alias("compute_timestamp"))
                            .agg({"compute_timestamp": "max"}).collect()[0][0])
        logger.info(f"last loaded mt_update_dt {mt_update_dt_max}")

        gbq_writer = (df.write
          .format('bigquery')
          .option("writeMethod", "direct")
          .option('parentProject', gbq_project_id)
          .option("url",GBQ_URL)
          .option("viewsEnabled", True)
        )

        if gbq_dataset is not None:
            gbq_writer = gbq_writer.option("dataset", gbq_dataset)
        
        gbq_writer.mode(gbq_mode).save(gbq_temp_table)

        logger.info("Save data to temp gbq table - OK")
        
        setUpdateDt(gbq_table_dc, mt_update_dt_max)

    else:
        logger.info('DataFrame is empty')
        return None
if __name__ == '__main__':
    fire.Fire(gp_to_gbq)
