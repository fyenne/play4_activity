
# %%
import pandas as pd 
import numpy as np 
import os
import re
import warnings
warnings.filterwarnings("ignore")
from datetime import date, datetime
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
from MergeDataFrameToTable import MergeDFToTable
spark = SparkSession.builder.enableHiveSupport().getOrCreate() 
 
import sys 


def run_etl(start_date, end_date ,env):
    # path = './cxm/'
    print("python version here:", sys.version, '\t') 
    print("=================================env:: %s================================"%env)
    print("list dir", os.listdir())
    print("list dir", env)
    """
    表一: 人天表. 总
    表二: 动作人表. 分
    """
    sql = """
        SELECT 
        worker_name,
        inc_day, 
        station_name, 
        work_group_name, 
        hire_time,
        min(start_time) as start_time, 
        max(end_time) as end_time, 
        sum(duration) as duration, 
        sum(adjusted_duration) as adjusted_duration, 
        coalesce(sum(60/work_content_refer), 0) as sprm
        FROM dm_dsc_smart.dwd_task
        where inc_day between '""" + start_date + "' and '" + end_date + """'
        and work_content != ''
        and work_content != '无效时间'
        and duration != 0
        group by 
        worker_name, 
        inc_day, 
        station_name, 
        work_group_name,
        hire_time
        """
    print(sql)
    coach = spark.sql(sql).select("*").toPandas()

    sql2 = """
        SELECT
        station_name,
        work_content,
        worker_name,
        work_group_name,
        up_worker_name,
        worker_level_name,
        sum(duration) as duration,
        sum(duration) / 3600 as duration_in_hour,
        sum(work_num) as work_num_sum,
        count(0) as work_content_cnt,
        coalesce(sum(60/work_content_refer), 0) as sprm_sum,
        inc_day
        FROM
        dm_dsc_smart.dwd_task
        where duration != 0
        and work_content not in ('无效时间', '转换时间')
        and inc_day between '""" + start_date + "' and '" + end_date + """'
        group by
        station_name,
        work_content,
        worker_name,
        work_group_name,
        up_worker_name,
        worker_level_name,
        inc_day 
        """

    print(sql2)
    coach2 = spark.sql(sql2).select("*").toPandas()
    print("==================================read_table================================")
    print(coach.head())
        # where substr(station_name, 1,4) = 'COAC' 
        # or substr(station_name, 1,4) = 'SIEM'
        # station_name in ('SIEMENS SZV XXX WHS', 'COACH SHA WGQ WHS')
 
    def data_prepare(coach):
        # coach = pd.read_csv('./data/coach1129_1202.csv', sep = '\001')
        # coach = coach.dropna(how = 'all', axis = 1)
        # coach.columns = [re.sub('\w+\.', '', i) for i in list(coach.columns)]
        # coach = coach.dropna(how = 'all', axis = 1)
        # coach = coach.drop('raw_data', axis = 1)
        # time_convert()
        def time_convert(col):
            coach[col] = coach[col].astype(int)
            coach[col] = [datetime.fromtimestamp(i).strftime('%Y-%m-%d %H:%M:%S') for i in coach[col]]
            return coach
        for i in ['start_time', 'end_time', 'hire_time']:
            time_convert(i)
        
        coach['sprm'] = coach['sprm'].fillna(0)
        coach['start_time'] = pd.to_datetime(coach['start_time'])
        coach['end_time']   = pd.to_datetime(coach['end_time'])
        wh = coach
        del coach
        # coach = coach[~coach['work_content'].isna()]
        # coach = coach[coach['work_content'] != '无效时间']
        # sprm calculation
        # coach['sprm'] = (60/coach['work_content_refer']).replace([np.inf, -np.inf], 0)

        # wh = coach.groupby([
        #     'worker_name','inc_day', 'station_name', 'work_group_name',
        #     ]).agg(
        #     {
        #         'start_time': 'min',
        #         'end_time': 'max',
        #         'duration':'sum',
        #         'adjusted_duration': 'sum',
        #         'sprm': 'sum',
        #     }
        # ).reset_index()

        wh['work_hour'] = wh['end_time'] - wh['start_time']
        # wh['work_hour_in_min']  = [i.total_seconds()/60 for i in wh['work_hour']]
        wh['tt_work_hour'] = [i.total_seconds()/3600 for i in wh['work_hour']]
        wh['tt_adj_duration_in_hour']  = wh['adjusted_duration']/3600
        wh = wh.drop(['end_time','start_time', 'work_hour'], axis = 1)
        wh = wh.rename({'sprm':'tt_sprm', 'duration' : 'tt_duration', 'adjusted_duration': 'tt_adj_duration' }, axis =1 )
        wh = wh[wh['tt_sprm'] != 0]
        wh['sprm_perhour'] =  wh['tt_sprm'] / wh['tt_work_hour']
        return wh
    wh = data_prepare(coach)
    # wh['inc_day'] = wh['inc_day'].astype(str)
    df = coach2.merge(
        wh, on = ['station_name', 'worker_name', 'inc_day', 'work_group_name'], how = 'left')
    df['inc_day'] = df['inc_day'].astype(int).astype(str)
    df['hire_time'] = df['hire_time'] .astype(str)
    df['sprm_hour_day'] = df.groupby('worker_name')['tt_sprm'].transform('mean') \
        / df.groupby('worker_name')['tt_work_hour'].transform('mean')

    print("===============================data_cols================================")
    print(df.columns)

    df = df[['station_name',
        'work_content',
        'worker_name',
        'work_group_name',
        'up_worker_name',
        'worker_level_name',
        'hire_time',
        'duration',
        'duration_in_hour',
        'work_num_sum',
        'work_content_cnt',
        'sprm_sum',
        'tt_duration',
        'tt_adj_duration',
        'tt_adj_duration_in_hour',
        'tt_sprm',
        'tt_work_hour',
        'sprm_perhour',
        'sprm_hour_day',
        'inc_day',]]
    

    # df.columns = ['station_name',
    #     'work_content',
    #     'worker_name',
    #     'work_group_name',
    #     'up_worker_name',
    #     'worker_level_name',
    #     'hire_time',
    #     'duration',
    #     'duration_in_hour',
    #     'work_num_sum',
    #     'work_content_cnt',
    #     'sprm_sum',
    #     'tt_duration',
    #     'tt_adj_duration',
    #     'tt_sprm',
    #     'tt_work_hour',
    #     'tt_adj_duration_in_hour',
    #     'sprm_perhour',
    #     'inc_day']

    print("===============================data_mani_done================================")
    print(df.info())
    
    """
    to bdp
    """
    # pd to spark table
    spark_df = spark.createDataFrame(df)
    # spark table as view, aka in to spark env. able to be selected or run by spark sql in the following part.
    spark_df.createOrReplaceTempView("df")
    # 
    print("==============================spark_df, env=%s!================================="%env)
    print(spark_df)

    """
    merge table preparation:
    """


    merge_table = "dsc_dws.dws_dsc_smart_work_efficiency_sum_di"
    if env == 'dev':
        merge_table =  merge_table
    else:
        merge_table = "dsc_dws.dws_dsc_smart_work_efficiency_sum_di"
    
    inc_df = spark.sql("""select * from df""")
    print("===============================merge_table=================================")
    print(merge_table)
    
    spark.sql("""set spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict""")
    # (table_name, df, pk_cols, order_cols, partition_cols=None):
    merge_data = MergeDFToTable(merge_table, inc_df, \
        "work_content,worker_name,inc_day", "inc_day", partition_cols="inc_day")
    merge_data.merge()
    

def main():
    args = argparse.ArgumentParser()
    args.add_argument("--start_date", help="start date for refresh data, format: yyyyMMdd"
                          , default=[(datetime.now()).strftime("%Y%m%d")], nargs="*")
    args.add_argument("--end_date", help="start date for refresh data, format: yyyyMMdd"
                          , default=[(datetime.now()).strftime("%Y%m%d")], nargs="*")
    args.add_argument("--env", help="dev environment or prod environment", default=["dev"], nargs="*")

    args_parse = args.parse_args()
    start_date = args_parse.start_date[0]
    end_date = args_parse.end_date[0]
    env = args_parse.env[0]
    print(env)
    run_etl(start_date, end_date, env)

    
if __name__ == '__main__':
    main()

    

# %%
