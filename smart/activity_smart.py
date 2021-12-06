
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
    print("=================================sysVersion================================")
    print("list dir", os.listdir())
    """
    offline version
    """
    sql = """
        SELECT * FROM dm_dsc_smart.dwd_task
        where substr(station_name, 1,4) = 'COAC' 
        and inc_day between '""" + start_date + "' and '" + end_date + """'
        """
    print(sql)
    coach = spark.sql(sql).select("*").toPandas()
    print("==================================read_table================================")
    print(coach.head())
    def data_prepare(coach):

        """
        time unix convert, 
        转换后标准时长换算成秒,
        sprm 计算.
        工作在勤时间
        """
        # coach = pd.read_csv('./data/coach1129_1202.csv', sep = '\001')
        # coach = coach.dropna(how = 'all', axis = 1)
        # coach.columns = [re.sub('\w+\.', '', i) for i in list(coach.columns)]
        
        coach = coach.drop('raw_data', axis = 1)
        def time_convert(col):
            coach[col] = coach[col].astype(int)
            coach[col] = [datetime.fromtimestamp(i).strftime('%Y-%m-%d %H:%M:%S') for i in coach[col]]
            return coach
        for i in ['start_time', 'end_time', 'hire_time']:
            time_convert(i)
        coach['start_time'] = pd.to_datetime(coach['start_time'])
        coach['end_time']   = pd.to_datetime(coach['end_time']) 
        coach = coach[~coach['work_content'].isna()]
        coach = coach[coach['work_content'] != '无效时间']
        coach = coach[coach['worker_post_name'] != '操作经理']
        # sprm calculation
        coach['sprm'] = (60/coach['work_content_refer']).replace([np.inf, -np.inf], 0)
    
        wh = coach.groupby(['worker_name','inc_day']).agg(
            {
                'start_time': 'min',
                'end_time': 'max',
                'sprm': 'sum',
            }
        ).reset_index()
        
        wh['work_hour'] = wh['end_time'] - wh['start_time']
        wh['work_hour_in_min']  = [i.total_seconds()/60 for i in wh['work_hour']]
        wh['work_hour_in_hour'] = [i.total_seconds()/3600 for i in wh['work_hour']]
        wh = wh.drop(['end_time','start_time', 'work_hour'], axis = 1).rename({'sprm':'SPRM_total_of_day'}, axis =1)
        coach = coach.merge(wh, on = ['worker_name', 'inc_day'], how = 'inner')
        # coach = coach[coach['duration'] != '0']
        coach = coach[coach['sprm'] != 0]
        coach['sprm_perhour'] =  coach['SPRM_total_of_day'] / coach['work_hour_in_hour']
        """
        计算转换后时间长度, 换算成 
        秒
        """
        # coach = pd.concat([coach, pd.DataFrame(list(coach['adjusted_duration'].str.split(':')))], axis =1) 
        # coach = coach[coach[[0,1,2]].astype(int).sum(axis = 1) != 0]
        # coach[[0,1,2]] = coach[[0,1,2]].astype(int)
        # coach['time_len'] = coach[0]*3600 + coach[1]*60 + coach[2]
        # coach = coach.drop([0,1,2], axis = 1) 
        
        return coach
    coach = data_prepare(coach)


    coach_out = coach[['worker_name', 'work_group_name', 'up_worker_name', 'hire_time',\
            'worker_post_name', 'SPRM_total_of_day', 'work_hour_in_min',
            'work_hour_in_hour', 'sprm_perhour', 'station_name', 'inc_day']].drop_duplicates()
    coach_out['inc_day'] = coach_out['inc_day'].astype(str)
    df = coach
    print("===============================data_mani_done================================")
    print(df.head())

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
        merge_table = "tmp_" + merge_table
    
    inc_df = spark.sql("""select * from df""")
    print("===============================merge_table=================================")
    print(merge_table)
    
    spark.sql("""set spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict""")
    # (table_name, df, pk_cols, order_cols, partition_cols=None):
    merge_data = MergeDFToTable(merge_table, inc_df, \
        "worker_name,inc_day", "inc_day", partition_cols="inc_day")
    merge_data.merge()
    

def main():
    args = argparse.ArgumentParser()
    args.add_argument("--start_date", help="start date for refresh data, format: yyyyMMdd"
                          , default=[(datetime.now()).strftime("%Y%m%d")], nargs="*")
    args.add_argument("--end_date", help="start date for refresh data, format: yyyyMMdd"
                          , default=[(datetime.now()).strftime("%Y%m%d")], nargs="*")
    args.add_argument("--env", help="dev environment or prod environment", default="dev", nargs="*")

    args_parse = args.parse_args()
    start_date = args_parse.start_date[0]
    end_date = args_parse.end_date[0]
    env = args_parse.env[0]
 
    run_etl(start_date, end_date, env)

    
if __name__ == '__main__':
    main()

    

# %%
