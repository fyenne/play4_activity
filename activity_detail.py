import pandas as pd
import numpy as np
import re
import os
import argparse
from datetime import datetime, timedelta
os.getcwd()

import warnings
warnings.filterwarnings('ignore')
from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
spark = SparkSession.builder.enableHiveSupport().getOrCreate()

spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')



def run_python(start_day, end_day):
        
    """
    spark to pandas
    """
    # start_day = datetime.today().strftime('%Y%m%d')
    # end_day = datetime.today().strftime('%Y%m%d')
    sql0 = "select * from dsc_dws.dws_dsc_activity_hourly_detail_di where inc_day between '" + \
            start_day  + "' and '" + end_day  + "'"
    
    print(sql0)
    df = spark.sql(sql0)
    df.show(15,False)

    df = df.select("*").toPandas()

    """
    data prepare
    """
    print("=================================0================================")

    def data_prepare(df):
        df = df.dropna(subset=['activity_start_time'])
        df['activity_end_time'] = pd.to_datetime(df['activity_end_time'])
        df['activity_start_time'] = pd.to_datetime(df['activity_start_time'].fillna(0))
        df['create_time'] = pd.to_datetime(df['create_time'])
        """
        d单个activity的耗费时间.
        """
        df['time_gap_inner'] = df['activity_end_time']-df['activity_start_time']
        df['time_gap_inner'] = df['time_gap_inner'].fillna(
            timedelta(0)
            ).apply(timedelta.total_seconds)
        df['time_gap_inner'] = [0 if i < 0 else i for i in df['time_gap_inner']]

        """
        前一个activity到后一个activity相差的时间\.
        """
        df = df.sort_values('activity_start_time')
        df['time_gap_outer'] = df\
            .groupby(['user_id','inc_day'], dropna = False)['activity_start_time'].transform('diff')
        df['time_gap_outer'] = df['time_gap_outer'].fillna(timedelta(0)).apply(timedelta.total_seconds)
        df['time_gap_outer'] = [0 if i< 0 else i for i in df['time_gap_outer']]

        """
        time gap of the day. 总计活跃时间
        """
        mid1 = df.groupby(['inc_day', 'user_id']).agg(
            a = ('activity_start_time','min'), 
            b  = ('activity_start_time', 'max'))\
                [['a','b']].diff(axis = 1)['b'].apply(timedelta.total_seconds).reset_index()
        mid1.columns = ['inc_day','user_id','time_gap_today']
        df = df.merge(mid1, on = ['inc_day', 'user_id'], how = 'left')
        
        return df

    def get_hour(df):
        list = ['activity_start_time', 'activity_end_time','create_time']
        
        for i in list: 
            listnew = i + '_hour'
            df[listnew] = df[i].dt.round('min').astype(str).str.slice(11,16)
        return df
    df = data_prepare(df)
    df = get_hour(df) 


    df = df[['ou_code', 'ou_name', 'bg_code', 'bg_name_cn', 'customer_id',
        'customer_name', 'hour',
        'wms_warehouse_id', 'user_id', 'activity_type', 'activity_sub_type',
        'activity_start_time', 'activity_end_time', 'lpn', 'sku_code',
        'order_id', 'from_location', 'to_location', 'qty', 'create_time',
        'activity_id', 'inc_day',
        'time_gap_inner','time_gap_outer','time_gap_today',
        'activity_start_time_hour', 'activity_end_time_hour', 'create_time_hour']]

    df = spark.createDataFrame(df)
    df.show(11, False)
    df.createOrReplaceTempView("df")
 

    print("=================================data_prepare================================")



def main():
    args = argparse.ArgumentParser()
    default_day = datetime.today().strftime('%Y%m%d')
    args.add_argument("--start_day", help="start day for refresh data, format: yyyyMMdd"
                      , default=[default_day], nargs="*")
    args.add_argument("--end_day", help="end day for refresh data, format: yyyyMMdd"
                      , default=[default_day], nargs="*")

    args_parse = args.parse_args()
    start_day = args_parse.start_day[0]
    end_day = args_parse.end_day[0]

    run_python(start_day, end_day)


if __name__ == '__main__':
    main()

