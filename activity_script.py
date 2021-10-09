from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import argparse 

spark = SparkSession.builder.appName(
    "activity_scale"
    ).enableHiveSupport().getOrCreate()




def run_etl(tables, today):

    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
    names = tables.split(",")
    today = today 

    # today = '20210101'
    # names = ['aaa', 'bbb']

    for i in names: 
        sql = """
        select
        a.warehouse as wms_warehouse_id
        ,a.user_name as user_id
        ,a.work_group as activity_type
        ,a.work_type as activity_sub_type
        ,a.activity_date_time as activity_start_time
        ,b.activity_date_time as activity_end_time
        ,a.container_id as lpn
        ,a.item as sku_code
        ,a.reference_id as order_id
        ,a.location as from_location
        ,b.location as to_location
        ,a.quantity as qty
        ,a.activity_date_time as create_time,
        cast(a.internal_container_num as bigint) as internal_container_num,
        cast(a.internal_id as bigint) as internal_id,
        a.internal_key_id as activity_id,
        date_format(a.activity_date_time, 'HH') as hh,
        a.company as wms_company_id,
        a.transaction_type,
        null as transaction_code
        from 
        """ + i + """.transaction_history a 
        left join 
        """ + i + """.transaction_history b
        on b.inc_day = '""" + today + """'
        and b.direction = 'To'
        and a.reference_id = b.reference_id
        and a.item = b.item  
        where a.inc_day = '""" + today + """'
        and a.direction = 'From'
        union all 

        select
            warehouse as wms_warehouse_id,
            user_name as user_id,
            work_group as activity_type,
            work_type as activity_sub_type,
            activity_date_time as activity_start_time,
            null as activity_end_time,
            container_id as lpn,
            item as sku_code,
            reference_id as order_id,
            location as from_location,
            location as to_location,
            quantity as qty,
            activity_date_time as create_time,
            cast(internal_container_num as bigint) as internal_container_num,
            cast(internal_id as bigint) as internal_id,
            internal_key_id as activity_id,
            date_format(activity_date_time, 'HH') as hh,
            '""" + i + """' as data_source,
            company as wms_company_id,
            transaction_type,
            null as transaction_code
        from 
        """ + i + """.transaction_history  
        where
            inc_day = '""" + today + """'
        and coalesce(direction, '') = ''"""

        print(sql)
        sql = sql.replace('\n', '')
        spark.sql(sql)



def main():
    args = argparse.ArgumentParser()
    tables = "ods_cn_bose,ods_cn_apple_sz,ods_cn_apple_sh,ods_cn_costacoffee,ods_cn_diadora,ods_cn_ferrero,ods_cn_fuji,ods_cn_hd,ods_cn_hp_ljb,ods_cn_hpi,ods_cn_hualiancosta,ods_cn_jiq,ods_cn_kone,ods_cn_michelin,ods_cn_razer,ods_cn_squibb,ods_cn_vzug,ods_cn_zebra,ods_dbo,ods_hk_abbott,ods_hk_revlon,ods_hk_fredperry"
    # default. not passing
    args.add_argument("--tables", help="tables"
                      , default=[tables], nargs="*")
    # passing 
    args.add_argument("--inc_day", help="inc_dayx format: yyyyMMdd"
                      , default=[datetime.now().strftime("%Y%m%d")], 
                      nargs="*")
    args_parse = args.parse_args()
    table_names = args_parse.tables[0]
    today = args_parse.inc_day[0] 
    run_etl(table_names, today)

if __name__ == '__main__':
    main()
 


 