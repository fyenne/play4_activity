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
    sql_upper = """
        insert
            overwrite table tmp_dsc_dwd.dwd_dsc_wh_activity_di partition(
                inc_day = '""" + today + """',
                src = 'scale'
            )
        select
            a.wms_warehouse_id,
            user_id,
            activity_type,
            activity_sub_type,
            from_unixtime(
                cast(
                    unix_timestamp(activity_start_time) + 28800 as bigint
                ),
                'yyyy-MM-dd HH:mm:ss'
            ) as activity_start_time,
            activity_end_time,
            lpn,
            sku_code,
            order_id,
            from_location,
            to_location,
            qty,
            from_unixtime(
                cast(
                    unix_timestamp(create_time) + 28800 as bigint
                ),
                'yyyy-MM-dd HH:mm:ss'
            ) as create_time,
            cast(internal_container_num as bigint),
            internal_id,
            activity_id,
            cast(hh + 8 as bigint) as hh,
            a.data_source,
            a.wms_company_id,
            gcd.description as transaction_type,
            transaction_code,
            case
                when activity_type is null then case
                    when substr(activity_sub_type, 1, 4) = 'Ship' then 'Shipping'
                    when activity_sub_type = 'Receipt' then 'Receiving'
                    else case
                        when gcd.description = 'Inventory status change' then 'Inventory status change'
                        when gcd.description = 'Inventory adjustment' then 'Inventory Management'
                        when gcd.description = 'Check In' then 'Receiving '
                        when gcd.description = 'Inventory transfer' then 'Inventory Movement'
                        else 'Misc'
                    end
                end
                else activity_type
            end as standard_activity_type,
            rel.ou as ou_code
        from(
        """
    sql_miche = """
        select
            case
                when substr(a.work_zone, 1, 2) = 'OE' then 'OE'
                when substr(a.work_zone, 1, 2) = 'RT' then 'RT'
                when substr(a.work_zone, 1, 2) = 'la' then 'OE'
                else 'UNKNOWN'
            end as wms_warehouse_id,
            a.user_name as user_id,
            a.work_group as activity_type,
            a.work_type as activity_sub_type,
            a.activity_date_time as activity_start_time,
            b.activity_date_time as activity_end_time,
            a.container_id as lpn,
            a.item as sku_code,
            a.reference_id as order_id,
            a.location as from_location,
            b.location as to_location,
            a.quantity as qty,
            a.activity_date_time as create_time,
            cast(a.internal_container_num as bigint) as internal_container_num,
            cast(a.internal_id as bigint) as internal_id,
            a.internal_key_id as activity_id,
            date_format(a.activity_date_time, 'HH') as hh,
            'ods_cn_michelin' as data_source,
            a.company as wms_company_id,
            a.transaction_type,
            null as transaction_code
        from  
        ods_cn_michelin.transaction_history a 
        left join 
        ods_cn_michelin.transaction_history b
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
            'ods_cn_michelin' as data_source,
            company as wms_company_id,
            transaction_type,
            null as transaction_code
        from 
        ods_cn_michelin.transaction_history  
        where
            inc_day = '""" + today + """'
        and coalesce(direction, '') = ''
        """
 
    sql = ""
    for i in names: 
        sql += """
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
        '""" + i + """' as data_source,
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
        and coalesce(direction, '') = ''
        union all
        """
         
        sql_long = sql.replace('\n', '')
        print(sql_long)
        
        sql_tail = """
        ) AS a
        LEFT JOIN dsc_dim.dim_dsc_ou_whse_rel rel
        ON lower(a.wms_warehouse_id) = lower(rel.wms_warehouse_id) 
        AND lower(coalesce(a.wms_company_id, '')) = lower(coalesce(rel.company_id , ''))
        LEFT JOIN ods_cn_bose.generic_config_detail gcd
        ON gcd.record_type='HIST TR TY' AND a.transaction_type = gcd.identifier
        """

        sql_final = (sql_upper + sql_long + sql_miche + sql_tail).replace('\n', '')
        spark.sql(sql_final)

def main():
    args = argparse.ArgumentParser()
    tables = "ods_cn_bose,ods_cn_apple_sz,ods_cn_apple_sh,ods_cn_costacoffee,ods_cn_diadora,ods_cn_ferrero,ods_cn_fuji,ods_cn_hd,ods_cn_hp_ljb,ods_cn_hpi,ods_cn_hualiancosta,ods_cn_jiq,ods_cn_kone,ods_cn_razer,ods_cn_squibb,ods_cn_vzug,ods_cn_zebra,ods_dbo,ods_hk_abbott,ods_hk_revlon,ods_hk_fredperry"
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
 


 