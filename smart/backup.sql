/* backup */

 
drop  table dsc_dws.dws_dsc_smart_work_efficiency_sum_di;


CREATE EXTERNAL TABLE `dsc_dws.dws_dsc_smart_work_efficiency_sum_di`(
station_name string comment ''
,work_content string comment ''
,worker_name string comment ''
,work_group_name string comment ''
,up_worker_name string comment ''
,worker_level_name string comment ''
,hire_time string comment ''
,duration bigint comment ''
,duration_in_hour double comment ''
,work_num_sum double comment ''
,work_content_cnt bigint comment ''
,sprm_sum double comment ''
,tt_duration double comment ''
,tt_adj_duration double comment ''
,tt_sprm double comment ''
,tt_work_hour double comment ''
,tt_adj_duration_in_hour double comment ''
,sprm_perhour double comment ''
)
COMMENT 'smart summary 2'
PARTITIONED BY (
`inc_day` string)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
'field.delim'='\u0001',
'line.delim'='\n',
'serialization.format'='\u0001')
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'

 

 



SELECT 
account_name
,station_id
,station_code
,station_name
,worker_id
,worker_name
,work_group_id
,work_group_name
,up_worker_id
,up_worker_name
,hire_time
,worker_post_id
,worker_post_name
,work_num
,start_time
,end_time
,duration
,adjustment_duration
,adjusted_duration
,sku_no
,work_content
,work_content_type
,work_content_is_measure
,work_content_refer
,start_date
,end_date
,report_date ,
inc_day
FROM dm_dsc_smart.dwd_task
 where substr(station_name, 1,4) = 'COAC' 
 and inc_day >= '20211129'
 


# col order. 
'station_name',
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
'tt_sprm',
'tt_work_hour',
'tt_adj_duration_in_hour',
'sprm_perhour',