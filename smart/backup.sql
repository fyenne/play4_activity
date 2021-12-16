/* backup */


drop table tmp_dsc_dws.dws_dsc_smart_work_efficiency_sum_di;

CREATE EXTERNAL TABLE `tmp_dsc_dws.dws_dsc_smart_work_efficiency_sum_di`(
`worker_name` string COMMENT '',
`work_group_name` string COMMENT '',
`up_worker_name` string COMMENT '',
`hire_time` string COMMENT '',
`worker_post_name` string COMMENT '',
`sprm_total_of_day` double COMMENT '',
`work_hour_in_min` double COMMENT '',
`work_hour_in_hour` double COMMENT '',
`sprm_perhour` double COMMENT '',
`station_name` string COMMENT '')
COMMENT 'smart_activity_efficiency'
PARTITIONED BY (
`inc_day` string COMMENT '日分区')
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
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