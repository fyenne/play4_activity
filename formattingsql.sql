SET tez.queue.name=root.dsc;
SET hive.execution.engine=tez;

INSERT OVERWRITE TABLE tmp_dsc_dwd.dwd_dsc_wh_activity_di partition (inc_day='$[time(yyyyMMdd)]', src='scale')
SELECT  a.wms_warehouse_id
       ,user_id
       ,activity_type
       ,activity_sub_type
       ,from_unixtime( cast( unix_timestamp(activity_start_time) + 28800 AS bigint ),'yyyy-MM-dd HH:mm:ss' )                    AS activity_start_time
       ,activity_end_time
       ,lpn
       ,sku_code
       ,order_id
       ,from_location
       ,to_location
       ,qty
       ,from_unixtime( cast( unix_timestamp(create_time) + 28800 AS bigint ),'yyyy-MM-dd HH:mm:ss' )                            AS create_time
       ,cast(internal_container_num                                                                                             AS bigint)
       ,internal_id
       ,activity_id
       ,cast(hh + 8 AS bigint)                                                                                                  AS hh
       ,a.data_source
       ,a.wms_company_id
       ,gcd.description                                                                                                         AS transaction_type
       ,transaction_code
       ,CASE WHEN activity_type is null THEN CASE
             WHEN substr(activity_sub_type,1,4) = 'Ship' THEN 'Shipping'
             WHEN activity_sub_type = 'Receipt' THEN 'Receiving'  ELSE CASE
             WHEN gcd.description = 'Inventory status change' THEN 'Inventory status change'
             WHEN gcd.description = 'Inventory adjustment' THEN 'Inventory Management'
             WHEN gcd.description = 'Check In' THEN 'Receiving '
             WHEN gcd.description = 'Inventory transfer' THEN 'Inventory Movement'  ELSE 'Misc' END END  ELSE activity_type END AS standard_activity_type
       ,rel.ou                                                                                                                  AS ou_code
FROM
(
	SELECT  CASE WHEN substr(work_zone,1,2) = 'OE' THEN 'OE'
	             WHEN substr(work_zone,1,2) = 'RT' THEN 'RT'
	             WHEN substr(work_zone,1,2) = 'la' THEN 'OE'  ELSE 'UNKNOWN' END AS wms_warehouse_id
	       ,user_name                                                            AS user_id
	       ,work_group                                                           AS activity_type
	       ,work_type                                                            AS activity_sub_type
	       ,activity_date_time                                                   AS activity_start_time
	       ,null                                                                 AS activity_end_time
	       ,container_id                                                         AS lpn
	       ,item                                                                 AS sku_code
	       ,reference_id                                                         AS order_id
	       ,CASE WHEN direction = 'From' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END   AS from_location
	       ,CASE WHEN direction = 'To' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END   AS to_location
	       ,quantity                                                             AS qty
	       ,activity_date_time                                                   AS create_time
	       ,cast(internal_container_num AS bigint)                               AS internal_container_num
	       ,cast(internal_id AS bigint)                                          AS internal_id
	       ,internal_key_id                                                      AS activity_id
	       ,date_format(activity_date_time,'HH')                                 AS hh
	       ,'ods_cn_michelin'                                                    AS data_source
	       ,company                                                              AS wms_company_id
	       ,transaction_type
	       ,null                                                                 AS transaction_code
	FROM ods_cn_michelin.transaction_history
	WHERE inc_day = '$[time(yyyyMMdd)]' 
	UNION ALL
	SELECT  warehouse                                                          AS wms_warehouse_id
	       ,user_name                                                          AS user_id
	       ,work_group                                                         AS activity_type
	       ,work_type                                                          AS activity_sub_type
	       ,activity_date_time                                                 AS activity_start_time
	       ,null                                                               AS activity_end_time
	       ,container_id                                                       AS lpn
	       ,item                                                               AS sku_code
	       ,reference_id                                                       AS order_id
	       ,CASE WHEN direction = 'From' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END AS from_location
	       ,CASE WHEN direction = 'To' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END AS to_location
	       ,quantity                                                           AS qty
	       ,activity_date_time                                                 AS create_time
	       ,cast(internal_container_num AS bigint)                             AS internal_container_num
	       ,cast(internal_id AS bigint)                                        AS internal_id
	       ,internal_key_id                                                    AS activity_id
	       ,date_format(activity_date_time,'HH')                               AS hh
	       ,'ods_cn_bose'                                                      AS data_source
	       ,company                                                            AS wms_company_id
	       ,transaction_type
	       ,null                                                               AS transaction_code
	FROM ods_cn_bose.transaction_history
	WHERE inc_day = '$[time(yyyyMMdd)]' 
	UNION ALL
	SELECT  warehouse                                                          AS wms_warehouse_id
	       ,user_name                                                          AS user_id
	       ,work_group                                                         AS activity_type
	       ,work_type                                                          AS activity_sub_type
	       ,activity_date_time                                                 AS activity_start_time
	       ,null                                                               AS activity_end_time
	       ,container_id                                                       AS lpn
	       ,item                                                               AS sku_code
	       ,reference_id                                                       AS order_id
	       ,CASE WHEN direction = 'From' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END AS from_location
	       ,CASE WHEN direction = 'To' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END AS to_location
	       ,quantity                                                           AS qty
	       ,activity_date_time                                                 AS create_time
	       ,cast(internal_container_num AS bigint)                             AS internal_container_num
	       ,cast(internal_id AS bigint)                                        AS internal_id
	       ,internal_key_id                                                    AS activity_id
	       ,date_format(activity_date_time,'HH')                               AS hh
	       ,'ods_cn_apple_sz'                                                  AS data_source
	       ,company                                                            AS wms_company_id
	       ,transaction_type
	       ,null                                                               AS transaction_code
	FROM ods_cn_apple_sz.transaction_history
	WHERE inc_day = '$[time(yyyyMMdd)]' 
	UNION ALL
	SELECT  warehouse                                                          AS wms_warehouse_id
	       ,user_name                                                          AS user_id
	       ,work_group                                                         AS activity_type
	       ,work_type                                                          AS activity_sub_type
	       ,activity_date_time                                                 AS activity_start_time
	       ,null                                                               AS activity_end_time
	       ,container_id                                                       AS lpn
	       ,item                                                               AS sku_code
	       ,reference_id                                                       AS order_id
	       ,CASE WHEN direction = 'From' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END AS from_location
	       ,CASE WHEN direction = 'To' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END AS to_location
	       ,quantity                                                           AS qty
	       ,activity_date_time                                                 AS create_time
	       ,cast(internal_container_num AS bigint)                             AS internal_container_num
	       ,cast(internal_id AS bigint)                                        AS internal_id
	       ,internal_key_id                                                    AS activity_id
	       ,date_format(activity_date_time,'HH')                               AS hh
	       ,'ods_cn_apple_sh'                                                  AS data_source
	       ,company                                                            AS wms_company_id
	       ,transaction_type
	       ,null                                                               AS transaction_code
	FROM ods_cn_apple_sh.transaction_history
	WHERE inc_day = '$[time(yyyyMMdd)]' 
	UNION ALL
	SELECT  warehouse                                                          AS wms_warehouse_id
	       ,user_name                                                          AS user_id
	       ,work_group                                                         AS activity_type
	       ,work_type                                                          AS activity_sub_type
	       ,activity_date_time                                                 AS activity_start_time
	       ,null                                                               AS activity_end_time
	       ,container_id                                                       AS lpn
	       ,item                                                               AS sku_code
	       ,reference_id                                                       AS order_id
	       ,CASE WHEN direction = 'From' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END AS from_location
	       ,CASE WHEN direction = 'To' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END AS to_location
	       ,quantity                                                           AS qty
	       ,activity_date_time                                                 AS create_time
	       ,cast(internal_container_num AS bigint)                             AS internal_container_num
	       ,cast(internal_id AS bigint)                                        AS internal_id
	       ,internal_key_id                                                    AS activity_id
	       ,date_format(activity_date_time,'HH')                               AS hh
	       ,'ods_cn_costacoffee'                                               AS data_source
	       ,company                                                            AS wms_company_id
	       ,transaction_type
	       ,null                                                               AS transaction_code
	FROM ods_cn_costacoffee.transaction_history
	WHERE inc_day = '$[time(yyyyMMdd)]' 
	UNION ALL
	SELECT  warehouse                                                          AS wms_warehouse_id
	       ,user_name                                                          AS user_id
	       ,work_group                                                         AS activity_type
	       ,work_type                                                          AS activity_sub_type
	       ,activity_date_time                                                 AS activity_start_time
	       ,null                                                               AS activity_end_time
	       ,container_id                                                       AS lpn
	       ,item                                                               AS sku_code
	       ,reference_id                                                       AS order_id
	       ,CASE WHEN direction = 'From' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END AS from_location
	       ,CASE WHEN direction = 'To' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END AS to_location
	       ,quantity                                                           AS qty
	       ,activity_date_time                                                 AS create_time
	       ,cast(internal_container_num AS bigint)                             AS internal_container_num
	       ,cast(internal_id AS bigint)                                        AS internal_id
	       ,internal_key_id                                                    AS activity_id
	       ,date_format(activity_date_time,'HH')                               AS hh
	       ,'ods_cn_diadora'                                                   AS data_source
	       ,company                                                            AS wms_company_id
	       ,transaction_type
	       ,null                                                               AS transaction_code
	FROM ods_cn_diadora.transaction_history
	WHERE inc_day = '$[time(yyyyMMdd)]' 
	UNION ALL
	SELECT  warehouse                                                          AS wms_warehouse_id
	       ,user_name                                                          AS user_id
	       ,work_group                                                         AS activity_type
	       ,work_type                                                          AS activity_sub_type
	       ,activity_date_time                                                 AS activity_start_time
	       ,null                                                               AS activity_end_time
	       ,container_id                                                       AS lpn
	       ,item                                                               AS sku_code
	       ,reference_id                                                       AS order_id
	       ,CASE WHEN direction = 'From' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END AS from_location
	       ,CASE WHEN direction = 'To' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END AS to_location
	       ,quantity                                                           AS qty
	       ,activity_date_time                                                 AS create_time
	       ,cast(internal_container_num AS bigint)                             AS internal_container_num
	       ,cast(internal_id AS bigint)                                        AS internal_id
	       ,internal_key_id                                                    AS activity_id
	       ,date_format(activity_date_time,'HH')                               AS hh
	       ,'ods_cn_ferrero'                                                   AS data_source
	       ,company                                                            AS wms_company_id
	       ,transaction_type
	       ,null                                                               AS transaction_code
	FROM ods_cn_ferrero.transaction_history
	WHERE inc_day = '$[time(yyyyMMdd)]' 
	UNION ALL
	SELECT  warehouse                                                          AS wms_warehouse_id
	       ,user_name                                                          AS user_id
	       ,work_group                                                         AS activity_type
	       ,work_type                                                          AS activity_sub_type
	       ,activity_date_time                                                 AS activity_start_time
	       ,null                                                               AS activity_end_time
	       ,container_id                                                       AS lpn
	       ,item                                                               AS sku_code
	       ,reference_id                                                       AS order_id
	       ,CASE WHEN direction = 'From' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END AS from_location
	       ,CASE WHEN direction = 'To' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END AS to_location
	       ,quantity                                                           AS qty
	       ,activity_date_time                                                 AS create_time
	       ,cast(internal_container_num AS bigint)                             AS internal_container_num
	       ,cast(internal_id AS bigint)                                        AS internal_id
	       ,internal_key_id                                                    AS activity_id
	       ,date_format(activity_date_time,'HH')                               AS hh
	       ,'ods_cn_fuji'                                                      AS data_source
	       ,company                                                            AS wms_company_id
	       ,transaction_type
	       ,null                                                               AS transaction_code
	FROM ods_cn_fuji.transaction_history
	WHERE inc_day = '$[time(yyyyMMdd)]' 
	UNION ALL
	SELECT  warehouse                                                          AS wms_warehouse_id
	       ,user_name                                                          AS user_id
	       ,work_group                                                         AS activity_type
	       ,work_type                                                          AS activity_sub_type
	       ,activity_date_time                                                 AS activity_start_time
	       ,null                                                               AS activity_end_time
	       ,container_id                                                       AS lpn
	       ,item                                                               AS sku_code
	       ,reference_id                                                       AS order_id
	       ,CASE WHEN direction = 'From' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END AS from_location
	       ,CASE WHEN direction = 'To' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END AS to_location
	       ,quantity                                                           AS qty
	       ,activity_date_time                                                 AS create_time
	       ,cast(internal_container_num AS bigint)                             AS internal_container_num
	       ,cast(internal_id AS bigint)                                        AS internal_id
	       ,internal_key_id                                                    AS activity_id
	       ,date_format(activity_date_time,'HH')                               AS hh
	       ,'ods_cn_hd'                                                        AS data_source
	       ,company                                                            AS wms_company_id
	       ,transaction_type
	       ,null                                                               AS transaction_code
	FROM ods_cn_hd.transaction_history
	WHERE inc_day = '$[time(yyyyMMdd)]' 
	UNION ALL
	SELECT  warehouse                                                          AS wms_warehouse_id
	       ,user_name                                                          AS user_id
	       ,work_group                                                         AS activity_type
	       ,work_type                                                          AS activity_sub_type
	       ,activity_date_time                                                 AS activity_start_time
	       ,null                                                               AS activity_end_time
	       ,container_id                                                       AS lpn
	       ,item                                                               AS sku_code
	       ,reference_id                                                       AS order_id
	       ,CASE WHEN direction = 'From' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END AS from_location
	       ,CASE WHEN direction = 'To' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END AS to_location
	       ,quantity                                                           AS qty
	       ,activity_date_time                                                 AS create_time
	       ,cast(internal_container_num AS bigint)                             AS internal_container_num
	       ,cast(internal_id AS bigint)                                        AS internal_id
	       ,internal_key_id                                                    AS activity_id
	       ,date_format(activity_date_time,'HH')                               AS hh
	       ,'ods_cn_hp_ljb'                                                    AS data_source
	       ,company                                                            AS wms_company_id
	       ,transaction_type
	       ,null                                                               AS transaction_code
	FROM ods_cn_hp_ljb.transaction_history
	WHERE inc_day = '$[time(yyyyMMdd)]' 
	UNION ALL
	SELECT  warehouse                                                          AS wms_warehouse_id
	       ,user_name                                                          AS user_id
	       ,work_group                                                         AS activity_type
	       ,work_type                                                          AS activity_sub_type
	       ,activity_date_time                                                 AS activity_start_time
	       ,null                                                               AS activity_end_time
	       ,container_id                                                       AS lpn
	       ,item                                                               AS sku_code
	       ,reference_id                                                       AS order_id
	       ,CASE WHEN direction = 'From' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END AS from_location
	       ,CASE WHEN direction = 'To' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END AS to_location
	       ,quantity                                                           AS qty
	       ,activity_date_time                                                 AS create_time
	       ,cast(internal_container_num AS bigint)                             AS internal_container_num
	       ,cast(internal_id AS bigint)                                        AS internal_id
	       ,internal_key_id                                                    AS activity_id
	       ,date_format(activity_date_time,'HH')                               AS hh
	       ,'ods_cn_hpi'                                                       AS data_source
	       ,company                                                            AS wms_company_id
	       ,transaction_type
	       ,null                                                               AS transaction_code
	FROM ods_cn_hpi.transaction_history
	WHERE inc_day = '$[time(yyyyMMdd)]' 
	UNION ALL
	SELECT  warehouse                                                          AS wms_warehouse_id
	       ,user_name                                                          AS user_id
	       ,work_group                                                         AS activity_type
	       ,work_type                                                          AS activity_sub_type
	       ,activity_date_time                                                 AS activity_start_time
	       ,null                                                               AS activity_end_time
	       ,container_id                                                       AS lpn
	       ,item                                                               AS sku_code
	       ,reference_id                                                       AS order_id
	       ,CASE WHEN direction = 'From' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END AS from_location
	       ,CASE WHEN direction = 'To' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END AS to_location
	       ,quantity                                                           AS qty
	       ,activity_date_time                                                 AS create_time
	       ,cast(internal_container_num AS bigint)                             AS internal_container_num
	       ,cast(internal_id AS bigint)                                        AS internal_id
	       ,internal_key_id                                                    AS activity_id
	       ,date_format(activity_date_time,'HH')                               AS hh
	       ,'ods_cn_hualiancosta'                                              AS data_source
	       ,company                                                            AS wms_company_id
	       ,transaction_type
	       ,null                                                               AS transaction_code
	FROM ods_cn_hualiancosta.transaction_history
	WHERE inc_day = '$[time(yyyyMMdd)]' 
	UNION ALL
	SELECT  warehouse                                                          AS wms_warehouse_id
	       ,user_name                                                          AS user_id
	       ,work_group                                                         AS activity_type
	       ,work_type                                                          AS activity_sub_type
	       ,activity_date_time                                                 AS activity_start_time
	       ,null                                                               AS activity_end_time
	       ,container_id                                                       AS lpn
	       ,item                                                               AS sku_code
	       ,reference_id                                                       AS order_id
	       ,CASE WHEN direction = 'From' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END AS from_location
	       ,CASE WHEN direction = 'To' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END AS to_location
	       ,quantity                                                           AS qty
	       ,activity_date_time                                                 AS create_time
	       ,cast(internal_container_num AS bigint)                             AS internal_container_num
	       ,cast(internal_id AS bigint)                                        AS internal_id
	       ,internal_key_id                                                    AS activity_id
	       ,date_format(activity_date_time,'HH')                               AS hh
	       ,'ods_cn_jiq'                                                       AS data_source
	       ,company                                                            AS wms_company_id
	       ,transaction_type
	       ,null                                                               AS transaction_code
	FROM ods_cn_jiq.transaction_history
	WHERE inc_day = '$[time(yyyyMMdd)]' 
	UNION ALL
	SELECT  warehouse                                                          AS wms_warehouse_id
	       ,user_name                                                          AS user_id
	       ,work_group                                                         AS activity_type
	       ,work_type                                                          AS activity_sub_type
	       ,activity_date_time                                                 AS activity_start_time
	       ,null                                                               AS activity_end_time
	       ,container_id                                                       AS lpn
	       ,item                                                               AS sku_code
	       ,reference_id                                                       AS order_id
	       ,CASE WHEN direction = 'From' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END AS from_location
	       ,CASE WHEN direction = 'To' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END AS to_location
	       ,quantity                                                           AS qty
	       ,activity_date_time                                                 AS create_time
	       ,cast(internal_container_num AS bigint)                             AS internal_container_num
	       ,cast(internal_id AS bigint)                                        AS internal_id
	       ,internal_key_id                                                    AS activity_id
	       ,date_format(activity_date_time,'HH')                               AS hh
	       ,'ods_cn_kone'                                                      AS data_source
	       ,company                                                            AS wms_company_id
	       ,transaction_type
	       ,null                                                               AS transaction_code
	FROM ods_cn_kone.transaction_history
	WHERE inc_day = '$[time(yyyyMMdd)]' 
	UNION ALL
	SELECT  warehouse                                                          AS wms_warehouse_id
	       ,user_name                                                          AS user_id
	       ,work_group                                                         AS activity_type
	       ,work_type                                                          AS activity_sub_type
	       ,activity_date_time                                                 AS activity_start_time
	       ,null                                                               AS activity_end_time
	       ,container_id                                                       AS lpn
	       ,item                                                               AS sku_code
	       ,reference_id                                                       AS order_id
	       ,CASE WHEN direction = 'From' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END AS from_location
	       ,CASE WHEN direction = 'To' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END AS to_location
	       ,quantity                                                           AS qty
	       ,activity_date_time                                                 AS create_time
	       ,cast(internal_container_num AS bigint)                             AS internal_container_num
	       ,cast(internal_id AS bigint)                                        AS internal_id
	       ,internal_key_id                                                    AS activity_id
	       ,date_format(activity_date_time,'HH')                               AS hh
	       ,'ods_cn_razer'                                                     AS data_source
	       ,company                                                            AS wms_company_id
	       ,transaction_type
	       ,null                                                               AS transaction_code
	FROM ods_cn_razer.transaction_history
	WHERE inc_day = '$[time(yyyyMMdd)]' 
	UNION ALL
	SELECT  warehouse                                                          AS wms_warehouse_id
	       ,user_name                                                          AS user_id
	       ,work_group                                                         AS activity_type
	       ,work_type                                                          AS activity_sub_type
	       ,activity_date_time                                                 AS activity_start_time
	       ,null                                                               AS activity_end_time
	       ,container_id                                                       AS lpn
	       ,item                                                               AS sku_code
	       ,reference_id                                                       AS order_id
	       ,CASE WHEN direction = 'From' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END AS from_location
	       ,CASE WHEN direction = 'To' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END AS to_location
	       ,quantity                                                           AS qty
	       ,activity_date_time                                                 AS create_time
	       ,cast(internal_container_num AS bigint)                             AS internal_container_num
	       ,cast(internal_id AS bigint)                                        AS internal_id
	       ,internal_key_id                                                    AS activity_id
	       ,date_format(activity_date_time,'HH')                               AS hh
	       ,'ods_cn_squibb'                                                    AS data_source
	       ,company                                                            AS wms_company_id
	       ,transaction_type
	       ,null                                                               AS transaction_code
	FROM ods_cn_squibb.transaction_history
	WHERE inc_day = '$[time(yyyyMMdd)]' 
	UNION ALL
	SELECT  warehouse                                                          AS wms_warehouse_id
	       ,user_name                                                          AS user_id
	       ,work_group                                                         AS activity_type
	       ,work_type                                                          AS activity_sub_type
	       ,activity_date_time                                                 AS activity_start_time
	       ,null                                                               AS activity_end_time
	       ,container_id                                                       AS lpn
	       ,item                                                               AS sku_code
	       ,reference_id                                                       AS order_id
	       ,CASE WHEN direction = 'From' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END AS from_location
	       ,CASE WHEN direction = 'To' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END AS to_location
	       ,quantity                                                           AS qty
	       ,activity_date_time                                                 AS create_time
	       ,cast(internal_container_num AS bigint)                             AS internal_container_num
	       ,cast(internal_id AS bigint)                                        AS internal_id
	       ,internal_key_id                                                    AS activity_id
	       ,date_format(activity_date_time,'HH')                               AS hh
	       ,'ods_cn_vzug'                                                      AS data_source
	       ,company                                                            AS wms_company_id
	       ,transaction_type
	       ,null                                                               AS transaction_code
	FROM ods_cn_vzug.transaction_history
	WHERE inc_day = '$[time(yyyyMMdd)]' 
	UNION ALL
	SELECT  warehouse                                                          AS wms_warehouse_id
	       ,user_name                                                          AS user_id
	       ,work_group                                                         AS activity_type
	       ,work_type                                                          AS activity_sub_type
	       ,activity_date_time                                                 AS activity_start_time
	       ,null                                                               AS activity_end_time
	       ,container_id                                                       AS lpn
	       ,item                                                               AS sku_code
	       ,reference_id                                                       AS order_id
	       ,CASE WHEN direction = 'From' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END AS from_location
	       ,CASE WHEN direction = 'To' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END AS to_location
	       ,quantity                                                           AS qty
	       ,activity_date_time                                                 AS create_time
	       ,cast(internal_container_num AS bigint)                             AS internal_container_num
	       ,cast(internal_id AS bigint)                                        AS internal_id
	       ,internal_key_id                                                    AS activity_id
	       ,date_format(activity_date_time,'HH')                               AS hh
	       ,'ods_cn_zebra'                                                     AS data_source
	       ,company                                                            AS wms_company_id
	       ,transaction_type
	       ,null                                                               AS transaction_code
	FROM ods_cn_zebra.transaction_history
	WHERE inc_day = '$[time(yyyyMMdd)]' 
	UNION ALL
	SELECT  warehouse                                                          AS wms_warehouse_id
	       ,user_name                                                          AS user_id
	       ,work_group                                                         AS activity_type
	       ,work_type                                                          AS activity_sub_type
	       ,activity_date_time                                                 AS activity_start_time
	       ,null                                                               AS activity_end_time
	       ,container_id                                                       AS lpn
	       ,item                                                               AS sku_code
	       ,reference_id                                                       AS order_id
	       ,CASE WHEN direction = 'From' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END AS from_location
	       ,CASE WHEN direction = 'To' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END AS to_location
	       ,quantity                                                           AS qty
	       ,activity_date_time                                                 AS create_time
	       ,cast(internal_container_num AS bigint)                             AS internal_container_num
	       ,cast(internal_id AS bigint)                                        AS internal_id
	       ,internal_key_id                                                    AS activity_id
	       ,date_format(activity_date_time,'HH')                               AS hh
	       ,'ods_dbo'                                                          AS data_source
	       ,company                                                            AS wms_company_id
	       ,transaction_type
	       ,null                                                               AS transaction_code
	FROM ods_dbo.transaction_history
	WHERE inc_day = '$[time(yyyyMMdd)]' 
	UNION ALL
	SELECT  warehouse                                                          AS wms_warehouse_id
	       ,user_name                                                          AS user_id
	       ,work_group                                                         AS activity_type
	       ,work_type                                                          AS activity_sub_type
	       ,activity_date_time                                                 AS activity_start_time
	       ,null                                                               AS activity_end_time
	       ,container_id                                                       AS lpn
	       ,item                                                               AS sku_code
	       ,reference_id                                                       AS order_id
	       ,CASE WHEN direction = 'From' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END AS from_location
	       ,CASE WHEN direction = 'To' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END AS to_location
	       ,quantity                                                           AS qty
	       ,activity_date_time                                                 AS create_time
	       ,cast(internal_container_num AS bigint)                             AS internal_container_num
	       ,cast(internal_id AS bigint)                                        AS internal_id
	       ,internal_key_id                                                    AS activity_id
	       ,date_format(activity_date_time,'HH')                               AS hh
	       ,'ods_hk_abbott'                                                    AS data_source
	       ,company                                                            AS wms_company_id
	       ,transaction_type
	       ,null                                                               AS transaction_code
	FROM ods_hk_abbott.transaction_history
	WHERE inc_day = '$[time(yyyyMMdd)]' 
	UNION ALL
	SELECT  warehouse                                                          AS wms_warehouse_id
	       ,user_name                                                          AS user_id
	       ,work_group                                                         AS activity_type
	       ,work_type                                                          AS activity_sub_type
	       ,activity_date_time                                                 AS activity_start_time
	       ,null                                                               AS activity_end_time
	       ,container_id                                                       AS lpn
	       ,item                                                               AS sku_code
	       ,reference_id                                                       AS order_id
	       ,CASE WHEN direction = 'From' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END AS from_location
	       ,CASE WHEN direction = 'To' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END AS to_location
	       ,quantity                                                           AS qty
	       ,activity_date_time                                                 AS create_time
	       ,cast(internal_container_num AS bigint)                             AS internal_container_num
	       ,cast(internal_id AS bigint)                                        AS internal_id
	       ,internal_key_id                                                    AS activity_id
	       ,date_format(activity_date_time,'HH')                               AS hh
	       ,'ods_hk_revlon'                                                    AS data_source
	       ,company                                                            AS wms_company_id
	       ,transaction_type
	       ,null                                                               AS transaction_code
	FROM ods_hk_revlon.transaction_history
	WHERE inc_day = '$[time(yyyyMMdd)]' 
	UNION ALL
	SELECT  warehouse                                                          AS wms_warehouse_id
	       ,user_name                                                          AS user_id
	       ,work_group                                                         AS activity_type
	       ,work_type                                                          AS activity_sub_type
	       ,activity_date_time                                                 AS activity_start_time
	       ,null                                                               AS activity_end_time
	       ,container_id                                                       AS lpn
	       ,item                                                               AS sku_code
	       ,reference_id                                                       AS order_id
	       ,CASE WHEN direction = 'From' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END AS from_location
	       ,CASE WHEN direction = 'To' THEN location
	             WHEN coalesce(direction,'') = '' THEN location  ELSE null END AS to_location
	       ,quantity                                                           AS qty
	       ,activity_date_time                                                 AS create_time
	       ,cast(internal_container_num AS bigint)                             AS internal_container_num
	       ,cast(internal_id AS bigint)                                        AS internal_id
	       ,internal_key_id                                                    AS activity_id
	       ,date_format(activity_date_time,'HH')                               AS hh
	       ,'ods_hk_fredperry'                                                 AS data_source
	       ,company                                                            AS wms_company_id
	       ,transaction_type
	       ,null                                                               AS transaction_code
	FROM ods_hk_fredperry.transaction_history
	WHERE inc_day = '$[time(yyyyMMdd)]' 
) AS a
LEFT JOIN dsc_dim.dim_dsc_ou_whse_rel rel
ON lower(a.wms_warehouse_id) = lower(rel.wms_warehouse_id) AND lower(coalesce(a.wms_company_id, '')) = lower(coalesce(rel.company_id , ''))
LEFT JOIN ods_cn_bose.generic_config_detail gcd
ON gcd.record_type='HIST TR TY' AND a.transaction_type = gcd.identifier
-- Inventory status change --Inventory Status Change
-- Inventory adjustment --Inventory Management
-- Check IN -- Receiving
-- Inventory transfer -- Inventory Movement