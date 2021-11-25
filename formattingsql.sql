SELECT  config.config_group AS Data_Type
       ,ou.bg_id            AS BG
       ,ou.ou_code          AS OU_Code
       ,ou.ou_name          AS OU_Name
       ,ou.wms_system       AS Source_System
       ,o.last_update_date  AS ODS_Last_Update_Date
       ,f.last_update_date  AS DWD_Last_Update_Date
       ,CASE WHEN (regexp_replace(f.last_update_date,'-','') < regexp_replace(o.last_update_date,'-','') AND regexp_replace(f.last_update_date,'-','')<date_format(date_add('Day',-1,current_date),'%Y%m%d')) or f.last_update_date is null THEN 1  ELSE 0 END AS warning_flag
FROM dsc_dim.dim_dsc_ou_info ou
CROSS JOIN
(
	SELECT  'inbound_header' AS config_group
	UNION ALL
	SELECT  'inbound_line' AS config_group
	UNION ALL
	SELECT  'outbound_header' AS config_group
	UNION ALL
	SELECT  'outbound_line' AS config_group
	UNION ALL
	SELECT  'inventory' AS config_group
	UNION ALL
	SELECT  'working_hours' AS config_group
) AS config
LEFT JOIN
(
	SELECT  config_group
	       ,pk_values AS ou_code
	       ,MAX(case WHEN substr(tgt_result,5,1) <> '-' THEN concat(substr(tgt_result,1,4),'-',substr(tgt_result,5,2),'-',substr(tgt_result,7,2)) else substr(tgt_result,1,10) end) AS last_update_date
	FROM dm_dsc_ads.dm_dsc_dqc_result_log_di
	WHERE check_date = date_format(current_date, '%Y%m%d')
	AND measure_name = 'max_date'
	AND pk_columns = 'ou_code'
	AND dw_layer ='ods'
	GROUP BY  config_group
	         ,pk_values
) o
ON ou.ou_code = o.ou_code AND config.config_group = o.config_group
LEFT JOIN
(
	SELECT  config_group
	       ,pk_values AS ou_code
	       ,MAX(case WHEN substr(tgt_result,5,1) <> '-' THEN concat(substr(tgt_result,1,4),'-',substr(tgt_result,5,2),'-',substr(tgt_result,7,2)) else substr(tgt_result,1,10) end) AS last_update_date
	FROM dm_dsc_ads.dm_dsc_dqc_result_log_di
	WHERE check_date = date_format(current_date, '%Y%m%d')
	AND measure_name = 'max_date'
	AND pk_columns = 'ou_code'
	AND dw_layer ='dwd'
	GROUP BY  config_group
	         ,pk_values
) f
ON ou.ou_code = f.ou_code AND config.config_group = f.config_group
WHERE ou.wms_system != ''
AND o.last_update_date != ''
ORDER BY Data_Type, OU_Code