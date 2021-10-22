SET tez.queue.name=root.dsc;
SET hive.execution.engine=tez;

INSERT OVERWRITE TABLE dsc_dwd.dwd_dsc_wh_activity_di 
partition (inc_day='$[time(yyyyMMdd)]', src='scale')

 SELECT
 wms_warehouse_id,
 user_id,
 activity_type,
 activity_sub_type,
 activity_start_time,
 activity_end_time,
 lpn,
 sku_code,
 order_id,
 from_location,
 to_location,
 sum(qty) as qty,
 create_time,
 internal_container_num,
 max(internal_id) internal_id,   
 max(activity_id) activity_id,
 hh,
 data_source,
 wms_company_id,
 transaction_type,
 transaction_code,
 standard_activity_type,
 ou_code,
 nbr_of_cases,
 inc_day,
 src
 



FROM  
(
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
 cast(internal_container_num as bigint) as internal_container_num 
,
 internal_id,
 activity_id,
 cast(hh + 8 as bigint) as hh,
 a.data_source,
 a.wms_company_id,
 gcd.description as transaction_type,
 transaction_code,
case 
    when activity_type is null
    then 
        case 
            when substr(activity_sub_type, 1,4) = 'Ship'
            then 'Shipping'
            when activity_sub_type = 'Receipt'
            then 'Receiving'
        		else 
            		case 
                		when gcd.description = 'Inventory status change'
                		then 'Inventory status change'
                		when gcd.description = 'Inventory adjustment'
                		then 'Inventory Management'
                		when gcd.description = 'Check In'
                		then 'Receiving '
                        when gcd.description = 'Inventory transfer'
                        then 'Inventory Movement'
                		else 'Misc'
            		end
       		 end
    else activity_type  
 end as
 standard_activity_type
 , rel.ou as ou_code 
 , null as nbr_of_cases

 , date_format(from_unixtime(
                cast(
                    unix_timestamp(activity_start_time) + 28800 as bigint
                ),
                'yyyy-MM-dd HH:mm:ss'
            ), 'yyyyMMdd') as inc_day
 , 'scale' as src
from 
 (select 
 case when substr(work_zone, 1,2) = 'OE'
 then 'OE' 
 when substr(work_zone, 1,2) = 'RT'
 then 'RT'
  when substr(work_zone, 1,2) = 'la'
 then 'OE' 
 else 'UNKNOWN'
 end as wms_warehouse_id 
 , user_name as user_id ,work_group as activity_type , work_type as activity_sub_type , activity_date_time as activity_start_time ,null as activity_end_time , container_id as lpn ,item as sku_code ,reference_id as order_id 
 , case when direction = 'From'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as from_location 
 , case when direction = 'To'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as to_location   
 , quantity as qty , activity_date_time as create_time ,cast(internal_container_num as bigint) as internal_container_num 
  , cast(internal_id as bigint) as  internal_id  ,internal_key_id as activity_id  , date_format(activity_date_time,'HH') as hh
 , 'scale_michelin' as data_source
 , company as wms_company_id, transaction_type , null as transaction_code
 
from ods_cn_michelin.transaction_history
  where inc_day between '20210908' and '20211021'  
 union all
 select warehouse as wms_warehouse_id , user_name as user_id ,work_group as activity_type ,work_type as activity_sub_type , activity_date_time as activity_start_time ,null as activity_end_time , container_id as lpn ,item as sku_code ,reference_id as order_id, 
  case when direction = 'From'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as from_location 
 , case when direction = 'To'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as to_location,  quantity as qty , activity_date_time as create_time ,cast(internal_container_num as bigint) as internal_container_num 
  , cast(internal_id as bigint) as  internal_id  ,internal_key_id as activity_id  , date_format(activity_date_time,'HH') as hh, 'scale_bose' as data_source, company as wms_company_id, transaction_type , null as transaction_code 
from ods_cn_bose.transaction_history   where inc_day between '20210908' and '20211021'  union all
 select warehouse as wms_warehouse_id , user_name as user_id ,work_group as activity_type ,work_type as activity_sub_type , activity_date_time as activity_start_time ,null as activity_end_time , container_id as lpn ,item as sku_code ,reference_id as order_id, 
  case when direction = 'From'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as from_location 
 , case when direction = 'To'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as to_location,  quantity as qty , activity_date_time as create_time ,cast(internal_container_num as bigint) as internal_container_num 
  , cast(internal_id as bigint) as  internal_id  ,internal_key_id as activity_id  , date_format(activity_date_time,'HH') as hh, 'scale_apple_sz' as data_source, company as wms_company_id, transaction_type , null as transaction_code 
 from ods_cn_apple_sz.transaction_history   where inc_day between '20210908' and '20211021'  union all
 select warehouse as wms_warehouse_id , user_name as user_id ,work_group as activity_type ,work_type as activity_sub_type , activity_date_time as activity_start_time ,null as activity_end_time , container_id as lpn ,item as sku_code ,reference_id as order_id, 
  case when direction = 'From'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as from_location 
 , case when direction = 'To'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as to_location,  quantity as qty , activity_date_time as create_time ,cast(internal_container_num as bigint) as internal_container_num 
  , cast(internal_id as bigint) as  internal_id  ,internal_key_id as activity_id  , date_format(activity_date_time,'HH') as hh, 'scale_apple_sh' as data_source, company as wms_company_id, transaction_type , null as transaction_code 
from ods_cn_apple_sh.transaction_history   where inc_day between '20210908' and '20211021'  union all
 select warehouse as wms_warehouse_id , user_name as user_id ,work_group as activity_type ,work_type as activity_sub_type , activity_date_time as activity_start_time ,null as activity_end_time , container_id as lpn ,item as sku_code ,reference_id as order_id, 
  case when direction = 'From'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as from_location 
 , case when direction = 'To'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as to_location,  quantity as qty , activity_date_time as create_time ,cast(internal_container_num as bigint) as internal_container_num 
  , cast(internal_id as bigint) as  internal_id  ,internal_key_id as activity_id  , date_format(activity_date_time,'HH') as hh, 'scale_costacoffee' as data_source, company as wms_company_id, transaction_type , null as transaction_code 
from ods_cn_costacoffee.transaction_history   where inc_day between '20210908' and '20211021'  union all
 select warehouse as wms_warehouse_id , user_name as user_id ,work_group as activity_type ,work_type as activity_sub_type , activity_date_time as activity_start_time ,null as activity_end_time , container_id as lpn ,item as sku_code ,reference_id as order_id, 
  case when direction = 'From'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as from_location 
 , case when direction = 'To'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as to_location,  quantity as qty , activity_date_time as create_time ,cast(internal_container_num as bigint) as internal_container_num 
  , cast(internal_id as bigint) as  internal_id  ,internal_key_id as activity_id  , date_format(activity_date_time,'HH') as hh, 'scale_diadora' as data_source, company as wms_company_id, transaction_type , null as transaction_code 
from ods_cn_diadora.transaction_history   where inc_day between '20210908' and '20211021'  union all
 select warehouse as wms_warehouse_id , user_name as user_id ,work_group as activity_type ,work_type as activity_sub_type , activity_date_time as activity_start_time ,null as activity_end_time , container_id as lpn ,item as sku_code ,reference_id as order_id, 
  case when direction = 'From'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as from_location 
 , case when direction = 'To'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as to_location,  quantity as qty , activity_date_time as create_time ,cast(internal_container_num as bigint) as internal_container_num 
  , cast(internal_id as bigint) as  internal_id  ,internal_key_id as activity_id  , date_format(activity_date_time,'HH') as hh, 'scale_ferrero' as data_source, company as wms_company_id, transaction_type , null as transaction_code 
from ods_cn_ferrero.transaction_history   where inc_day between '20210908' and '20211021'  union all
 select warehouse as wms_warehouse_id , user_name as user_id ,work_group as activity_type ,work_type as activity_sub_type , activity_date_time as activity_start_time ,null as activity_end_time , container_id as lpn ,item as sku_code ,reference_id as order_id, 
  case when direction = 'From'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as from_location 
 , case when direction = 'To'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as to_location,  quantity as qty , activity_date_time as create_time ,cast(internal_container_num as bigint) as internal_container_num 
  , cast(internal_id as bigint) as  internal_id  ,internal_key_id as activity_id  , date_format(activity_date_time,'HH') as hh, 'scale_fuji' as data_source, company as wms_company_id, transaction_type , null as transaction_code 
from ods_cn_fuji.transaction_history   where inc_day between '20210908' and '20211021'  union all
 select warehouse as wms_warehouse_id , user_name as user_id ,work_group as activity_type ,work_type as activity_sub_type , activity_date_time as activity_start_time ,null as activity_end_time , container_id as lpn ,item as sku_code ,reference_id as order_id, 
  case when direction = 'From'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as from_location 
 , case when direction = 'To'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as to_location,  quantity as qty , activity_date_time as create_time ,cast(internal_container_num as bigint) as internal_container_num 
  , cast(internal_id as bigint) as  internal_id  ,internal_key_id as activity_id  , date_format(activity_date_time,'HH') as hh, 'scale_hd' as data_source, company as wms_company_id, transaction_type , null as transaction_code 
from ods_cn_hd.transaction_history   where inc_day between '20210908' and '20211021'  union all
 select warehouse as wms_warehouse_id , user_name as user_id ,work_group as activity_type ,work_type as activity_sub_type , activity_date_time as activity_start_time ,null as activity_end_time , container_id as lpn ,item as sku_code ,reference_id as order_id, 
  case when direction = 'From'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as from_location 
 , case when direction = 'To'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as to_location,  quantity as qty , activity_date_time as create_time ,cast(internal_container_num as bigint) as internal_container_num 
  , cast(internal_id as bigint) as  internal_id  ,internal_key_id as activity_id  , date_format(activity_date_time,'HH') as hh, 'scale_hp_ljb' as data_source, company as wms_company_id, transaction_type , null as transaction_code 
from ods_cn_hp_ljb.transaction_history   where inc_day between '20210908' and '20211021'  union all
 select warehouse as wms_warehouse_id , user_name as user_id ,work_group as activity_type ,work_type as activity_sub_type , activity_date_time as activity_start_time ,null as activity_end_time , container_id as lpn ,item as sku_code ,reference_id as order_id, 
  case when direction = 'From'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as from_location 
 , case when direction = 'To'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as to_location,  quantity as qty , activity_date_time as create_time ,cast(internal_container_num as bigint) as internal_container_num 
  , cast(internal_id as bigint) as  internal_id  ,internal_key_id as activity_id  , date_format(activity_date_time,'HH') as hh, 'scale_hpi' as data_source, company as wms_company_id, transaction_type , null as transaction_code 
from ods_cn_hpi.transaction_history   where inc_day between '20210908' and '20211021'  union all
 select warehouse as wms_warehouse_id , user_name as user_id ,work_group as activity_type ,work_type as activity_sub_type , activity_date_time as activity_start_time ,null as activity_end_time , container_id as lpn ,item as sku_code ,reference_id as order_id, 
  case when direction = 'From'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as from_location 
 , case when direction = 'To'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as to_location,  quantity as qty , activity_date_time as create_time ,cast(internal_container_num as bigint) as internal_container_num 
  , cast(internal_id as bigint) as  internal_id  ,internal_key_id as activity_id  , date_format(activity_date_time,'HH') as hh, 'scale_hualiancosta' as data_source, company as wms_company_id, transaction_type , null as transaction_code 
from ods_cn_hualiancosta.transaction_history   where inc_day between '20210908' and '20211021'  union all
 select warehouse as wms_warehouse_id , user_name as user_id ,work_group as activity_type ,work_type as activity_sub_type , activity_date_time as activity_start_time ,null as activity_end_time , container_id as lpn ,item as sku_code ,reference_id as order_id, 
  case when direction = 'From'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as from_location 
 , case when direction = 'To'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as to_location,  quantity as qty , activity_date_time as create_time ,cast(internal_container_num as bigint) as internal_container_num 
  , cast(internal_id as bigint) as  internal_id  ,internal_key_id as activity_id  , date_format(activity_date_time,'HH') as hh, 'scale_jiq' as data_source, company as wms_company_id, transaction_type , null as transaction_code 
from ods_cn_jiq.transaction_history   where inc_day between '20210908' and '20211021'  union all
 select warehouse as wms_warehouse_id , user_name as user_id ,work_group as activity_type ,work_type as activity_sub_type , activity_date_time as activity_start_time ,null as activity_end_time , container_id as lpn ,item as sku_code ,reference_id as order_id, 
  case when direction = 'From'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as from_location 
 , case when direction = 'To'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as to_location,  quantity as qty , activity_date_time as create_time ,cast(internal_container_num as bigint) as internal_container_num 
  , cast(internal_id as bigint) as  internal_id  ,internal_key_id as activity_id  , date_format(activity_date_time,'HH') as hh, 'scale_kone' as data_source, company as wms_company_id, transaction_type , null as transaction_code 
from ods_cn_kone.transaction_history   where inc_day between '20210908' and '20211021'  union all
 select warehouse as wms_warehouse_id , user_name as user_id ,work_group as activity_type ,work_type as activity_sub_type , activity_date_time as activity_start_time ,null as activity_end_time , container_id as lpn ,item as sku_code ,reference_id as order_id, 
  case when direction = 'From'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as from_location 
 , case when direction = 'To'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as to_location,  quantity as qty , activity_date_time as create_time ,cast(internal_container_num as bigint) as internal_container_num 
  , cast(internal_id as bigint) as  internal_id  ,internal_key_id as activity_id  , date_format(activity_date_time,'HH') as hh, 'scale_razer' as data_source, company as wms_company_id, transaction_type , null as transaction_code 
from ods_cn_razer.transaction_history   where inc_day between '20210908' and '20211021'  union all
 select warehouse as wms_warehouse_id , user_name as user_id ,work_group as activity_type ,work_type as activity_sub_type , activity_date_time as activity_start_time ,null as activity_end_time , container_id as lpn ,item as sku_code ,reference_id as order_id, 
  case when direction = 'From'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as from_location 
 , case when direction = 'To'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as to_location,  quantity as qty , activity_date_time as create_time ,cast(internal_container_num as bigint) as internal_container_num 
  , cast(internal_id as bigint) as  internal_id  ,internal_key_id as activity_id  , date_format(activity_date_time,'HH') as hh, 'scale_squibb' as data_source, company as wms_company_id, transaction_type , null as transaction_code 
from ods_cn_squibb.transaction_history   where inc_day between '20210908' and '20211021'  union all
 select warehouse as wms_warehouse_id , user_name as user_id ,work_group as activity_type ,work_type as activity_sub_type , activity_date_time as activity_start_time ,null as activity_end_time , container_id as lpn ,item as sku_code ,reference_id as order_id, 
  case when direction = 'From'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as from_location 
 , case when direction = 'To'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as to_location,  quantity as qty , activity_date_time as create_time ,cast(internal_container_num as bigint) as internal_container_num 
  , cast(internal_id as bigint) as  internal_id  ,internal_key_id as activity_id  , date_format(activity_date_time,'HH') as hh, 'scale_vzug' as data_source, company as wms_company_id, transaction_type , null as transaction_code 
from ods_cn_vzug.transaction_history   where inc_day between '20210908' and '20211021'  union all
 select warehouse as wms_warehouse_id , user_name as user_id ,work_group as activity_type ,work_type as activity_sub_type , activity_date_time as activity_start_time ,null as activity_end_time , container_id as lpn ,item as sku_code ,reference_id as order_id, 
  case when direction = 'From'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as from_location 
 , case when direction = 'To'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as to_location,  quantity as qty , activity_date_time as create_time ,cast(internal_container_num as bigint) as internal_container_num 
  , cast(internal_id as bigint) as  internal_id  ,internal_key_id as activity_id  , date_format(activity_date_time,'HH') as hh, 'scale_zebra' as data_source, company as wms_company_id, transaction_type , null as transaction_code 
from ods_cn_zebra.transaction_history   where inc_day between '20210908' and '20211021'  union all
 select warehouse as wms_warehouse_id , user_name as user_id ,work_group as activity_type ,work_type as activity_sub_type , activity_date_time as activity_start_time ,null as activity_end_time , container_id as lpn ,item as sku_code ,reference_id as order_id, 
  case when direction = 'From'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as from_location 
 , case when direction = 'To'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as to_location,  quantity as qty , activity_date_time as create_time ,cast(internal_container_num as bigint) as internal_container_num 
  , internal_id  as  internal_id  ,internal_key_id as activity_id  , date_format(activity_date_time,'HH') as hh, 'scale_fas' as data_source, company as wms_company_id, transaction_type , null as transaction_code 
from ods_dbo.transaction_history   where inc_day between '20210908' and '20211021'  union all
 select warehouse as wms_warehouse_id , user_name as user_id ,work_group as activity_type ,work_type as activity_sub_type , activity_date_time as activity_start_time ,null as activity_end_time , container_id as lpn ,item as sku_code ,reference_id as order_id, 
  case when direction = 'From'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as from_location 
 , case when direction = 'To'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as to_location,  quantity as qty , activity_date_time as create_time ,cast(internal_container_num as bigint) as internal_container_num 
  , cast(internal_id as bigint) as  internal_id  ,internal_key_id as activity_id  , date_format(activity_date_time,'HH') as hh, 'scale_hk_abbott' as data_source, company as wms_company_id, transaction_type , null as transaction_code 
from ods_hk_abbott.transaction_history   where inc_day between '20210908' and '20211021'  union all
 select warehouse as wms_warehouse_id , user_name as user_id ,work_group as activity_type ,work_type as activity_sub_type , activity_date_time as activity_start_time ,null as activity_end_time , container_id as lpn ,item as sku_code ,reference_id as order_id, 
  case when direction = 'From'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as from_location 
 , case when direction = 'To'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as to_location,  quantity as qty , activity_date_time as create_time ,cast(internal_container_num as bigint) as internal_container_num 
  , cast(internal_id as bigint) as  internal_id  ,internal_key_id as activity_id  , date_format(activity_date_time,'HH') as hh, 'scale_hk_revlon' as data_source, company as wms_company_id, transaction_type , null as transaction_code 
from ods_hk_revlon.transaction_history   where inc_day between '20210908' and '20211021'  union all
 select warehouse as wms_warehouse_id , user_name as user_id ,work_group as activity_type ,work_type as activity_sub_type , activity_date_time as activity_start_time ,null as activity_end_time , container_id as lpn ,item as sku_code ,reference_id as order_id, 
  case when direction = 'From'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as from_location 
 , case when direction = 'To'
   then location 
   when coalesce(direction, '') = ''
   then location
   else null
   end as to_location,  quantity as qty , activity_date_time as create_time ,cast(internal_container_num as bigint) as internal_container_num 
  , cast(internal_id as bigint) as  internal_id  ,internal_key_id as activity_id  , date_format(activity_date_time,'HH') as hh, 'scale_hk_fredperry' as data_source, company as wms_company_id, transaction_type , null as transaction_code 
from ods_hk_fredperry.transaction_history  where inc_day between '20210908' and '20211021'
 )
 as a

 left join
    dsc_dim.dim_dsc_ou_whse_rel rel
    on lower(a.wms_warehouse_id) = lower(rel.wms_warehouse_id)
    and 
    lower(coalesce(a.wms_company_id,'')) = lower(coalesce(rel.company_id ,''))
 left join 
 ods_cn_bose.generic_config_detail  gcd
 on gcd.record_type='HIST TR TY'
 and a.transaction_type = gcd.identifier
 

 ) b

group by
wms_warehouse_id, user_id, activity_type, activity_sub_type,
activity_start_time,activity_end_time,lpn,sku_code,order_id,from_location,to_location,create_time,
internal_container_num, hh,data_source,wms_company_id,transaction_type,transaction_code,
standard_activity_type,ou_code, nbr_of_cases, inc_day,src
 



-- """
-- activity try to merge two lines of same act to one, 
-- it would lost info related to lpn, internal_id and so on 
-- ## this is an example /.
-- """

-- select 
--  wms_warehouse_id,
--  user_id,
--  activity_type,
--  activity_sub_type,
--  activity_start_time,
--  activity_end_time,
--  max(lpn) lpn,
--  sku_code,
--  order_id,
-- max(from_location) from_location,
--  max(to_location) to_location,
--  qty,
--  create_time,
-- internal_container_num,
--  max(internal_id) internal_id,   
--  activity_id,
--  hh,
--  data_source,
--  wms_company_id,
--  transaction_type,
--  transaction_code,
--  standard_activity_type,
--  ou_code,
--  nbr_of_cases
-- from
-- dsc_dwd.dwd_dsc_wh_activity_di 
-- where inc_day = '20211021'
-- and src = 'scale'
-- and activity_start_time = '2021-10-21 09:17:16'
 
-- group by 
-- wms_warehouse_id,
--  user_id,
--  activity_type,
--  activity_sub_type,
--  activity_start_time,
--  activity_end_time,
--  sku_code,
--  order_id,qty,
--  create_time,
--  internal_container_num,
--  activity_id,
--  hh,
--  data_source,
--  wms_company_id,
--  transaction_type,
--  transaction_code,
--  standard_activity_type,
--  ou_code,
--  nbr_of_cases