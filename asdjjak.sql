select
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
    qty,
    create_time,
    cast(internal_container_num as bigint),
    internal_id,
    activity_id,
    hh,
    data_source,
    wms_company_id,
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
    end as standard_activity_type
from
    (
        select
            case
                when substr(work_zone, 1, 2) = 'OE' then 'OE'
                when substr(work_zone, 1, 2) = 'RT' then 'RT'
                when substr(work_zone, 1, 2) = 'la' then 'OE'
                else 'UNKNOWN'
            end as wms_warehouse_id,
            user_name as user_id,
            work_group as activity_type,
            work_type as activity_sub_type,
            from_unixtime(
                cast(
                    unix_timestamp(activity_date_time) + 28800 as bigint
                ),
                'yyyy-MM-dd HH:mm:ss'
            ) as activity_start_time,
            null as activity_end_time,
            container_id as lpn,
            item as sku_code,
            reference_id as order_id,
            location as from_location,
            null as to_location,
            quantity as qty,
            from_unixtime(
                cast(
                    unix_timestamp(activity_date_time) + 28800 as bigint
                ),
                'yyyy-MM-dd HH:mm:ss'
            ) as create_time,
            cast(internal_container_num as bigint) as internal_container_num,
            cast(internal_id as bigint) as internal_id,
            internal_key_id as activity_id,
            date_format(
                from_unixtime(
                    cast(
                        unix_timestamp(activity_date_time) + 28800 as bigint
                    ),
                    'yyyy-MM-dd HH:mm:ss'
                ),
                'HH'
            ) as hh,
            'ods_cn_michelin' as data_source,
            company as wms_company_id,
            transaction_type,
            null as transaction_code
        from
            ods_cn_michelin.transaction_history
        where
            inc_day = '$[time(yyyyMMdd)]'