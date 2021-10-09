) AS a
LEFT JOIN dsc_dim.dim_dsc_ou_whse_rel rel
ON lower(a.wms_warehouse_id) = lower(rel.wms_warehouse_id) AND lower(coalesce(a.wms_company_id, '')) = lower(coalesce(rel.company_id , ''))
LEFT JOIN ods_cn_bose.generic_config_detail gcd
ON gcd.record_type='HIST TR TY' AND a.transaction_type = gcd.identifier