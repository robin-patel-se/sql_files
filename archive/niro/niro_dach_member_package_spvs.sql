-- Niro has raised there is a spike in June 2021 data for DACH member Package SPVs

USE WAREHOUSE pipe_xlarge;

--member spvs by date for June
SELECT sts.event_tstamp::DATE          AS date,
       DAYNAME(sts.event_tstamp::DATE) AS day_name,
       COUNT(*)                        AS spvs
FROM se.data.scv_touched_spvs sts
    INNER JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id AND stba.stitched_identity_type = 'se_user_id'
WHERE DATE_TRUNC(MONTH, sts.event_tstamp) = '2021-06-01'
GROUP BY 1, 2
;

--DACH member spvs by date for June, split by product config
SELECT sts.event_tstamp::DATE                                 AS date,
       COUNT(*)                                               AS spvs,
       COUNT_IF(ds.product_configuration = 'Hotel Plus')      AS hotelplus,
       COUNT_IF(ds.product_configuration = 'IHP - static')    AS ihpstatic,
       COUNT_IF(ds.product_configuration = 'WRD')             AS wrd,
       COUNT_IF(ds.product_configuration = '3PP')             AS thirdpartypackage,
       COUNT_IF(ds.product_configuration = 'N/A')             AS na,
       COUNT_IF(ds.product_configuration = 'Catalogue')       AS catalogue,
       COUNT_IF(ds.product_configuration = 'IHP - dynamic')   AS ihpdynamic,
       COUNT_IF(ds.product_configuration = 'Hotel')           AS hotel,
       COUNT_IF(ds.product_configuration = 'IHP - connected') AS ihpconnected,
       COUNT_IF(ds.product_configuration = 'WRD - direct')    AS wrddirect
FROM se.data.scv_touched_spvs sts
    INNER JOIN se.data_pii.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id AND stba.stitched_identity_type = 'se_user_id'
    INNER JOIN se.data.se_user_attributes sua ON stba.attributed_user_id = sua.shiro_user_id::VARCHAR AND se.data.posa_category_from_territory(sua.current_affiliate_territory) = 'DACH'
    INNER JOIN se.data.dim_sale ds ON sts.se_sale_id = ds.se_sale_id
WHERE DATE_TRUNC(MONTH, sts.event_tstamp) = '2021-06-01'
GROUP BY 1
;
--clear influx in 3PP spvs on the 16th June


--DACH member spvs by date for 16th June for 3pp sales
SELECT sts.*,
       stba.*
FROM se.data.scv_touched_spvs sts
    INNER JOIN se.data_pii.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id AND stba.stitched_identity_type = 'se_user_id'
    INNER JOIN se.data.se_user_attributes sua ON stba.attributed_user_id = sua.shiro_user_id::VARCHAR AND se.data.posa_category_from_territory(sua.current_affiliate_territory) = 'DACH'
    INNER JOIN se.data.dim_sale ds ON sts.se_sale_id = ds.se_sale_id
WHERE sts.event_tstamp::DATE = '2021-06-16'
  AND ds.product_configuration = '3PP'
;

SELECT *
FROM se.data_pii.se_user_attributes sua
WHERE sua.shiro_user_id = '75053759'


--DACH member spvs by date for June, split by product config for shiro user id 75053759
SELECT sts.event_tstamp::DATE                                 AS date,
       COUNT(*)                                               AS spvs,
       COUNT_IF(ds.product_configuration = 'Hotel Plus')      AS hotelplus,
       COUNT_IF(ds.product_configuration = 'IHP - static')    AS ihpstatic,
       COUNT_IF(ds.product_configuration = 'WRD')             AS wrd,
       COUNT_IF(ds.product_configuration = '3PP')             AS thirdpartypackage,
       COUNT_IF(ds.product_configuration = 'N/A')             AS na,
       COUNT_IF(ds.product_configuration = 'Catalogue')       AS catalogue,
       COUNT_IF(ds.product_configuration = 'IHP - dynamic')   AS ihpdynamic,
       COUNT_IF(ds.product_configuration = 'Hotel')           AS hotel,
       COUNT_IF(ds.product_configuration = 'IHP - connected') AS ihpconnected,
       COUNT_IF(ds.product_configuration = 'WRD - direct')    AS wrddirect
FROM se.data.scv_touched_spvs sts
    INNER JOIN se.data_pii.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id AND stba.stitched_identity_type = 'se_user_id'
    INNER JOIN se.data.se_user_attributes sua ON stba.attributed_user_id = sua.shiro_user_id::VARCHAR AND se.data.posa_category_from_territory(sua.current_affiliate_territory) = 'DACH'
    INNER JOIN se.data.dim_sale ds ON sts.se_sale_id = ds.se_sale_id
WHERE sts.event_tstamp >= '2021-06-01'
  AND sua.shiro_user_id = '75053759'
GROUP BY 1
;


SELECT sts.event_tstamp::DATE,
       sts.created_at::DATE,
       sts.updated_at::DATE,
       COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs sts
    INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes stba ON sts.touch_id = stba.touch_id AND stba.stitched_identity_type = 'se_user_id'
    INNER JOIN se.data.se_user_attributes sua ON stba.attributed_user_id = sua.shiro_user_id::VARCHAR AND se.data.posa_category_from_territory(sua.current_affiliate_territory) = 'DACH'
    INNER JOIN se.data.dim_sale ds ON sts.se_sale_id = ds.se_sale_id
WHERE sts.event_tstamp >= '2021-06-01'
  AND sua.shiro_user_id = '75053759'
GROUP BY 1, 2, 3
;

SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touchification mt
WHERE mt.touch_id = 'c49f2ac2a1150c69935011292851ba72693a819006998c96c24caf86fe0b412c';

SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_identity_stitching mis
WHERE mis.attributed_user_id = '75053759';

SELECT sts.search_context,
       sts.trip_types,
       sts.search_context:trip_types::ARRAY
FROM se.data.scv_touched_searches sts
WHERE sts.event_hash = 'f3c2d82b74e49af2d40a102200953c773a3436205661a178a8a0891aef48454c';