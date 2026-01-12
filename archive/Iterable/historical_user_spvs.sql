WITH spvs AS (
    SELECT mt.attributed_user_id::INT AS shiro_user_id,
           mts.se_sale_id,
           MAX(mt.event_tstamp)       AS last_event_tstamp
    FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs mts
        INNER JOIN data_vault_mvp.single_customer_view_stg.module_touchification mt
                   ON mts.touch_id = mt.touch_id
    WHERE mts.event_tstamp::DATE >= CURRENT_DATE - 8     --to get a weeks worth of data
      AND mt.stitched_identity_type = 'se_user_id'       -- only member spvs
      AND LEFT(mts.se_sale_id, 3) IS DISTINCT FROM 'TVL' --remove travelist spvs
    GROUP BY 1, 2
)

SELECT s.shiro_user_id,
       ARRAY_AGG(IFF(s.last_event_tstamp::DATE = CURRENT_DATE - 1, se_sale_id, NULL)) WITHIN GROUP (ORDER BY s.last_event_tstamp DESC) AS daily_spv_deals,
       ARRAY_AGG(IFF(s.last_event_tstamp::DATE = CURRENT_DATE - 8, se_sale_id, NULL)) WITHIN GROUP (ORDER BY s.last_event_tstamp DESC) AS weekly_spv_deals
FROM spvs s
GROUP BY 1;

WITH spvs AS (
    SELECT mt.attributed_user_id::INT AS shiro_user_id,
           mts.se_sale_id,
           ds.posu_city,
           ds.posu_division,
           ds.posu_country,
           ds.sale_type,
           MAX(mt.event_tstamp)       AS last_event_tstamp,
           OBJECT_CONSTRUCT(
                   'dealId', mts.se_sale_id,
                   'city', LOWER(ds.posu_city),
                   'division', LOWER(ds.posu_division),
                   'county', LOWER(ds.posu_country),
                   'saleType', LOWER(ds.sale_type),
                   'lastEventTstamp', MAX(mt.event_tstamp)
               )                      AS deal_object
    FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs mts
        INNER JOIN data_vault_mvp.single_customer_view_stg.module_touchification mt
                   ON mts.touch_id = mt.touch_id
        INNER JOIN data_vault_mvp.dwh.dim_sale ds ON mts.se_sale_id = ds.se_sale_id
    WHERE mts.event_tstamp::DATE >= CURRENT_DATE - 180   --to get a weeks worth of data
      AND mt.stitched_identity_type = 'se_user_id'       -- only member spvs
      AND LEFT(mts.se_sale_id, 3) IS DISTINCT FROM 'TVL' --remove travelist spvs
      AND mts.se_sale_id IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5, 6
)

SELECT s.shiro_user_id,
       ARRAY_AGG(IFF(s.last_event_tstamp::DATE = CURRENT_DATE - 1, se_sale_id, NULL)) WITHIN GROUP (ORDER BY s.last_event_tstamp DESC)     AS daily_spv_deals,
       ARRAY_AGG(IFF(s.last_event_tstamp::DATE >= CURRENT_DATE - 8, se_sale_id, NULL)) WITHIN GROUP (ORDER BY s.last_event_tstamp DESC)    AS weekly_spv_deals,
       ARRAY_AGG(IFF(s.last_event_tstamp::DATE >= CURRENT_DATE - 30, deal_object, NULL)) WITHIN GROUP (ORDER BY s.last_event_tstamp DESC)  AS sales_pageviews_last_30_days,
       ARRAY_AGG(IFF(s.last_event_tstamp::DATE >= CURRENT_DATE - 60, deal_object, NULL)) WITHIN GROUP (ORDER BY s.last_event_tstamp DESC)  AS sales_pageviews_last_60_days,
       ARRAY_AGG(IFF(s.last_event_tstamp::DATE >= CURRENT_DATE - 180, deal_object, NULL)) WITHIN GROUP (ORDER BY s.last_event_tstamp DESC) AS sales_pageviews_last_180_days
FROM spvs s
GROUP BY 1;



USE WAREHOUSE pipe_2xlarge;

WITH spvs AS (
    SELECT mt.attributed_user_id::INT AS shiro_user_id,
           mts.se_sale_id,
           ds.posu_city,
           ds.posu_division,
           ds.posu_country,
           ds.sale_product,
           MAX(mt.event_tstamp)       AS last_event_tstamp,
           OBJECT_CONSTRUCT(
                   'dealId', mts.se_sale_id,
                   'city', LOWER(ds.posu_city),
                   'division', LOWER(ds.posu_division),
                   'county', LOWER(ds.posu_country),
                   'saleType', LOWER(ds.sale_product),
                   'lastEventTstamp', TO_VARCHAR(MAX(mt.event_tstamp), 'YYYY-MM-DD HH24:MI:SS +00:00')
               )                      AS deal_object
    FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs mts
        INNER JOIN data_vault_mvp.single_customer_view_stg.module_touchification mt
                   ON mts.touch_id = mt.touch_id
        INNER JOIN data_vault_mvp.dwh.dim_sale ds ON mts.se_sale_id = ds.se_sale_id
    WHERE mts.event_tstamp::DATE >= CURRENT_DATE - 60    --to get a weeks worth of data
      AND mt.stitched_identity_type = 'se_user_id'       -- only member spvs
      AND LEFT(mts.se_sale_id, 3) IS DISTINCT FROM 'TVL' --remove travelist spvs
      AND mts.se_sale_id IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5, 6
)

SELECT s.shiro_user_id,
       ARRAY_AGG(IFF(s.last_event_tstamp::DATE = CURRENT_DATE - 1, se_sale_id, NULL)) WITHIN GROUP (ORDER BY s.last_event_tstamp DESC)  AS daily_spv_deals,
       ARRAY_AGG(IFF(s.last_event_tstamp::DATE >= CURRENT_DATE - 8, se_sale_id, NULL)) WITHIN GROUP (ORDER BY s.last_event_tstamp DESC) AS weekly_spv_deals,
       ARRAY_AGG(IFF(s.last_event_tstamp::DATE >= CURRENT_DATE - 30, deal_object, NULL))                                                AS sales_pageviews_last_30_days,
       ARRAY_AGG(deal_object)                                                                                                           AS sales_pageviews_last_60_days
FROM spvs s
GROUP BY 1;


SELECT DISTINCT sale_product
FROM se.data.dim_sale ds;


airflow backfill --start_date '2021-11-26 00:00:00' --end_date '2021-11-27 00:00:00' --reset_dagruns --task_regex '.*' outgoing__iterable__user_profile_activity__daily_at_03h00


------------------------------------------------------------------------------------------------------------------------


self_describing_task --include 'dv/dwh/iterable/user_profile_activity.py'  --method 'run' --start '2021-12-06 00:00:00' --end '2021-12-06 00:00:00'

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes CLONE data_vault_mvp.dwh.user_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_recent_activities CLONE data_vault_mvp.dwh.user_recent_activities;

SELECT GET_DDL('table', 'data_vault_mvp_dev_robin.dwh.dim_sale');
CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.dim_sale AS
SELECT *
FROM data_vault_mvp.dwh.dim_sale ds;

DROP TABLE data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity
WHERE daily_spv_deals IS NOT NULL;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.dim_sale AS
SELECT *
FROM data_vault_mvp.dwh.dim_sale;
CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification AS
SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touchification;
CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs AS
SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs;
CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.user_attributes AS
SELECT *
FROM data_vault_mvp.dwh.user_attributes;
CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.user_recent_activities AS
SELECT *
FROM data_vault_mvp.dwh.user_recent_activities;
CREATE OR REPLACE VIEW data_science_dev_robin.operational_output.vw_recommended_deals_augmented AS
SELECT *
FROM data_science.operational_output.vw_recommended_deals_augmented;

USE WAREHOUSE pipe_xlarge;

SELECT mt.attributed_user_id::INT AS shiro_user_id,
       mts.se_sale_id,
       ds.posu_city,
       li.city_id,
       ds.posu_division,
       li.division_id,
       ds.posu_country,
       li.country_id,
       ds.sale_product,
       OBJECT_CONSTRUCT(
               'dealId', mts.se_sale_id,
               'city', LOWER(ds.posu_city),
               'division', LOWER(ds.posu_division),
               'county', LOWER(ds.posu_country),
               'saleType', LOWER(ds.sale_product),
               'lastEventTstamp', TO_VARCHAR(mt.event_tstamp, 'YYYY-MM-DD HH24:MI:SS +00:00')
           )                      AS deal_object
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs mts
    INNER JOIN data_vault_mvp.single_customer_view_stg.module_touchification mt
               ON mts.touch_id = mt.touch_id
    INNER JOIN data_vault_mvp.dwh.dim_sale ds ON mts.se_sale_id = ds.se_sale_id
    INNER JOIN data_vault_mvp.dwh.se_sale ss ON mts.se_sale_id = ss.se_sale_id
    INNER JOIN data_vault_mvp.cms_mysql_snapshots.location_info_snapshot li ON ss.location_info_id = li.id
WHERE mts.event_tstamp::DATE >= CURRENT_DATE - 1     --to get a weeks worth of data
  AND mt.stitched_identity_type = 'se_user_id'       -- only member spvs
  AND LEFT(mts.se_sale_id, 3) IS DISTINCT FROM 'TVL' --remove travelist spvs
  AND mts.se_sale_id IS NOT NULL;

SELECT COUNT(*)
FROM data_vault_mvp.dwh.se_sale ss
-- INNER JOIN   se.data.se_location_info sli ON ss.location_info_id = sli.id


    self_describing_task --include '/dv/dwh/iterable/user_profile_activity.py'  --method 'run' --start '2021-12-07 00:00:00' --end '2021-12-07 00:00:00'


SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity
WHERE iterable__user_profile_activity.sales_pageviews_last_60_days IS NOT NULL;

SELECT *
FROM unload_vault_mvp_dev_robin.iterable.user_profile_activity_preprod__20211207t030000__daily_at_03h00
WHERE record:dataFields:userActivity:updatedAt::VARCHAR IS NOT NULL;


USE WAREHOUSE pipe_xlarge;


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.iterable__user_profile_activity_20211208 CLONE data_vault_mvp.dwh.iterable__user_profile_activity