CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.dim_sale CLONE data_vault_mvp.dwh.dim_sale;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification CLONE data_vault_mvp.single_customer_view_stg.module_touchification;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes CLONE data_vault_mvp.dwh.user_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_recent_activities CLONE data_vault_mvp.dwh.user_recent_activities;
CREATE OR REPLACE TRANSIENT TABLE data_science_dev_robin.operational_output.vw_recommended_deals_augmented CLONE data_science.operational_output.vw_recommended_deals_augmented;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity CLONE data_vault_mvp.dwh.iterable__user_profile_activity;

SELECT GET_DDL('table', 'data_science_dev_robin.operational_output.vw_recommended_deals_augmented');


SELECT OBJECT_CONSTRUCT(
               'userId', shiro_user_id::VARCHAR,
               'preferUserId', FALSE, -- we don't want to create the user if it doesn't already exist
               'dataFields', OBJECT_CONSTRUCT(
                       'userActivity', OBJECT_CONSTRUCT(
                        'updatedAt', TO_VARCHAR(updated_at, 'YYYY-MM-DD HH24:MI:SS +00:00'),
                        'lastEmailOpenTstamp', TO_VARCHAR(last_email_open_tstamp, 'YYYY-MM-DD HH24:MI:SS +00:00'),
                        'lastEmailClickTstamp', TO_VARCHAR(last_email_click_tstamp, 'YYYY-MM-DD HH24:MI:SS +00:00'),
                        'lastSpvTstamp', TO_VARCHAR(last_sale_pageview_tstamp, 'YYYY-MM-DD HH24:MI:SS +00:00'),
                        'lastPurchaseTstamp', TO_VARCHAR(last_purchase_tstamp, 'YYYY-MM-DD HH24:MI:SS +00:00'),
                        'dailySpvDeals', daily_spv_deals,
                        'weeklySpvDeals', weekly_spv_deals,
                        'segmentName', segment_name,
                        'athenaSegmentName', athena_segment_name,
                        'salesPageViewsLast30Days', sales_page_views_last_30_days,
                        'salesPageViewsLast60Days', sales_page_views_last_60_days,

                    -- our metadata fields
                        'outgoingScheduleTstamp', TO_VARCHAR('2022-01-03 03:00:00'::TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS +00:00'),
                        'outgoingRunTstamp', TO_VARCHAR('2022-01-04 10:51:48'::TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS +00:00')
                    )
                   )
           ) AS record
FROM unload_vault_mvp_dev_robin.iterable.user_profile_activity__20220103t030000__daily_at_03h00__step01__get_source_batch;


SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity
WHERE updated_at > '2022-01-03 03:00:00'::TIMESTAMP

SELECT * FROM data_vault_mvp.dwh.iterable__user_profile_activity iupa WHERE iupa.sales_page_views_last_30_days IS NOT NULL AND iupa.sales_page_views_last_30_days::varchar != '[]';