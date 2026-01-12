WITH spvs AS (
    SELECT mt.attributed_user_id::INT AS shiro_user_id,
           mts.se_sale_id,
           MAX(mt.event_tstamp)       AS last_event_tstamp
    FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs mts
        INNER JOIN data_vault_mvp.single_customer_view_stg.module_touchification mt ON mts.touch_id = mt.touch_id
    WHERE mts.event_tstamp::DATE >= CURRENT_DATE - 8
      AND mt.stitched_identity_type = 'se_user_id' -- only member spvs
    GROUP BY 1, 2
)
SELECT ura.shiro_user_id,
       ura.last_email_open_tstamp,
       ura.last_email_click_tstamp,
       ura.last_sale_pageview_tstamp,
       ura.last_purchase_tstamp,
       ARRAY_AGG(IFF(s.last_event_tstamp::DATE = CURRENT_DATE - 1, s.se_sale_id, NULL))
                 WITHIN GROUP (ORDER BY s.last_event_tstamp DESC)               AS daily_spv_deals,
       ARRAY_AGG(s.se_sale_id) WITHIN GROUP (ORDER BY s.last_event_tstamp DESC) AS weekly_spv_deals,
       SHA2(
                   ura.shiro_user_id ||
                   COALESCE(ura.last_email_open_tstamp::VARCHAR, '') ||
                   COALESCE(ura.last_email_click_tstamp::VARCHAR, '') ||
                   COALESCE(ura.last_sale_pageview_tstamp::VARCHAR, '') ||
                   COALESCE(ura.last_purchase_tstamp::VARCHAR, '') ||
                   COALESCE(daily_spv_deals::VARCHAR, '') ||
                   COALESCE(weekly_spv_deals::VARCHAR, '')
           , 256)                                                               AS row_hash
FROM data_vault_mvp.dwh.user_recent_activities ura
    LEFT JOIN spvs s ON ura.shiro_user_id = s.shiro_user_id
GROUP BY 1, 2, 3, 4, 5;


USE WAREHOUSE pipe_xlarge;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;
CREATE VIEW data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification AS
SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touchification;


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_recent_activities CLONE data_vault_mvp.dwh.user_recent_activities;
self_describing_task --include 'dv/dwh/iterable/user_profile_activity.py'  --method 'run' --start '2021-10-10 00:00:00' --end '2021-10-10 00:00:00'

SELECT *
FROM data_vault_mvp.dwh.user_recent_activities ura
WHERE ura.shiro_user_id IS NULL;

SELECT *
FROM data_vault_mvp.dwh.user_recent_activities ura;

SELECT iupa.schedule_tstamp,
       iupa.run_tstamp,
       iupa.operation_id,
       iupa.created_at,
       iupa.updated_at,
       iupa.shiro_user_id,
       iupa.last_email_open_tstamp,
       iupa.last_email_click_tstamp,
       iupa.last_sale_pageview_tstamp,
       iupa.last_purchase_tstamp,
       iupa.daily_spv_deals,
       iupa.weekly_spv_deals,
       iupa.row_hash
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity iupa
WHERE iupa.last_sale_pageview_tstamp::DATE = CURRENT_DATE - 1;
self_describing_task --include 'dv/dwh/iterable/user_profile.py'  --method 'run' --start '2021-10-10 00:00:00' --end '2021-10-10 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile;

SELECT mts.event_tstamp::DATE,
       COUNT(DISTINCT mt.attributed_user_id) AS users,
       COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs mts
    INNER JOIN data_vault_mvp.single_customer_view_stg.module_touchification mt ON mts.touch_id = mt.touch_id
WHERE mt.stitched_identity_type = 'se_user_id'
  AND mt.event_tstamp::DATE >= CURRENT_DATE - 10
GROUP BY 1;

------------------------------------------------------------------------------------------------------------------------

--remove us users

SELECT COUNT(*)
FROM data_vault_mvp.dwh.iterable__user_profile_activity iupa;
SELECT MIN(iupa.created_at)
FROM data_vault_mvp.dwh.iterable__user_profile_activity iupa;

SELECT COUNT(*)
FROM data_vault_mvp.dwh.iterable__user_profile iup;

SELECT ua.current_affiliate_territory,
       COUNT(*)
FROM data_vault_mvp.dwh.iterable__user_profile_activity iup
    INNER JOIN data_vault_mvp.dwh.user_attributes ua ON iup.shiro_user_id = ua.shiro_user_id
GROUP BY 1;


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes CLONE data_vault_mvp.dwh.user_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_recent_activities CLONE data_vault_mvp.dwh.user_recent_activities;

self_describing_task --include 'dv/dwh/iterable/user_profile_activity.py'  --method 'run' --start '2021-10-24 00:00:00' --end '2021-10-24 00:00:00'

DROP TABLE data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity;



------------------------------------------------------------------------------------------------------------------------
--post deployment

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.iterable__user_profile_activity_20211025 CLONE data_vault_mvp.dwh.iterable__user_profile_activity;

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity COPY GRANTS
(
    -- (lineage) metadata for the current job
    schedule_tstamp             TIMESTAMP,
    run_tstamp                  TIMESTAMP,
    operation_id                VARCHAR,
    created_at                  TIMESTAMP,
    updated_at                  TIMESTAMP,

    shiro_user_id               INT PRIMARY KEY NOT NULL,
    current_affiliate_territory VARCHAR,
    membership_account_status   VARCHAR,
    last_email_open_tstamp      TIMESTAMP,
    last_email_click_tstamp     TIMESTAMP,
    last_sale_pageview_tstamp   TIMESTAMP,
    last_purchase_tstamp        TIMESTAMP,
    daily_spv_deals             ARRAY,
    weekly_spv_deals            ARRAY,
    row_hash                    VARCHAR
);

INSERT INTO data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity
SELECT iupa.schedule_tstamp,
       iupa.run_tstamp,
       iupa.operation_id,
       iupa.created_at,
       iupa.updated_at,
       iupa.shiro_user_id,
       ua.current_affiliate_territory,
       iupa.membership_account_status,
       iupa.last_email_open_tstamp,
       iupa.last_email_click_tstamp,
       iupa.last_sale_pageview_tstamp,
       iupa.last_purchase_tstamp,
       iupa.daily_spv_deals,
       iupa.weekly_spv_deals,
       iupa.row_hash
FROM data_vault_mvp.dwh.iterable__user_profile_activity iupa
INNER JOIN data_vault_mvp.dwh.user_attributes ua ON iupa.shiro_user_id = ua.shiro_user_id
WHERE ua.current_affiliate_territory IS DISTINCT FROM 'US'