SELECT count(*)
FROM se.data.se_user_attributes sua
WHERE sua.first_app_activity_tstamp IS NOT NULL;

SELECT count(DISTINCT attributed_user_id)
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE mtba.stitched_identity_type = 'se_user_id'
  AND mtba.touch_experience = 'native app';

SELECT COUNT(DISTINCT mtba.attributed_user_id)
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
         LEFT JOIN se.data.se_user_attributes sua ON mtba.attributed_user_id = sua.shiro_user_id
WHERE mtba.stitched_identity_type = 'se_user_id'
  AND mtba.touch_experience = 'native app'
  AND sua.shiro_user_id IS NOT NULL;

USE WAREHOUSE pipe_xlarge;

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.user_first_activities_bkup CLONE data_vault_mvp.dwh.user_first_activities;
TRUNCATE data_vault_mvp_dev_robin.dwh.user_first_activities;

SELECT count(*)
FROM data_vault_mvp.dwh.user_first_activities ua
WHERE ua.first_app_activity_tstamp IS NOT NULL;

INSERT INTO data_vault_mvp_dev_robin.dwh.user_first_activities
WITH first_tstamp AS (
    SELECT attributed_user_id           AS se_user_id,
           MIN(mtba.touch_start_tstamp) AS first_app_activity_tstamp
    FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
    WHERE mtba.stitched_identity_type = 'se_user_id'
      AND mtba.touch_experience = 'native app'
    GROUP BY 1
)

SELECT current_timestamp::TIMESTAMP,
       current_timestamp::TIMESTAMP,
       'initial backfill',
       current_timestamp::TIMESTAMP,
       current_timestamp::TIMESTAMP,
       se_user_id,
       first_app_activity_tstamp
FROM first_tstamp
;


CREATE OR REPLACE TABLE data_vault_mvp.dwh.user_first_activities clone data_vault_mvp_dev_robin.dwh.user_first_activities;

SELECT count(*) FROM se.data.se_user_attributes sua WHERE sua.first_app_activity_tstamp IS NOT NULL;


