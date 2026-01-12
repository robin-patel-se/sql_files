USE WAREHOUSE pipe_2xlarge;

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.user_has_ranks AS (
    SELECT user_id
    FROM data_science.operational_output.vw_recommended_deals_augmented vrda
    GROUP BY 1
);

CREATE OR REPLACE VIEW data_science_dev_robin.operational_output.vw_recommended_deals_augmented AS
SELECT *
FROM data_science.operational_output.vw_recommended_deals_augmented
;


WITH step01__model_spvs AS (
    SELECT mt.attributed_user_id::INT AS shiro_user_id,
           mts.se_sale_id,
           MAX(mt.event_tstamp)       AS last_event_tstamp
    FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs mts
        INNER JOIN data_vault_mvp.single_customer_view_stg.module_touchification mt
                   ON mts.touch_id = mt.touch_id
    WHERE mts.event_tstamp::DATE >= CURRENT_DATE - 8 --to get a weeks worth of data
      AND mt.stitched_identity_type = 'se_user_id'   -- only member spvs
    GROUP BY 1, 2
),
     user_has_ranks AS (
         --temp created as scratch to aid prototype
         SELECT user_id
         FROM scratch.robinpatel.user_has_ranks
     )
SELECT ura.shiro_user_id,
       ua.current_affiliate_territory,
       ua.membership_account_status,
       ua.email_receive_sales_reminders IS NOT DISTINCT FROM 1         AS daily_opt_in,
       ua.email_receive_weekly_offers IS NOT DISTINCT FROM 1           AS weekly_opt_in,
       uhr.user_id IS NOT NULL                                         AS user_has_ranks,
       ura.last_email_open_tstamp,
       ura.last_email_click_tstamp,
       ura.last_sale_pageview_tstamp,
       ura.last_purchase_tstamp,
       GREATEST(COALESCE(ura.last_email_open_tstamp, '1970-01-01'),
                COALESCE(ura.last_email_click_tstamp, '1970-01-01'),
                COALESCE(ura.last_sale_pageview_tstamp, '1970-01-01')) AS most_recent_non_purchase_activity,
       CASE
           WHEN weekly_opt_in THEN
               CASE
                   WHEN daily_opt_in = FALSE AND DATEDIFF('month', most_recent_non_purchase_activity, CURRENT_DATE) < 24
                       THEN 'SEGMENT_CORE_' || UPPER(ua.current_affiliate_territory) || '_ACT_WKLY'
                   WHEN DATEDIFF('month', most_recent_non_purchase_activity, CURRENT_DATE) < 1
                       THEN 'SEGMENT_CORE_' || UPPER(ua.current_affiliate_territory) || '_ACT_01M'
                   WHEN DATEDIFF('month', most_recent_non_purchase_activity, CURRENT_DATE) < 3
                       THEN 'SEGMENT_CORE_' || UPPER(ua.current_affiliate_territory) || '_ACT_03M'
                   WHEN DATEDIFF('month', most_recent_non_purchase_activity, CURRENT_DATE) < 6
                       THEN 'SEGMENT_CORE_' || UPPER(ua.current_affiliate_territory) || '_ACT_06M'
                   WHEN DATEDIFF('month', most_recent_non_purchase_activity, CURRENT_DATE) < 9
                       THEN 'SEGMENT_CORE_' || UPPER(ua.current_affiliate_territory) || '_ACT_09M'
                   WHEN DATEDIFF('month', most_recent_non_purchase_activity, CURRENT_DATE) < 15
                       THEN 'SEGMENT_CORE_' || UPPER(ua.current_affiliate_territory) || '_ACT_15M'
                   WHEN DATEDIFF('month', most_recent_non_purchase_activity, CURRENT_DATE) < 24
                       THEN 'SEGMENT_CORE_' || UPPER(ua.current_affiliate_territory) || '_ACT_24M'
                   ELSE 'SEGMENT_CORE_' || UPPER(ua.current_affiliate_territory) || '_ACT_DEAD'
                   END
           END                                                         AS core_segment_name,
       IFF(core_segment_name LIKE '%_ACT_01M' AND UPPER(ua.current_affiliate_territory) IN ('UK', 'DE', 'SE', 'IT', 'NL', 'BE'),
           CASE
               --UK users with ranks
               WHEN UPPER(ua.current_affiliate_territory) = 'UK' AND uhr.user_id IS NOT NULL -- user has ranks
                   THEN
                   CASE
                       WHEN
                           ura.shiro_user_id REGEXP '.*(0|3|4|01|11|21|31|41|02|12|22|32|42)' THEN 'SEGMENT_CORE_UK_ACT_01M_ATHENA_PoC_A'
                       WHEN
                           ura.shiro_user_id REGEXP '.*(7|8|9|05|15|25|35|45|06|16|26|36|46)' THEN 'SEGMENT_CORE_UK_ACT_01M_ATHENA_PoC_A_10'
                       WHEN
                           ura.shiro_user_id REGEXP '.*(51|61|71|81|91|52|62|72|82|92)' THEN 'SEGMENT_CORE_UK_ACT_01M_ATHENA_PoC_B'
                       WHEN
                           ura.shiro_user_id REGEXP '.*(55|65|75|85|95|56|66|76|86|96)' THEN 'SEGMENT_CORE_UK_ACT_01M_ATHENA_PoC_B_10'
                       END
               --UK users without ranks
               WHEN UPPER(ua.current_affiliate_territory) = 'UK' AND uhr.user_id IS NULL -- user does not have ranks
                   THEN
                   CASE
                       WHEN
                           ura.shiro_user_id REGEXP '.*(0|5|6|3|4)' THEN 'SEGMENT_CORE_UK_ACT_01M_ATHENA_PoC_C'
                       WHEN
                           ura.shiro_user_id REGEXP '.*(1|2|7|8|9)' THEN 'SEGMENT_CORE_UK_ACT_01M_ATHENA_PoC_D'
                       END
               --DE users with ranks
               WHEN UPPER(ua.current_affiliate_territory) = 'DE' AND uhr.user_id IS NOT NULL -- user has ranks
                   THEN
                   CASE
                       WHEN
                           ura.shiro_user_id REGEXP '.*(9|2|3|1|8)' THEN 'SEGMENT_CORE_DE_ACT_01M_ATHENA_PoC_A'
                       WHEN
                           ura.shiro_user_id REGEXP '.*(4|5|6|7|0)' THEN 'SEGMENT_CORE_DE_ACT_01M_ATHENA_PoC_B'
                       END
               --DE users without ranks
               WHEN UPPER(ua.current_affiliate_territory) = 'DE' AND uhr.user_id IS NULL -- user does not have ranks
                   THEN
                   CASE
                       WHEN
                           ura.shiro_user_id REGEXP '.*(6|2|7|0|5)' THEN 'SEGMENT_CORE_DE_ACT_01M_ATHENA_PoC_C'
                       WHEN
                           ura.shiro_user_id REGEXP '.*(1|3|4|8|9)' THEN 'SEGMENT_CORE_DE_ACT_01M_ATHENA_PoC_D'
                       END
               --SE users with ranks
               WHEN UPPER(ua.current_affiliate_territory) = 'SE' AND uhr.user_id IS NOT NULL -- user has ranks
                   THEN
                   CASE
                       WHEN
                           ura.shiro_user_id REGEXP '.*(2|1|7|0|5)' THEN 'SEGMENT_CORE_SE_ACT_01M_ATHENA_PoC_A'
                       WHEN
                           ura.shiro_user_id REGEXP '.*(4|3|8|9|6)' THEN 'SEGMENT_CORE_SE_ACT_01M_ATHENA_PoC_B'
                       END
               --SE users without ranks
               WHEN UPPER(ua.current_affiliate_territory) = 'SE' AND uhr.user_id IS NULL -- user does not have ranks
                   THEN
                   CASE
                       WHEN
                           ura.shiro_user_id REGEXP '.*(9|2|3|1|8)' THEN 'SEGMENT_CORE_SE_ACT_01M_ATHENA_PoC_C'
                       WHEN
                           ura.shiro_user_id REGEXP '.*(7|4|5|6|0)' THEN 'SEGMENT_CORE_SE_ACT_01M_ATHENA_PoC_D'
                       END
               --IT, NL, BE users with ranks
               WHEN UPPER(ua.current_affiliate_territory) IN ('IT', 'NL', 'BE') AND uhr.user_id IS NOT NULL -- user has ranks
                   THEN
                   CASE
                       WHEN
                           ura.shiro_user_id REGEXP '.*(0|1|2|3|4)' THEN 'SEGMENT_CORE_' || UPPER(ua.current_affiliate_territory) || '_ACT_01M_ATHENA_PoC_A'
                       WHEN
                           ura.shiro_user_id REGEXP '.*(5|6|7|8|9)' THEN 'SEGMENT_CORE_' || UPPER(ua.current_affiliate_territory) || '_ACT_01M_ATHENA_PoC_B'
                       END
               --IT, NL, BE users without ranks
               WHEN UPPER(ua.current_affiliate_territory) IN ('IT', 'NL', 'BE') AND uhr.user_id IS NULL -- user does not have ranks
                   THEN
                   CASE
                       WHEN
                           ura.shiro_user_id REGEXP '.*(0|5|6|3|4)' THEN 'SEGMENT_CORE_' || UPPER(ua.current_affiliate_territory) || '_ACT_01M_ATHENA_PoC_C'
                       WHEN
                           ura.shiro_user_id REGEXP '.*(1|2|7|8|9)' THEN 'SEGMENT_CORE_' || UPPER(ua.current_affiliate_territory) || '_ACT_01M_ATHENA_PoC_D'
                       END
               END
           , NULL)                                                     AS athena_segment_name,

       ARRAY_AGG(IFF(s.last_event_tstamp::DATE = CURRENT_DATE - 1, s.se_sale_id, NULL))
                 WITHIN
                     GROUP (ORDER BY s.last_event_tstamp DESC)         AS daily_spv_deals_test,
       ARRAY_AGG(s.se_sale_id) WITHIN
           GROUP (ORDER BY s.last_event_tstamp DESC)                   AS weekly_spv_deals_test,
       -- used for delta loading, deliberately exclude membership_account_status
       -- and current affiliate because that's not relevant for delta
       SHA2(
                   ura.shiro_user_id ||
                   COALESCE(ura.last_email_open_tstamp::VARCHAR, '') ||
                   COALESCE(ura.last_email_click_tstamp::VARCHAR, '') ||
                   COALESCE(ura.last_sale_pageview_tstamp::VARCHAR, '') ||
                   COALESCE(ura.last_purchase_tstamp::VARCHAR, '') ||
                   COALESCE(daily_spv_deals_test::VARCHAR, '') ||
                   COALESCE(weekly_spv_deals_test::VARCHAR, '') ||
                   COALESCE(core_segment_name, '')
           , 256)                                                      AS row_hash
FROM data_vault_mvp.dwh.user_recent_activities ura
    INNER JOIN data_vault_mvp.dwh.user_attributes ua
               ON ura.shiro_user_id = ua.shiro_user_id
    LEFT JOIN  step01__model_spvs s ON ura.shiro_user_id = s.shiro_user_id
    LEFT JOIN  user_has_ranks uhr ON ura.shiro_user_id = uhr.user_id
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13;


------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification CLONE data_vault_mvp.single_customer_view_stg.module_touchification;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes CLONE data_vault_mvp.dwh.user_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_recent_activities CLONE data_vault_mvp.dwh.user_recent_activities;

self_describing_task --include 'dv/dwh/iterable/user_profile_activity.py'  --method 'run' --start '2021-11-22 00:00:00' --end '2021-11-22 00:00:00'

DROP TABLE data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity;

CREATE OR REPLACE TRANSIENT TABLE collab.iterable_data.user_segment_name COPY GRANTS AS (
    SELECT iupa.shiro_user_id,
           iupa.current_affiliate_territory,
           iupa.membership_account_status,
           iupa.daily_opt_in,
           iupa.weekly_opt_in,
           iupa.user_has_ranks,
           iupa.last_email_open_tstamp,
           iupa.last_email_click_tstamp,
           iupa.last_sale_pageview_tstamp,
           iupa.last_purchase_tstamp,
           iupa.most_recent_non_purchase_activity,
           iupa.segment_name,
           iupa.athena_segment_name
    FROM data_vault_mvp.dwh.iterable__user_profile_activity iupa
);



self_describing_task --include 'staging/outgoing/iterable/user_profile_activity/modelling.py'  --method 'run' --start '2021-11-22 00:00:00' --end '2021-11-22 00:00:00'

SELECT *
FROM unload_vault_mvp_dev_robin.iterable.user_profile_activity__20211121t030000__daily_at_03h00;


GRANT SELECT ON TABLE collab.iterable_data.user_segment_name TO ROLE personal_role__jenniferbirks;
GRANT SELECT ON TABLE collab.iterable_data.user_segment_name TO ROLE personal_role__kostaschaveles;

SELECT *
FROM data_vault_mvp.dwh.iterable__user_profile_activity iupal

SELECT iupa.segment_name,
--        iupa.athena_segment_name,
       COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity iupa
GROUP BY 1;

DROP TABLE data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity;

------------------------------------------------------------------------------------------------------------------------


curl --X -H "Api_Key: ed24426db35d4e6dbb71691699fef3d4" https://api.iterable.com/api/lists/getUsers?listId=1311433 > list_1311433.csv

--download a list of segment_core_uk_act_01m id 1311433
curl --X -H "Api_Key: d2302ebc74e14c8d9ce6cef9336c721f" https://api.iterable.com/api/lists/getUsers?listId=1311433 > list_1311433.csv

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.iterable_list_1311433_users
(
    email VARCHAR
);

USE SCHEMA scratch.robinpatel;

PUT file:///Users/robin/myrepos/sql_files/Iterable/list_1311433.csv @%iterable_list_1311433_users;

COPY INTO scratch.robinpatel.iterable_list_1311433_users
    FILE_FORMAT = (
        TYPE = CSV
            FIELD_DELIMITER = ','
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
            RECORD_DELIMITER = '\\n'
        );
-- 2,076,856
SELECT *
FROM scratch.robinpatel.iterable_list_1311433_users;

-- 2,172,147
SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity iupa
WHERE iupa.segment_name = 'SEGMENT_CORE_UK_ACT_01M';


--work out difference in users
WITH exception_users AS (
    SELECT LOWER(ua.email) AS email
    FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity iupa
        INNER JOIN data_vault_mvp.dwh.user_attributes ua ON iupa.shiro_user_id = ua.shiro_user_id
    WHERE iupa.segment_name = 'SEGMENT_CORE_UK_ACT_01M'

    MINUS

    SELECT LOWER(email) AS email
    FROM scratch.robinpatel.iterable_list_1311433_users
)
SELECT u.email,
       i.shiro_user_id,
       i.current_affiliate_territory,
       i.daily_opt_in,
       i.weekly_opt_in,
       i.user_has_ranks,
       i.last_email_open_tstamp,
       i.last_email_click_tstamp,
       i.last_sale_pageview_tstamp,
       i.most_recent_non_purchase_activity,
       i.segment_name,
       i.athena_segment_name,
       i.row_hash
FROM exception_users eu
    INNER JOIN data_vault_mvp.dwh.user_attributes u ON eu.email = u.email
    INNER JOIN data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity i ON u.shiro_user_id = i.shiro_user_id
;
USE WAREHOUSE pipe_xlarge;
SELECT *
FROM data_science.operational_output.vw_recommended_deals_augmented
WHERE user_id = 71382712;

--found the issue might live with the main affiliate id filter

self_describing_task --include 'dv/dwh/user_attributes/user_attributes.py'  --method 'run' --start '2021-11-24 00:00:00' --end '2021-11-24 00:00:00'
CREATE OR REPLACE TRANSIENT TABLE raw_vault_mvp_dev_robin.chiasma_sql_server.affiliate_classification CLONE raw_vault_mvp.chiasma_sql_server.affiliate_classification;
CREATE OR REPLACE TRANSIENT TABLE raw_vault_mvp_dev_robin.chiasma_sql_server.user_acquisition_platform CLONE raw_vault_mvp.chiasma_sql_server.user_acquisition_platform;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.affiliate CLONE hygiene_snapshot_vault_mvp.cms_mysql.affiliate;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.membership CLONE hygiene_snapshot_vault_mvp.cms_mysql.membership;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.profile CLONE hygiene_snapshot_vault_mvp.cms_mysql.profile;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.shiro_user CLONE hygiene_snapshot_vault_mvp.cms_mysql.shiro_user;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.subscription CLONE hygiene_snapshot_vault_mvp.cms_mysql.subscription;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.territory CLONE hygiene_snapshot_vault_mvp.cms_mysql.territory;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.theme CLONE hygiene_snapshot_vault_mvp.cms_mysql.theme;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_booking_review CLONE data_vault_mvp.dwh.user_booking_review;


SELECT segment_name,
       COUNT(*)
FROM data_vault_mvp.dwh.iterable__user_profile_activity
GROUP BY 1;

SELECT athena_segment_name,
       COUNT(*)
FROM data_vault_mvp.dwh.iterable__user_profile_activity
GROUP BY 1;

SELECT COUNT(*)
FROM data_vault_mvp.dwh.iterable__user_profile_activity;

SELECT *
FROM data_vault_mvp.dwh.iterable__user_profile_activity iupa
WHERE iupa.shiro_user_id = 75050255;

------------------------------------------------------------------------------------------------------------------------

SELECT upa.daily_spv_deals
FROM data_vault_mvp.dwh.iterable__user_profile_activity upa
WHERE upa.daily_spv_deals::VARCHAR LIKE '%TVL%';

SELECT upa.daily_spv_deals
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity upa
WHERE upa.daily_spv_deals::VARCHAR LIKE '%TVL%';


SELECT upa.weekly_spv_deals
FROM data_vault_mvp.dwh.iterable__user_profile_activity upa
WHERE upa.weekly_spv_deals::VARCHAR LIKE '%TVL%';

SELECT upa.weekly_spv_deals
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity upa
WHERE upa.weekly_spv_deals::VARCHAR LIKE '%TVL%';


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification CLONE data_vault_mvp.single_customer_view_stg.module_touchification;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes CLONE data_vault_mvp.dwh.user_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_recent_activities CLONE data_vault_mvp.dwh.user_recent_activities;
CREATE OR REPLACE TRANSIENT TABLE data_science_dev_robin.operational_output.vw_recommended_deals_augmented CLONE data_science.operational_output.vw_recommended_deals_augmented;

