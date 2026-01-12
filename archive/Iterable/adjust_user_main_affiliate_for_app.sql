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


self_describing_task --include 'dv/dwh/user_attributes/user_attributes.py'  --method 'run' --start '2021-12-13 00:00:00' --end '2021-12-13 00:00:00'

DROP VIEW data_vault_mvp_dev_robin.dwh.user_attributes;

SELECT *
FROM hygiene_snapshot_vault_mvp.cms_mysql.affiliate a
WHERE a.domain = 'api.secretescapes.com'


SELECT *
FROM data_vault_mvp_dev_robin.dwh.user_attributes
WHERE original_affiliate_domain = 'api.secretescapes.com';

self_describing_task --include 'dv/dwh/iterable/user_profile.py'  --method 'run' --start '2021-12-13 00:00:00' --end '2021-12-13 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile iup
WHERE iup.affiliate_id = 658;

USE WAREHOUSE pipe_xlarge
;


SELECT iup.shiro_user_id,
       ua.email,
       iup.affiliate_id,
       iup.affiliate_brand,
       ua.current_affiliate_domain,
       iup.affiliate_domain,
       ua.main_affiliate_id,
       ua.membership_account_status
FROM data_vault_mvp.dwh.iterable__user_profile iup
    INNER JOIN data_vault_mvp.dwh.user_attributes ua ON iup.shiro_user_id = ua.shiro_user_id
WHERE ua.membership_account_status IS DISTINCT FROM 'DELETED'
  AND ua.shiro_user_id IN (75950562, 36012188)
;

self_describing_task --include 'staging/outgoing/iterable/user_profile_activity/modelling.py'  --method 'run' --start '2021-12-15 00:00:00' --end '2021-12-15 00:00:00'


SELECT *
FROM unload_vault_mvp_dev_robin.iterable.user_profile_activity__20211214t030000__daily_at_03h00;

SELECT ua.shiro_user_id,
       ua.email,
       ua.current_affiliate_id,
       ua.main_affiliate_id,
       ua.current_affiliate_domain,
       ua.main_affiliate_domain
FROM data_vault_mvp.dwh.user_attributes ua
WHERE ua.shiro_user_id = 72625129

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.iterable_api_users
(
    email             varchar,
    userid1           number,
    userid2           number,
    applicationdomain varchar,
    dailyoptin        boolean,
    mainaffiliateid   number,
    user_id3          number,
    weeklyoptin       boolean
);



USE SCHEMA scratch.robinpatel;

PUT file:///Users/robin/myrepos/sql_files/Iterable/api_users_in_iterable.csv @%iterable_api_users;



COPY INTO scratch.robinpatel.iterable_api_users
    FILE_FORMAT = (
        TYPE = CSV
            FIELD_DELIMITER = ','
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
            RECORD_DELIMITER = '\\n'
        );

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.iterable_api_users_to_update AS
WITH sf_api_users AS (

    SELECT iup.shiro_user_id,
           ua.email,
           iup.affiliate_id,
           iup.affiliate_brand,
           ua.current_affiliate_domain,
           iup.affiliate_domain,
           ua.main_affiliate_id,
           ua.membership_account_status
    FROM data_vault_mvp.dwh.iterable__user_profile iup
        INNER JOIN data_vault_mvp.dwh.user_attributes ua ON iup.shiro_user_id = ua.shiro_user_id AND ua.current_affiliate_domain = 'api.secretescapes.com'
)
SELECT sau.email,
       sau.shiro_user_id,
       sau.affiliate_domain,
       sau.main_affiliate_id
FROM sf_api_users sau
    INNER JOIN scratch.robinpatel.iterable_api_users iau ON sau.shiro_user_id = iau.userid2;


SELECT *
FROM scratch.robinpatel.iterable_api_users_to_update;


------------------------------------------------------------------------------------------------------------------------
--investigate the users that should be set to update that havent

WITH sf_api_users AS (

    SELECT iup.shiro_user_id,
           ua.email,
           iup.affiliate_id,
           iup.affiliate_brand,
           ua.current_affiliate_domain,
           iup.affiliate_domain,
           ua.main_affiliate_id,
           ua.membership_account_status
    FROM data_vault_mvp.dwh.iterable__user_profile iup
        INNER JOIN data_vault_mvp.dwh.user_attributes ua ON iup.shiro_user_id = ua.shiro_user_id AND ua.current_affiliate_domain = 'api.secretescapes.com'
)
SELECT sau.email,
       sau.shiro_user_id,
       sau.affiliate_domain,
       sau.main_affiliate_id,
       iau.email,
       iau.userid2           AS shiro_user_id,
       iau.applicationdomain AS affiliate_domain,
       iau.mainaffiliateid   AS main_affiliate_id
FROM sf_api_users sau
    FULL OUTER JOIN scratch.robinpatel.iterable_api_users iau ON sau.shiro_user_id = iau.userid2


------------------------------------------------------------------------------------------------------------------------

WITH sf_api_users AS (

    SELECT iup.shiro_user_id,
           ua.email,
           iup.affiliate_id,
           iup.affiliate_brand,
           ua.current_affiliate_domain,
           iup.affiliate_domain,
           ua.main_affiliate_id,
           ua.membership_account_status
    FROM data_vault_mvp.dwh.iterable__user_profile iup
        INNER JOIN data_vault_mvp.dwh.user_attributes ua ON iup.shiro_user_id = ua.shiro_user_id AND ua.current_affiliate_domain = 'api.secretescapes.com'
),
     compare AS (
         SELECT sau.email,
                sau.shiro_user_id,
                sau.affiliate_domain,
                sau.main_affiliate_id AS new_main_aff_id,
                iau.mainaffiliateid   AS old_main_aff_id
         FROM sf_api_users sau
             INNER JOIN scratch.robinpatel.iterable_api_users iau ON sau.shiro_user_id = iau.userid2
     )
SELECT compare.new_main_aff_id,
       compare.old_main_aff_id,
       COUNT(*)
FROM compare
GROUP BY 1, 2;


SELECT DISTINCT sua.current_affiliate_id
FROM se.data.se_user_attributes sua
WHERE sua.current_affiliate_domain = 'api.secretescapes.com'
;


CREATE TRANSIENT TABLE collab.iterable_data.main_affiliate_users_to_update AS (
    SELECT ua.shiro_user_id,
           ua.main_affiliate_id
    FROM data_vault_mvp.dwh.user_attributes_20211217 ua
        EXCEPT
    SELECT ua.shiro_user_id,
           ua.main_affiliate_id
    FROM data_vault_mvp.dwh.user_attributes ua
);

WITH comparison AS (
    SELECT u.shiro_user_id,
           u.current_affiliate_id,
           u.current_affiliate_domain,
           u.main_affiliate_id     AS dev_main_affiliate_id,
           u.main_affiliate_domain AS dev_main_affiliate_domain,
           a.main_affiliate_id     AS prod_main_affiliate_id,
           a.main_affiliate_domain AS prod_main_affiliate_domain
    FROM data_vault_mvp_dev_robin.dwh.user_attributes u
        INNER JOIN data_vault_mvp.dwh.user_attributes a ON u.shiro_user_id = a.shiro_user_id
    WHERE a.main_affiliate_id != u.main_affiliate_id
)
SELECT c.dev_main_affiliate_id,
       c.prod_main_affiliate_id,
       COUNT(*)
FROM comparison c
GROUP BY 1, 2;


SELECT DISTINCT
       ua.current_affiliate_id,
       ua.main_affiliate_id
FROM data_vault_mvp_dev_robin.dwh.user_attributes ua
-- WHERE ua.current_affiliate_id IN (131, 132, 133, 134, 135, 136, 137);
WHERE ua.current_affiliate_id = 883