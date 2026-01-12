--user field schema

SELECT ua.shiro_user_id                                                                         AS shiro_user_id,
       ua.email                                                                                 AS email_address,
       ua.membership_account_status                                                             AS account_status,
       RIGHT(LEFT(SHA2(COALESCE(ua.email, '') || COALESCE(su.password_hash, ''), 256), 20), 15) AS reference,
       t.locale,
       SPLIT_PART(t.locale, '_', 1)                                                             AS territory,
       ua.current_affiliate_territory                                                           AS territory_region,
       ua.current_affiliate_id                                                                  AS affiliate_id,
       ua.current_affiliate_brand                                                               AS affilate_brand,
       ua.current_affiliate_domain                                                              AS affiliate_domain,
       ua.signup_tstamp                                                                         AS date_joined,
       ua.email_opt_in IS NOT DISTINCT FROM 1                                                   AS weekly_opt_in,
       ua.email_opt_in IS NOT DISTINCT FROM 2                                                   AS daily_opt_in,
       p.receive_hand_picked_offers IS NOT DISTINCT FROM 1                                      AS third_party_optin,
       ua.pause_subscription_end_tstamp,
       --unsubscribe_type
       --subscription_modified_tstamp
       ua.title,
       ua.first_name,
       ua.surname,
       p.region                                                                                 AS division,
       ua.country,
       ua.referrer_id                                                                           AS referred_by,
       --date_modified
       SHA2(CONCAT(
               ua.shiro_user_id,
               ua.email,
               COALESCE(ua.membership_account_status, ''),
               reference,
               t.locale,
               SPLIT_PART(t.locale, '_', 1),
               ua.current_affiliate_territory,
               ua.current_affiliate_id,
               COALESCE(ua.current_affiliate_brand, ''),
               COALESCE(ua.current_affiliate_domain, ''),
               (ua.signup_tstamp)::VARCHAR,
               (weekly_opt_in)::VARCHAR,
               (daily_opt_in)::VARCHAR,
               (third_party_optin)::VARCHAR,
               IFF(p.receive_hand_picked_offers IS NOT DISTINCT FROM 1, 'TRUE', 'FALSE'),
               COALESCE(ua.pause_subscription_end_tstamp::VARCHAR, ''),
               COALESCE(ua.title, ''),
               COALESCE(ua.first_name, ''),
               COALESCE(ua.surname, ''),
               COALESCE(p.region, ''),
               COALESCE(ua.country, ''),
               COALESCE(ua.referrer_id::VARCHAR, '')
           ))                                                                                   AS row_hash

FROM data_vault_mvp.dwh.user_attributes ua
    LEFT JOIN hygiene_snapshot_vault_mvp.cms_mysql.shiro_user su ON ua.shiro_user_id = su.id
    LEFT JOIN hygiene_snapshot_vault_mvp.cms_mysql.territory t ON ua.current_affiliate_territory_id = t.id
    LEFT JOIN hygiene_snapshot_vault_mvp.cms_mysql.profile p ON ua.profile_id = p.id
WHERE row_hash IS NULL
;

self_describing_task --include 'dv/dwh/iterable/user_profile.py'  --method 'run' --start '2021-09-14 00:00:00' --end '2021-09-14 00:00:00'

DROP TABLE data_vault_mvp_dev_robin.dwh.iterable__user_profile;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile iup;



SELECT ua.shiro_user_id                                                                         AS shiro_user_id,
       ua.email                                                                                 AS email_address,
       ua.membership_account_status                                                             AS account_status,
       RIGHT(LEFT(SHA2(COALESCE(ua.email, '') || COALESCE(su.password_hash, ''), 256), 20), 15) AS reference,
       t.locale,
       SPLIT_PART(t.locale, '_', 1)                                                             AS territory,
       ua.current_affiliate_territory                                                           AS territory_region,
       ua.current_affiliate_id                                                                  AS affiliate_id,
       ua.current_affiliate_brand                                                               AS affilate_brand,
       ua.current_affiliate_domain                                                              AS affiliate_domain,
       ua.signup_tstamp                                                                         AS date_joined,
       ua.email_opt_in IS NOT DISTINCT FROM 1                                                   AS weekly_opt_in,
       ua.email_opt_in IS NOT DISTINCT FROM 2                                                   AS daily_opt_in,
       p.receive_hand_picked_offers IS NOT DISTINCT FROM 1                                      AS third_party_optin,
       ua.pause_subscription_end_tstamp,
       --unsubscribe_type
       --subscription_modified_tstamp
       ua.title,
       ua.first_name,
       ua.surname,
       p.region                                                                                 AS division,
       ua.country,
       ua.referrer_id                                                                           AS referred_by,

FROM data_vault_mvp.dwh.user_attributes ua
    LEFT JOIN hygiene_snapshot_vault_mvp.cms_mysql.shiro_user su ON ua.shiro_user_id = su.id
    LEFT JOIN hygiene_snapshot_vault_mvp.cms_mysql.territory t ON ua.current_affiliate_territory_id = t.id
    LEFT JOIN hygiene_snapshot_vault_mvp.cms_mysql.profile p ON ua.profile_id = p.id
WHERE ua.membership_account_status IS DISTINCT FROM 'DELETED' --not include any deleted user information
;


SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile;


SELECT *
FROM data_vault_mvp.dwh.iterable__user_profile;

CREATE SCHEMA collab.iterable_data;
GRANT USAGE ON SCHEMA collab.iterable_data TO ROLE personal_role__apoorvakapavarapu;
GRANT USAGE ON SCHEMA collab.iterable_data TO ROLE data_team_basic;
GRANT USAGE ON SCHEMA collab.iterable_data TO ROLE personal_role__bendeavin;
GRANT SELECT ON ALL TABLES IN SCHEMA collab.iterable_data TO ROLE personal_role__apoorvakapavarapu;
GRANT SELECT ON ALL TABLES IN SCHEMA collab.iterable_data TO ROLE data_team_basic;
GRANT SELECT ON ALL TABLES IN SCHEMA collab.iterable_data TO ROLE personal_role__bendeavin;
GRANT SELECT ON ALL VIEWS IN SCHEMA collab.iterable_data TO ROLE personal_role__apoorvakapavarapu;
GRANT SELECT ON ALL VIEWS IN SCHEMA collab.iterable_data TO ROLE data_team_basic;
GRANT SELECT ON ALL VIEWS IN SCHEMA collab.iterable_data TO ROLE personal_role__bendeavin;


USE ROLE personal_role__robinpatel;
CREATE OR REPLACE VIEW collab.iterable_data.iterable__user_profile__bulk AS
SELECT *
FROM data_vault_mvp.dwh.iterable__user_profile iup;



SELECT *
FROM collab.iterable_data.iterable__user_profile__bulk;

SELECT *
FROM se.data.se_booking sb
WHERE sb.booking_status = 'CANCELLED'
  AND sb.cancellation_date;

USE WAREHOUSE pipe_xlarge;



------------------------------------------------------------------------------------------------------------------------

--adjust

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes CLONE data_vault_mvp.dwh.user_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_recent_activities CLONE data_vault_mvp.dwh.user_recent_activities;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity CLONE data_vault_mvp.dwh.iterable__user_profile_activity;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.shiro_user CLONE hygiene_snapshot_vault_mvp.cms_mysql.shiro_user;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.profile CLONE hygiene_snapshot_vault_mvp.cms_mysql.profile;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.territory CLONE hygiene_snapshot_vault_mvp.cms_mysql.territory;

self_describing_task --include 'dv/dwh/iterable/user_profile.py'  --method 'run' --start '2021-10-19 00:00:00' --end '2021-10-19 00:00:00'


SELECT *
FROM data_vault_mvp.dwh.iterable__user_profile iup
WHERE iup.affiliate_domain IS NULL;
SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile iup
WHERE iup.affiliate_domain IS NULL;

SELECT *
FROM hygiene_snapshot_vault_mvp.cms_mysql.affiliate a
WHERE id = 1114;


SELECT ua.shiro_user_id,
       ua.current_affiliate_id,
       ua.current_affiliate_name,
       ua.current_affiliate_domain,
       ua.original_affiliate_id,
       ua.original_affiliate_name,
       ua.original_affiliate_domain
FROM data_vault_mvp.dwh.user_attributes ua
WHERE ua.current_affiliate_domain IS NULL;

SELECT *
FROM archive.information_schema.tables t
WHERE table_schema = 'SFMC';

SELECT dc.affiliateid,
       COUNT(*)
FROM archive.sfmc.dim_customers dc
GROUP BY 1,

SELECT *
FROM archive.sfmc.dim_customers;


------------------------------------------------------------------------------------------------------------------------

SELECT ua.shiro_user_id                                                                         AS shiro_user_id,
       ua.email                                                                                 AS email_address, -- NOTE: This column is considered PII
       ua.membership_account_status,
       RIGHT(LEFT(SHA2(COALESCE(su.password_hash, '') || COALESCE(ua.email, ''), 256), 21), 16) AS reference,
       t.locale,
       LOWER(SPLIT_PART(t.locale, '_', 1))                                                      AS territory,
       ua.current_affiliate_territory                                                           AS territory_region,
       ua.current_affiliate_id                                                                  AS affiliate_id,
       COALESCE(a.id, a2.id)                                                                    AS main_affiliate_id,
       th.application_name                                                                      AS affiliate_brand,
       COALESCE(a.domain, a2.domain)                                                            AS affiliate_domain,
       ua.signup_tstamp,
       p.receive_weekly_offers IS NOT DISTINCT FROM 1                                           AS weekly_opt_in,
       p.receive_sales_reminders IS NOT DISTINCT FROM 1                                         AS daily_opt_in,
       p.receive_hand_picked_offers IS NOT DISTINCT FROM 1                                      AS third_party_optin,
       ua.pause_subscription_end_tstamp,
       -- unsubscribe_type
       -- subscription_modified_tstamp
       ua.title,
       ua.first_name,                                                                                             -- NOTE: This column is considered PII
       ua.surname,                                                                                                -- NOTE: This column is considered PII
       p.region,
       ua.country,
       ua.referrer_id,
       upa.last_email_open_tstamp,
       upa.last_email_click_tstamp,
       upa.last_sale_pageview_tstamp,
       upa.last_purchase_tstamp,
       upa.daily_spv_deals,
       upa.weekly_spv_deals

FROM data_vault_mvp.dwh.user_attributes ua
    LEFT JOIN hygiene_snapshot_vault_mvp.cms_mysql.shiro_user su ON ua.shiro_user_id = su.id
    LEFT JOIN hygiene_snapshot_vault_mvp.cms_mysql.territory t ON ua.current_affiliate_territory_id = t.id
    LEFT JOIN hygiene_snapshot_vault_mvp.cms_mysql.profile p ON ua.profile_id = p.id
    LEFT JOIN data_vault_mvp.dwh.iterable__user_profile_activity upa ON ua.shiro_user_id = upa.shiro_user_id
                  --to compute application domain
    LEFT JOIN hygiene_snapshot_vault_mvp.cms_mysql.affiliate a ON ua.current_affiliate_id = a.id AND a.main_for_domain AND a.active
    LEFT JOIN hygiene_snapshot_vault_mvp.cms_mysql.affiliate a2 ON t.default_affiliate_id = a2.id
    LEFT JOIN hygiene_snapshot_vault_mvp.cms_mysql.theme th ON COALESCE(a.theme_id, a2.theme_id) = th.id

WHERE ua.membership_account_status IS DISTINCT FROM 'DELETED' --not include any deleted user information
  AND ua.current_affiliate_territory IS DISTINCT FROM 'US' -- CRM team have instructed us that NO US members should exist in iterable
;


self_describing_task --include 'dv/dwh/iterable/user_profile.py'  --method 'run' --start '2021-10-25 00:00:00' --end '2021-10-25 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile
    QUALIFY COUNT(*) OVER (PARTITION BY email_address) > 1;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity CLONE data_vault_mvp.dwh.iterable__user_profile_activity;
self_describing_task --include 'dv/dwh/iterable/user_profile_activity.py'  --method 'run' --start '2021-10-25 00:00:00' --end '2021-10-25 00:00:00'

USE WAREHOUSE pipe_xlarge;
SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile
WHERE is_email_address_duplicate
    QUALIFY COUNT(*) OVER (PARTITION BY LOWER(REGEXP_REPLACE(email_address, '\\s+', ''))) > 2;

SELECT *
FROM data_vault_mvp.dwh.user_recent_activities ura
    INNER JOIN data_vault_mvp.dwh.user_attributes ua ON ura.shiro_user_id = ua.shiro_user_id
WHERE ua.email LIKE 'robin.patel%';

USE WAREHOUSE pipe_xlarge;

SELECT sta.attribution_model,
       count(*)
FROM se.data.scv_touch_attribution sta
GROUP BY 1