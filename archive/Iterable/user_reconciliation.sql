WITH sfmc AS (
    SELECT t1.userid::INT                 AS sfmc_userid,
           LOWER(t1.email)                AS sfmc_email,
           t1.locale                      AS sfmc_locale,
           t1.territory                   AS sfmc_territory,
           t1.affiliateid                 AS sfmc_affiliateid,
           t1.affiliatebrand              AS sfmc_affiliatebrand,
           t1.applicationdomain           AS sfmc_applicationdomain,
           t1.datejoined::DATE            AS sfmc_datejoined,
           TRIM(t1.forename)              AS sfmc_forename,
           TRIM(t1.surname)               AS sfmc_surname,
           t1.weeklyoptin IS NOT NULL     AS sfmc_weeklyoptin,
           t1.dailyoptin IS NOT NULL      AS sfmc_dailyoptin,
           t1.thirdpartyoptin IS NOT NULL AS sfmc_thirdpartyoptin,
           t1.reference                   AS sfmc_reference
    FROM archive.sfmc.dim_customers t1
),
     snowflake AS (
         SELECT t2.shiro_user_id        AS snowflake_userid,
                LOWER(t2.email_address) AS snowflake_email,
                t2.locale               AS snowflake_locale,
                t2.territory_region     AS snowflake_territory, --ask jen to investigate
                t2.affiliate_id         AS snowflake_affiliateid,
                t2.affiliate_brand      AS snowflake_affiliatebrand,
                t2.affiliate_domain     AS snowflake_applicationdomain,
                t2.signup_tstamp::DATE  AS snowflake_datejoined,
                t2.first_name           AS snowflake_forename,
                t2.surname              AS snowflake_surname,
                t2.weekly_opt_in        AS snowflake_weeklyoptin,
                t2.daily_opt_in         AS snowflake_dailyoptin,
                t2.third_party_optin    AS snowflake_thirdpartyoptin,
                t2.membership_account_status,
                t2.reference            AS snowflake_reference
         FROM data_vault_mvp.dwh.iterable__user_profile t2
     ),
     modelling AS (

         SELECT COALESCE(s.sfmc_userid, sf.snowflake_userid)                                 AS user_id,
                CASE
                    WHEN s.sfmc_userid IS NOT NULL AND sf.snowflake_userid IS NULL THEN 'in sfmc not in snowflake'
                    WHEN s.sfmc_userid IS NULL AND sf.snowflake_userid IS NOT NULL THEN 'in snowflake not in sfmc'
                    ELSE 'match user id'
                    END                                                                      AS user_match,
                sf.membership_account_status,
                s.sfmc_email,
                sf.snowflake_email,
                s.sfmc_email IS NOT DISTINCT FROM sf.snowflake_email                         AS email_match,
                s.sfmc_locale,
                sf.snowflake_locale,
                s.sfmc_locale IS NOT DISTINCT FROM sf.snowflake_locale                       AS locale_match,
                s.sfmc_territory,
                sf.snowflake_territory,
                s.sfmc_territory IS NOT DISTINCT FROM sf.snowflake_territory                 AS territory_match,
                s.sfmc_affiliateid,
                sf.snowflake_affiliateid,
                s.sfmc_affiliateid IS NOT DISTINCT FROM sf.snowflake_affiliateid             AS affiliate_id_match,
                s.sfmc_affiliatebrand,
                sf.snowflake_affiliatebrand,
                s.sfmc_affiliatebrand IS NOT DISTINCT FROM sf.snowflake_affiliatebrand       AS affiliate_brand_match,
                s.sfmc_datejoined,
                sf.snowflake_datejoined,
                s.sfmc_datejoined IS NOT DISTINCT FROM sf.snowflake_datejoined               AS date_joined_match,
                s.sfmc_forename,
                sf.snowflake_forename,
                s.sfmc_forename IS NOT DISTINCT FROM sf.snowflake_forename                   AS forename_match,
                s.sfmc_surname,
                sf.snowflake_surname,
                s.sfmc_surname IS NOT DISTINCT FROM sf.snowflake_surname                     AS surname_match,
                s.sfmc_weeklyoptin,
                sf.snowflake_weeklyoptin,
                s.sfmc_weeklyoptin IS NOT DISTINCT FROM sf.snowflake_weeklyoptin             AS weeklyoptin_match,
                s.sfmc_dailyoptin,
                sf.snowflake_dailyoptin,
                s.sfmc_dailyoptin IS NOT DISTINCT FROM sf.snowflake_dailyoptin               AS dailyoptin_match,
                s.sfmc_thirdpartyoptin,
                sf.snowflake_thirdpartyoptin,
                s.sfmc_thirdpartyoptin IS NOT DISTINCT FROM sf.snowflake_thirdpartyoptin     AS thirdpartyoptin_match,
                s.sfmc_reference,
                sf.snowflake_reference,
                s.sfmc_reference IS NOT DISTINCT FROM sf.snowflake_reference                 AS reference_match,
                s.sfmc_applicationdomain,
                sf.snowflake_applicationdomain,
                s.sfmc_applicationdomain IS NOT DISTINCT FROM sf.snowflake_applicationdomain AS applicationdomain_match
         FROM sfmc s
             FULL OUTER JOIN snowflake sf ON s.sfmc_userid = sf.snowflake_userid


     )
-- SELECT m.user_match,
--        COUNT(*)
-- FROM modelling m
-- -- WHERE email_match = FALSE
-- --    OR locale_match = FALSE
-- --    OR territory_match = FALSE
-- --    OR affiliate_id_match = FALSE
-- --    OR affiliate_brand_match = FALSE
-- --    OR date_joined_match = FALSE
-- --    OR forname_match = FALSE
-- --    OR surname_match = FALSE
-- --    OR weeklyoptin_match = FALSE
-- --    OR dailyoptin_match = FALSE
-- --    OR thirdpartyoptin_match = FALSE
-- GROUP BY 1
SELECT m.user_id,
       m.locale_match,
       m.sfmc_locale,
       m.snowflake_locale
FROM modelling m
    LEFT JOIN hygiene_snapshot_vault_mvp.cms_mysql.affiliate a ON m.sfmc_affiliateid = a.id
WHERE m.user_match = 'match user id'
  AND m.locale_match = FALSE
;


USE WAREHOUSE pipe_xlarge;

------------------------------------------------------------------------------------------------------------------------
-- email differences - ~9k - IGNORE
-- remove casing differences ~4.9K
-- checked the accounts looks like Snowflake email address is more up to date.

-- locale difference ~ 8K
-- inspected users, looks like the territory (which dictates locale) is based on the original affiliate

-- territory difference ~13K
-- inspected users, looks like the territory is based on the original affiliate assigned to the user

-- datejoined differences ~1.3M
-- looks like the SFMC date format for sign up is inconsistent, it appears to have parsed some dates with yyyy-mm-dd and others yyyy-dd-mm
-- for some time in history probably during a bulk upload

-- forename differences ~86K
-- forename is better populated in snowflake (lots of nulls in sfmc)
-- Also witnessed some trailing whitespaces

-- surname differences ~101K
-- surname is better populated in snowflake (lots of nulls in sfmc)
-- special characters aren't preserved in sfmc data
-- Also witnessed some trailing whitespaces

-- weekly opt in differences ~25M
-- DATA TEAM need to adjust the logic that computes weekly/daily the logic makes them mutually
-- exclusive where in actual fact a person can be both daily AND weekly.

-- daily opt in difference ~2.6M
-- DATA TEAM need to adjust the logic that computes weekly/daily the logic makes them mutually
-- exclusive where in actual fact a person can be both daily AND weekly.

-- third party opt in difference ~3M
-- verified with cms mysql profile table and snowflake is showing accurate numbers. SFMC data probably not updated


SELECT *
FROM se.data.se_user_attributes sua
WHERE sua.shiro_user_id = 73342748;

SELECT sua.shiro_user_id,
       sua.profile_id,
       p.receive_weekly_offers,
       p.receive_sales_reminders,
       p.receive_hand_picked_offers
FROM se.data.se_user_attributes sua
    INNER JOIN hygiene_snapshot_vault_mvp.cms_mysql.profile p ON sua.profile_id = p.id
WHERE sua.shiro_user_id = 28739203;


SELECT *
FROM hygiene_snapshot_vault_mvp.cms_mysql.profile p;


SELECT *
FROM data_vault_mvp.dwh.iterable__catalogue_product icp
WHERE icp.territory = 'US';


-- weekly opt in differences ~1M
-- checked these with production tables and snowflake represents what is being shown in production tables

-- daily opt in difference ~2.6M
-- DATA TEAM need to adjust the logic that computes weekly/daily the logic makes them mutually
-- exclusive where in actual fact a person can be both daily AND weekly.


------------------------------------------------------------------------------------------------------------------------

--No US users should exist in SFMC

SELECT *
FROM archive.sfmc.dim_customers t1
WHERE t1.territory = 'US';
--territory field in SFMC yes only shows 397 US users

SELECT t1.subscriberkey,
       t1.userid,
       t1.territory,
       ua.current_affiliate_id,
       ua.current_affiliate_name,
       ua.current_affiliate_territory_id,
       ua.current_affiliate_territory
FROM archive.sfmc.dim_customers t1
    INNER JOIN data_vault_mvp.dwh.user_attributes ua ON t1.userid = ua.shiro_user_id
WHERE ua.current_affiliate_territory = 'US';

self_describing_task --include 'dv/dwh/iterable/user_profile.py'  --method 'run' --start '2021-10-17 00:00:00' --end '2021-10-17 00:00:00'


SELECT COUNT(*)
FROM data_vault_mvp.dwh.iterable__user_profile iup;

SELECT ua.shiro_user_id,
       ua.email,


FROM data_vault_mvp.dwh.user_attributes ua
    INNER JOIN data_vault_mvp.dwh.iterable__user_profile iup ON ua.shiro_user_id = iup.shiro_user_id
WHERE ua.shiro_user_id IN ('70265830',
                           '26862738',
                           '20262043',
                           '67296834',
                           '74516636',
                           '24127472',
                           '10139016',
                           '63315179',
                           '33483868',
                           '23937646',
                           '65382962'
    );



