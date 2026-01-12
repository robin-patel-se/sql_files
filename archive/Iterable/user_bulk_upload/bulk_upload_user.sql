ALTER SESSION SET TIMESTAMP_TZ_OUTPUT_FORMAT = 'yyyy-mm-dd hh24:mi:ss TZH:TZM';

SELECT iup.membership_account_status                                                  AS "accountStatus",
       NULL                                                                           AS "acquisitionPlatform",
       iup.affiliate_brand                                                            AS "affiliateBrand",
       iup.affiliate_id                                                               AS "affiliateId",
       iup.affiliate_domain                                                           AS "applicationDomain",
       iup.country                                                                    AS "country",
       iup.daily_opt_in                                                               AS "dailyOptIn",
       TO_VARCHAR(iup.signup_tstamp, 'YYYY-MM-DD HH24:MI:SS TZH:TZM')                 AS "dateJoined",
       TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS TZH:TZM')                 AS "dateModified",
       NULL                                                                           AS "devices",
       NULL                                                                           AS "devices.appBuild",
       NULL                                                                           AS "devices.appPackageName",
       NULL                                                                           AS "devices.appVersion",
       NULL                                                                           AS "devices.applicationName",
       NULL                                                                           AS "devices.deviceId",
       NULL                                                                           AS "devices.endpointEnabled",
       NULL                                                                           AS "devices.iterableSdkVersion",
       NULL                                                                           AS "devices.platform",
       NULL                                                                           AS "devices.platformEndpoint",
       NULL                                                                           AS "devices.token",
       iup.email_address                                                              AS "email",
       NULL                                                                           AS "emailListIds",
       iup.first_name                                                                 AS "forename",
       NULL                                                                           AS "invitationShareLink",
       NULL                                                                           AS "itblInternal.emailDomain",
       iup.locale                                                                     AS "locale",
       TO_VARCHAR(iup.pause_subscription_end_tstamp, 'YYYY-MM-DD HH24:MI:SS TZH:TZM') AS "pauseEndTimestamp",
       NULL                                                                           AS "phoneNumber",
       NULL                                                                           AS "phoneNumberDetails",
       NULL                                                                           AS "phoneNumberDetails.carrier",
       NULL                                                                           AS "phoneNumberDetails.countryCodeISO",
       NULL                                                                           AS "phoneNumberDetails.lineType",
       NULL                                                                           AS "phoneNumberDetails.updatedAt",
       TO_VARCHAR(p.last_updated, 'YYYY-MM-DD HH24:MI:SS TZH:TZM')                    AS "profileUpdatedAt",
       iup.reference                                                                  AS "reference",
       iup.referrer_id                                                                AS "referredBy",
       iup.region                                                                     AS "region",
       TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS TZH:TZM')                 AS "signupDate", -- when the user was added to iterable
       NULL                                                                           AS "signupSource",
       NULL                                                                           AS "subscribedMessageTypeIds",
       NULL                                                                           AS "subscriptionModifiedTimestamp",
       iup.surname                                                                    AS "surname",
       iup.territory                                                                  AS "territory",
       iup.territory_region                                                           AS "territoryRegion",
       NULL                                                                           AS "testingAaccountStatus",
       NULL                                                                           AS "testingAacquisitionPlatform",
       NULL                                                                           AS "testingAaffiliateBrand",
       NULL                                                                           AS "testingAaffiliateId",
       NULL                                                                           AS "testingAapplicationDomain",
       NULL                                                                           AS "testingAcountry",
       NULL                                                                           AS "testingAdailyOptIn",
       NULL                                                                           AS "testingAdateJoined",
       NULL                                                                           AS "testingAdateModified",
       NULL                                                                           AS "testingAemail",
       NULL                                                                           AS "testingAinvitationShareLink",
       NULL                                                                           AS "testingAlocale",
       NULL                                                                           AS "testingApreviousEmail",
       NULL                                                                           AS "testingAreference",
       NULL                                                                           AS "testingAsubscriptionModifiedTimestamp",
       NULL                                                                           AS "testingAterritory",
       NULL                                                                           AS "testingAterritoryRegion",
       NULL                                                                           AS "testingAthirdPartyOptIn",
       NULL                                                                           AS "testingAuserId",
       NULL                                                                           AS "testingAweeklyOptIn",
       iup.third_party_optin                                                          AS "thirdpartyoptin",
       iup.title                                                                      AS "title",
       NULL                                                                           AS "unsubscribedChannelIds",
       NULL                                                                           AS "unsubscribedMessageTypeIds",
       iup.shiro_user_id                                                              AS "userId",
       iup.weekly_opt_in                                                              AS "weeklyOptIn"
FROM data_vault_mvp.dwh.iterable__user_profile iup
    LEFT JOIN data_vault_mvp.dwh.user_attributes ua ON iup.shiro_user_id = ua.shiro_user_id
    LEFT JOIN hygiene_snapshot_vault_mvp.cms_mysql.profile p ON ua.profile_id = p.id
WHERE ua.membership_account_status IS NOT DISTINCT FROM 'FULL_ACCOUNT'
LIMIT 4000000;
-- WHERE iup.email_address =
--       'robin.patel@secretescapes.com'
--       'ben.deavin@secretescapes.com';
;
USE WAREHOUSE pipe_2xlarge;

SELECT DISTINCT membership_account_status
FROM data_vault_mvp.dwh.user_attributes ua;

------------------------------------------------------------------------------------------------------------------------
-- TO_VARCHAR(event_tstamp, 'YYYY-MM-DD HH24:MI:SS TZH:TZM')
;
SELECT *
FROM raw_vault_mvp.cms_mysql.profile p;

SELECT *
FROM se.data_pii.se_user_attributes sua
WHERE sua.shiro_user_id = 74618746;
SELECT *
FROM raw_vault_mvp.cms_mysql.profile p
WHERE id = 74733616;


SELECT TO_TIMESTAMP_TZ('04/05/2013 01:02:03', 'dd/mm/yyyy hh24:mi:ss');

SELECT iup.shiro_user_id,
       iup.email_address,
       su.password_hash,
       iup.reference
FROM data_vault_mvp.dwh.iterable__user_profile iup
    INNER JOIN hygiene_snapshot_vault_mvp.cms_mysql.shiro_user su ON iup.shiro_user_id = su.id
WHERE iup.shiro_user_id IN
      ('69203857',
       '57328696',
       '66218664',
       '20922280',
       '25952443',
       '11202545',
       '59480018',
       '57033713',
       '38953955',
       '47311303');

SELECT LENGTH('3fb496ad3acd813e');

------------------------------------------------------------------------------------------------------------------------
--with current formatting it looks like we can achieve approximately 6.5M user rows within the 2GB upload limit

SELECT iup.shiro_user_id,
       iup.email_address,
       su.password_hash,
       iup.reference,
       RIGHT(LEFT(SHA2(COALESCE(su.password_hash, '') || COALESCE(iup.email_address, ''), 256), 21), 16)                       AS reference1,
       RIGHT(LEFT(SHA2(IFF(su.password_hash IS NULL, iup.email_address, su.password_hash || iup.email_address), 256), 21), 16) AS reference2
FROM data_vault_mvp.dwh.iterable__user_profile iup
    INNER JOIN hygiene_snapshot_vault_mvp.cms_mysql.shiro_user su ON iup.shiro_user_id = su.id
WHERE iup.shiro_user_id IN
      ('69203857',
       '57328696',
       '66218664',
       '20922280',
       '25952443',
       '11202545',
       '59480018',
       '57033713',
       '38953955',
       '47311303');


------------------------------------------------------------------------------------------------------------------------
SELECT t1.userid::INT AS user_id,
       t1.email,
       t1.locale,
       t1.territory,
       t1.affiliateid,
       t1.affiliatebrand,
       t1.datejoined,
       t1.forename,
       t1.surname
FROM archive.sfmc.dim_customers t1
MINUS
SELECT t2.shiro_user_id   AS userid,
       t2.email_address   AS email,
       t2.locale,
       t2.territory,
       t2.affiliate_id    AS affiliateid,
       t2.affiliate_brand AS affiliatebrand,
       t2.signup_tstamp   AS datejoined,
       t2.first_name      AS forename,
       t2.surname
FROM collab.iterable_data.iterable__user_profile__bulk t2;


WITH sfmc AS (
    SELECT t1.userid::INT    AS sfmc_userid,
           t1.email          AS sfmc_email,
           t1.locale         AS sfmc_locale,
           t1.territory      AS sfmc_territory,
           t1.affiliateid    AS sfmc_affiliateid,
           t1.affiliatebrand AS sfmc_affiliatebrand,
           t1.datejoined     AS sfmc_datejoined,
           t1.forename       AS sfmc_forename,
           t1.surname        AS sfmc_surname
    FROM archive.sfmc.dim_customers t1
),
     snowflake AS (
         SELECT t2.shiro_user_id   AS snowflake_userid,
                t2.email_address   AS snowflake_email,
                t2.locale          AS snowflake_locale,
                t2.territory       AS snowflake_territory,
                t2.affiliate_id    AS snowflake_affiliateid,
                t2.affiliate_brand AS snowflake_affiliatebrand,
                t2.signup_tstamp   AS snowflake_datejoined,
                t2.first_name      AS snowflake_forename,
                t2.surname         AS snowflake_surname
         FROM collab.iterable_data.iterable__user_profile__bulk t2
     )

SELECT COALESCE(s.sfmc_userid, sf.snowflake_userid)                           AS user_id,
       s.sfmc_email,
       sf.snowflake_email,
       s.sfmc_email IS NOT DISTINCT FROM sf.snowflake_email                   AS email_match,
       s.sfmc_locale,
       sf.snowflake_locale,
       s.sfmc_locale IS NOT DISTINCT FROM sf.snowflake_locale                 AS locale_match,
       s.sfmc_territory,
       sf.snowflake_territory,
       s.sfmc_territory IS NOT DISTINCT FROM sf.snowflake_territory           AS territory_match,
       s.sfmc_affiliateid,
       sf.snowflake_affiliateid,
       s.sfmc_affiliateid IS NOT DISTINCT FROM sf.snowflake_affiliateid       AS affiliate_id_match,
       s.sfmc_affiliatebrand,
       sf.snowflake_affiliatebrand,
       s.sfmc_affiliatebrand IS NOT DISTINCT FROM sf.snowflake_affiliatebrand AS affiliate_brand_match,
       s.sfmc_datejoined,
       sf.snowflake_datejoined,
       s.sfmc_datejoined IS NOT DISTINCT FROM sf.snowflake_datejoined         AS date_joined_match,
       s.sfmc_forename,
       sf.snowflake_forename,
       s.sfmc_forename IS NOT DISTINCT FROM sf.snowflake_forename             AS forname_match,
       s.sfmc_surname,
       sf.snowflake_surname,
       s.sfmc_surname IS NOT DISTINCT FROM sf.snowflake_surname               AS surname_match
FROM sfmc s
    FULL OUTER JOIN snowflake sf ON s.sfmc_userid = sf.snowflake_userid
WHERE email_match = FALSE
   OR locale_match = FALSE
   OR territory_match = FALSE
   OR affiliate_id_match = FALSE
   OR affiliate_brand_match = FALSE
   OR date_joined_match = FALSE
   OR forname_match = FALSE
   OR surname_match = FALSE;

WITH sfmc AS (
    SELECT t1.userid::INT    AS sfmc_userid,
           t1.email          AS sfmc_email,
           t1.locale         AS sfmc_locale,
           t1.territory      AS sfmc_territory,
           t1.affiliateid    AS sfmc_affiliateid,
           t1.affiliatebrand AS sfmc_affiliatebrand,
           t1.datejoined     AS sfmc_datejoined,
           t1.forename       AS sfmc_forename,
           t1.surname        AS sfmc_surname
    FROM archive.sfmc.dim_customers t1
),
     snowflake AS (
         SELECT t2.shiro_user_id   AS snowflake_userid,
                t2.email_address   AS snowflake_email,
                t2.locale          AS snowflake_locale,
                t2.territory       AS snowflake_territory,
                t2.affiliate_id    AS snowflake_affiliateid,
                t2.affiliate_brand AS snowflake_affiliatebrand,
                t2.signup_tstamp   AS snowflake_datejoined,
                t2.first_name      AS snowflake_forename,
                t2.surname         AS snowflake_surname
         FROM collab.iterable_data.iterable__user_profile__bulk t2
     ), model AS (

    SELECT COALESCE(s.sfmc_userid, sf.snowflake_userid) AS user_id,
           CASE
               WHEN s.sfmc_userid IS NOT NULL AND sf.snowflake_userid IS NULL THEN 'in sfmc not in snowflake'
               WHEN s.sfmc_userid IS NULL AND sf.snowflake_userid IS NOT NULL THEN 'in snowflake not in sfmc'
               ELSE 'match user id'
               END                                      AS user_match,
           s.sfmc_email,
           sf.snowflake_email,
           s.sfmc_locale,
           sf.snowflake_locale,
           s.sfmc_territory,
           sf.snowflake_territory,
           s.sfmc_affiliateid,
           sf.snowflake_affiliateid,
           s.sfmc_affiliatebrand,
           sf.snowflake_affiliatebrand,
           s.sfmc_datejoined,
           sf.snowflake_datejoined,
           s.sfmc_forename,
           sf.snowflake_forename,
           s.sfmc_surname,
           sf.snowflake_surname

    FROM sfmc s
        FULL OUTER JOIN snowflake sf ON s.sfmc_userid = sf.snowflake_userid
)
SELECT user_match,
       count(*)
FROM model
GROUP BY 1;
