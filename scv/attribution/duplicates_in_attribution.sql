WITH dedupe AS (
    SELECT mta.touch_id,
           mta.attribution_model,
           mta.updated_at
    FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution mta
        QUALIFY ROW_NUMBER() OVER (PARTITION BY mta.touch_id, mta.attribution_model ORDER BY mta.updated_at DESC) != 1
)
SELECT attribution_model,
       COUNT(*)
FROM dedupe
GROUP BY 1
;
USE WAREHOUSE pipe_xlarge;


-- 43197980 dupes in attribution

SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touch_attribution mta
    QUALIFY COUNT(*) OVER (PARTITION BY mta.touch_id, mta.attribution_model) > 1
ORDER BY touch_id;

-- attribution model numbers don't match
SELECT attribution_model,
       COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_attribution mta
GROUP BY 1;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution_20211213 CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution;

DELETE
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution_20211213 AS target USING (
    SELECT mta.touch_id,
           mta.updated_at
    FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution_20211213 mta
        QUALIFY ROW_NUMBER() OVER (PARTITION BY mta.touch_id, mta.attribution_model ORDER BY mta.updated_at DESC) != 1
) AS batch
WHERE target.touch_id = batch.touch_id
  AND target.updated_at = batch.updated_at
;

-- attribution model numbers match
SELECT attribution_model,
       COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution_20211213 mta
GROUP BY 1;

------------------------------------------------------------------------------------------------------------------------
--rose's query
SELECT *
FROM se.data.scv_touched_transactions stt
    INNER JOIN se.data.scv_touch_attribution sta
               ON stt.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id
WHERE booking_id IN ('A3882823', 'A3493891', 'A3851332')
ORDER BY booking_id;

--rose's query after deletion
SELECT *
FROM se.data.scv_touched_transactions stt
    INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution sta ON stt.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id
WHERE stt.booking_id IN ('A3882823', 'A3493891', 'A3851332')
ORDER BY booking_id;

SELECT *
FROM data_vault_mvp.dwh.user_attributes ua;

self_describing_task --include 'dv/dwh/iterable/user_profile.py'  --method 'run' --start '2021-10-20 00:00:00' --end '2021-10-20 00:00:00'

SELECT COUNT(DISTINCT shiro_user_id)
FROM data_vault_mvp.dwh.user_recent_activities ura
WHERE ura.last_purchase_tstamp >= CURRENT_DATE - 30;

SELECT PARSE_JSON('{
    "email": "ben.deavin+10-22_1@secretescapes.com",
    "dataFields":
    {
        "accountStatus": "FULL_ACCOUNT",
        "reference": "1e80d432ede44252",
        "locale": "en_GB",
        "territory": "en",
        "territoryRegion": "UK",
        "affiliateId": 24,
        "mainAffiliateId": 24,
        "affiliateBrand": "Secret Escapes",
        "applicationDomain": "www.secretescapes.com",
        "dateJoined": "2021-09-27T13:11:42.000+0000",
        "weeklyOptIn": true,
        "dailyOptIn": true,
        "thirdPartyOptIn": true,
        "pauseEndTimestamp": "2021-12-21T15:18:46.490+0000",
        "subscriptionModifiedTimestamp": "2021-09-27T13:11:42.000+0000",
        "title": "Mr",
        "forename": "Test",
        "surname": "User",
        "country": "UK",
        "region": "Surrey",
        "invitationShareLink": "https://www.secretescapes.com/r/6",
        "referredBy": "8",
        "dateModified": "2021-09-27T13:11:42.000+0000",
        "acquisitionPlatform": "WEB",
        "userActivity": {
                "updatedAt": "2021-10-12 04:37:15 +00:00",
                "lastEmailOpenTstamp": "2021-10-11 00:00:00 +00:00",
                "lastEmailClickTstamp": "2021-10-11 00:00:00 +00:00",
                "lastPurchaseTstamp": "2021-08-14 15:16:58 +00:00",
                "lastSpvTstamp": "2021-10-11 13:14:23 +00:00",
                "dailySpvDeals": [
                        "A17654",
                        "A38459"
                ],
                "weeklySpvDeals": [
                        "A17654",
                        "A38459"
                ],
                "outgoingRunTstamp": "2021-10-12 15:43:52 +00:00",
                "outgoingScheduleTstamp": "2021-10-12 03:00:00 +00:00"
        }
    },
    "userId": "75894771",
    "preferUserId": true,
    "mergeNestedObjects": true
}') {
    "email": "ben.deavin+10-22_1@secretescapes.com",
    "accountStatus": "FULL_ACCOUNT",
    "reference": "1e80d432ede44252",
    "locale": "en_GB",
    "territory": "en",
    "territoryRegion": "UK",
    "affiliateId": 24,
    "mainAffiliateId": 24,
    "affiliateBrand": "Secret Escapes",
    "applicationDomain": "www.secretescapes.com",
    "dateJoined": "2021-09-27T13:11:42.000+0000",
    "weeklyOptIn": TRUE,
    "dailyOptIn": TRUE,
    "thirdPartyOptIn": TRUE,
    "pauseEndTimestamp": "2021-12-21T15:18:46.490+0000",
    "subscriptionModifiedTimestamp": "2021-09-27T13:11:42.000+0000",
    "title": "Mr",
    "forename": "Test",
    "surname": "User",
    "country": "UK",
    "region": "Surrey",
    "invitationShareLink": "https://www.secretescapes.com/r/6",
    "referredBy": "8",
    "dateModified": "2021-09-27T13:11:42.000+0000",
    "acquisitionPlatform": "WEB",
    "userId": "75894771",
    "preferUserId": TRUE,
    "mergeNestedObjects": TRUE
}

SELECT *
FROM unload_vault_mvp.iterable.user_profile_historical_preprod__20211023t030000__daily_at_03h00 u;


{
  "dataFields": {
    "accountStatus": "FULL_ACCOUNT",
    "acquisitionPlatform": "UNKNOWN",
    "affiliateBrand": "LateLuxury.com",
    "affiliateId": 817,
    "applicationDomain": "www.lateluxury.com",
    "dailyOptIn": FALSE,
    "dateJoined": "2015-01-03 03:10:08 +00:00",
    "dateModified": "2021-10-24 07:25:41 +00:00",
    "locale": "en_GB",
    "mainAffiliateId": 817,
    "outgoingRunTstamp": "2021-10-24 07:25:40 +00:00",
    "outgoingScheduleTstamp": "2021-10-23 03:00:00 +00:00",
    "reference": "50d01ee51445188e",
    "territory": "en",
    "territoryRegion": "UK",
    "thirdPartyOptIn": FALSE,
    "weeklyOptIn": FALSE
  },
  "email": "rooneyearle54@hotmail.com",
  "mergeNestedObjects": TRUE,
  "preferUserId": TRUE,
  "userId": "15932805"
}

SELECT iup.main_affiliate_id, iup.affiliate_domain, COUNT(*)
FROM data_vault_mvp.dwh.iterable__user_profile iup
GROUP BY 1, 2;

SELECT icp.is_hidden_for_whitelabels
FROM data_vault_mvp.dwh.iterable__catalogue_product icp;


SELECT *
FROM latest_vault.se_api.sales_kingfisher sk;

SELECT *
FROM collab.iterable_data.iterable__user_profile__bulk iupb


SELECT GET_DDL('table', 'collab.iterable_data.iterable__user_profile__bulk');



CREATE OR REPLACE VIEW collab.iterable_data.iterable__user_profile__bulk COPY GRANTS AS
SELECT iup.shiro_user_id,
       iup.email_address,
       iup.membership_account_status,
       iup.reference,
       iup.locale,
       iup.territory,
       iup.territory_region,
       iup.affiliate_id,
       iup.main_affiliate_id,
       iup.affiliate_brand,
       iup.affiliate_domain,
       iup.signup_tstamp,
       iup.weekly_opt_in,
       iup.daily_opt_in,
       iup.third_party_optin,
       iup.pause_subscription_end_tstamp,
       iup.title,
       iup.first_name,
       iup.surname,
       iup.region,
       iup.country,
       iup.referrer_id,
       iup.last_email_open_tstamp,
       iup.last_email_click_tstamp,
       iup.last_sale_pageview_tstamp,
       iup.last_purchase_tstamp,
       iup.daily_spv_deals,
       iup.weekly_spv_deals,
       iup.acquisition_platform
FROM data_vault_mvp.dwh.iterable__user_profile iup;

SELECT *
FROM se.data.se_user_attributes sua;
\


------------------------------------------------------------------------------------------------------------------------
--to delete dupes in attribution:
USE WAREHOUSE pipe_large;
DELETE
FROM data_vault_mvp.single_customer_view_stg.module_touch_attribution AS target USING (
    SELECT mta.touch_id,
           mta.updated_at
    FROM data_vault_mvp.single_customer_view_stg.module_touch_attribution mta
        QUALIFY ROW_NUMBER() OVER (PARTITION BY mta.touch_id, mta.attribution_model ORDER BY mta.updated_at DESC) != 1
) AS batch
WHERE target.touch_id = batch.touch_id
  AND target.updated_at = batch.updated_at
;

SELECT sta.attribution_model,
       COUNT(*)
FROM se.data.scv_touch_attribution sta
GROUP BY 1
;