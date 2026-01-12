SELECT
    YEAR(cv4mlpb.event_month) AS event_year,
    cv4mlpb.affiliate_category_group,
    cv4mlpb.channel,
    SUM(cv4mlpb.margin_gbp)
FROM data_vault_mvp.bi.cohort_v4_monthy_last_paid_bookings cv4mlpb
WHERE cv4mlpb.signup_month = cv4mlpb.event_month
GROUP BY 1, 2, 3
;


SELECT
    CASE
        WHEN lpb.member_original_affiliate_classification
            IN (
                'PPC_NON_BRAND_CPA',
                'AFFILIATE_PROGRAM',
                'PAID_SOCIAL_CPA',
                'DISPLAY_CPA'
                 ) THEN 'CPA'
        WHEN lpb.member_original_affiliate_classification
            IN (
                'DISPLAY_CPL',
                'PAID_SOCIAL_CPL',
                'PPC_NON_BRAND_CPL'
                 ) THEN 'CPL'
        ELSE 'Other'
        END                          AS affiliate_category_group,

    CASE
        WHEN lpb.touch_mkt_channel
            IN (
                'Display CPA',
                'Display CPL',
                'Affiliate Program',
                'Paid Social CPA',
                'Paid Social CPL',
                'PPC - Non Brand CPA',
                'PPC - Non Brand CPL'
                 ) THEN 'Attributed'
        ELSE 'Non-attributed'
        END                          AS channel,
    lpb.touch_mkt_channel,
    DATE_TRUNC(YEAR, lpb.event_date) AS event_year,
    SUM(lpb.margin_gbp)              AS margin_gbp
FROM data_vault_mvp.bi.cohort_v4_last_paid_bookings lpb
WHERE DATE_TRUNC(MONTH, lpb.signup_date) = DATE_TRUNC(MONTH, lpb.event_date)
GROUP BY 1, 2, 3, 4;


SELECT *
FROM data_vault_mvp.bi.cohort_v4_monthy_member_signups;
SELECT *
FROM data_vault_mvp.bi.cohort_v4_monthy_last_paid_bookings cv4mlpb;

SELECT *
FROM dbt_dev.dbt_robinpatel.cohort_v4_monthly_active_users;
------------------------------------------------------------------------------------------------------------------------

-- investigate why CPA and CPL transactions for 1M users are being attributed to non paid channels

USE WAREHOUSE pipe_xlarge;

SELECT
    sua.signup_tstamp::DATE                        AS signup_date,
    sua.signup_tstamp,
    sua.original_affiliate_territory,
    sua.original_affiliate_id,
    sua.original_affiliate_name,
    CASE
        WHEN sua.member_original_affiliate_classification
            IN (
                'PPC_NON_BRAND_CPA',
                'AFFILIATE_PROGRAM',
                'PAID_SOCIAL_CPA',
                'DISPLAY_CPA'
                 ) THEN 'CPA'
        WHEN sua.member_original_affiliate_classification
            IN (
                'DISPLAY_CPL',
                'PAID_SOCIAL_CPL',
                'PPC_NON_BRAND_CPL'
                 ) THEN 'CPL'
        ELSE 'Other'
        END                                        AS affiliate_category_group,
    sua.member_original_affiliate_classification,
    CASE
        WHEN stmc.touch_mkt_channel
            IN (
                'Display CPA',
                'Display CPL',
                'Affiliate Program',
                'Paid Social CPA',
                'Paid Social CPL',
                'PPC - Non Brand CPA',
                'PPC - Non Brand CPL'
                 ) THEN 'Performance Marketing'
        ELSE 'Non-Performance Marketing'
        END                                        AS channel,
    stmc.touch_mkt_channel,
    stmc.touch_landing_page,
    stmc.touch_hostname,
    stmc.touch_hostname_territory,
    stmc.attributed_user_id,
    stmc.utm_campaign,
    stmc.utm_medium,
    stmc.utm_source,
    stmc.utm_term,
    stmc.utm_content,
    stmc.click_id,
    stmc.sub_affiliate_name,
    stmc.affiliate,
    stmc.landing_page_parameters['affiliateUrlString']::VARCHAR AS affiliate_url_string,
    stmc.touch_affiliate_territory,
    stmc.awadgroupid,
    stmc.awcampaignid,
    stmc.referrer_hostname,
    stmc.referrer_medium,
    stmc.landing_page_parameters,
    stt.event_tstamp,
    fcb.booking_completed_date::DATE               AS event_date,
    fcb.booking_completed_timestamp,
    fcb.territory                                  AS booking_territory,
    fcb.margin_gross_of_toms_gbp_constant_currency AS margin_gbp
FROM data_vault_mvp.dwh.fact_booking fcb
    INNER JOIN data_vault_mvp.dwh.user_attributes sua ON fcb.shiro_user_id = sua.shiro_user_id
    INNER JOIN data_vault_mvp.single_customer_view_stg.module_touched_transactions stt ON fcb.booking_id = stt.booking_id
    INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_attribution sta ON stt.touch_id = sta.touch_id AND sta.attribution_model = 'last paid'
    INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id
WHERE fcb.booking_completed_date >= '2022-01-01'
  AND fcb.booking_status_type = 'live'
  AND DATE_TRUNC(MONTH, sua.signup_tstamp) = DATE_TRUNC(MONTH, fcb.booking_completed_date) --1m users
  AND affiliate_category_group IS DISTINCT FROM 'Other'
  AND channel = 'Non-Performance Marketing'
AND stmc.touch_mkt_channel NOT IN ('PPC - Brand', 'PPC - Undefined')
;