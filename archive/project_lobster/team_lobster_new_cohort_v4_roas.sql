SELECT
        COALESCE(sua.signup_tstamp, '')::DATE ||
        COALESCE(sua.original_affiliate_territory, '') ||
        COALESCE(sua.original_affiliate_id, 0) ||
        COALESCE(sua.original_affiliate_name, '') ||
        COALESCE(sua.member_original_affiliate_classification, '') AS id,
        sua.signup_tstamp::DATE                                    AS signup_date,
        sua.original_affiliate_territory,
        sua.original_affiliate_id,
        sua.original_affiliate_name,
        sua.member_original_affiliate_classification,
        COUNT(DISTINCT sua.shiro_user_id)                          AS members
FROM se.data.se_user_attributes sua
GROUP BY 1, 2, 3, 4, 5, 6;



SELECT
        COALESCE(sua.original_affiliate_territory, '') ||
        COALESCE(sua.original_affiliate_id, 0) ||
        COALESCE(sua.original_affiliate_name, '') ||
        COALESCE(sua.member_original_affiliate_classification, '') ||
        COALESCE(stmc.touch_mkt_channel, '') ||
        COALESCE(stmc.channel_category, '') ||
        COALESCE(fcb.booking_completed_date::DATE, '')      AS id,

        sua.original_affiliate_territory,
        sua.original_affiliate_id,
        sua.original_affiliate_name,
        sua.member_original_affiliate_classification,
        stmc.touch_mkt_channel,
        stmc.channel_category,
        fcb.booking_completed_date::DATE                    AS booking_date,
        SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp,
        COUNT(DISTINCT fcb.booking_id)                      AS bookings
FROM se.data.fact_complete_booking fcb
    INNER JOIN se.data.se_user_attributes sua ON fcb.shiro_user_id = sua.shiro_user_id
    INNER JOIN se.data.scv_touched_transactions stt ON fcb.booking_id = stt.booking_id
    INNER JOIN se.data.scv_touch_attribution sta ON stt.touch_id = sta.touch_id AND sta.attribution_model = 'last paid'
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id
WHERE fcb.booking_completed_date >= '2018-01-01'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
;

USE ROLE personal_role__cheansan;

USE WAREHOUSE pipe_default;


SELECT *
FROM data_vault_mvp.bi.cohort_v4_last_non_direct_bookings lndb;

SELECT *
FROM data_vault_mvp.bi.cohort_v4_member_signups ms;

SELECT *
FROM data_vault_mvp.bi.cohort_v4_last_paid_bookings lpb;

SELECT *
FROM data_vault_mvp.bi.cohort_v4_last_click_bookings lcb;


-- members
SELECT
    DATE_TRUNC(MONTH, ms.signup_date) AS signup_month,
    CASE
        WHEN ms.member_original_affiliate_classification
            IN (
                'PPC_NON_BRAND_CPA',
                'AFFILIATE_PROGRAM',
                'PAID_SOCIAL_CPA',
                'DISPLAY_CPA'
                 ) THEN 'CPA'
        WHEN ms.member_original_affiliate_classification
            IN (
                'DISPLAY_CPL',
                'PAID_SOCIAL_CPL',
                'PPC_NON_BRAND_CPL'
                 ) THEN 'CPL'
        ELSE 'Other'
        END                           AS affiliate_category_group,
    CASE
        WHEN ms.original_affiliate_territory IN ('TB_BE-FR', 'TB-BE_FR', 'BE', 'TB-BE_NL') THEN 'BE'
        WHEN ms.original_affiliate_territory IN ('DACH', 'DE') THEN 'DE'
        WHEN ms.original_affiliate_territory IN ('UK', 'Guardian - UK') THEN 'UK'
        WHEN ms.original_affiliate_territory IN ('CH', 'DK', 'IT', 'NO', 'SE') THEN ms.original_affiliate_territory
        WHEN ms.original_affiliate_territory IN ('TB-NL', 'NL') THEN 'NL'
        ELSE 'Other'
        END                           AS territory,
    SUM(ms.members)                   AS members
FROM data_vault_mvp.bi.cohort_v4_member_signups ms
WHERE ms.original_affiliate_territory NOT IN ('TL', 'PL')
GROUP BY 1, 2, 3;


/*SELECT DISTINCT
    CASE
        WHEN ms.original_affiliate_territory IN ('TB_BE-FR', 'TB-BE_FR', 'BE', 'TB-BE_NL') THEN 'BE'
        WHEN ms.original_affiliate_territory IN ('DACH', 'DE') THEN 'DE'
        WHEN ms.original_affiliate_territory IN ('UK', 'Guardian - UK') THEN 'UK'
        WHEN ms.original_affiliate_territory IN ('CH', 'DK', 'IT', 'NO', 'SE') THEN ms.original_affiliate_territory
        WHEN ms.original_affiliate_territory IN ('TB-NL', 'NL') THEN 'NL'
        ELSE 'Other'
        END AS territory,
    ms.original_affiliate_territory

FROM data_vault_mvp.bi.cohort_v4_member_signups ms;*/


-- bookings
-- scv model first then union with fact booking (booking status type = live)


SELECT
    DATE_TRUNC(MONTH, lpb.signup_date) AS signup_month,
    DATE_TRUNC(MONTH, lpb.event_date)  AS event_month,
    CASE
        WHEN lpb.booking_territory IN ('TB_BE-FR', 'TB-BE_FR', 'BE', 'TB-BE_NL') THEN 'BE'
        WHEN lpb.booking_territory IN ('DACH', 'DE') THEN 'DE'
        WHEN lpb.booking_territory IN ('TB-NL', 'NL') THEN 'NL'
        WHEN lpb.booking_territory IN ('CH', 'DK', 'IT', 'NO', 'SE', 'UK') THEN lpb.booking_territory
        ELSE 'Other'
        END                            AS territory,
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
        END                            AS affiliate_category_group,
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
        END                            AS channel,
    SUM(lpb.bookings)                  AS bookings,
    SUM(lpb.margin_gbp)                AS margin_gbp
FROM data_vault_mvp.bi.cohort_v4_last_click_bookings lpb
WHERE lpb.booking_territory NOT IN ('TL', 'PL')
GROUP BY 1, 2, 3, 4, 5

UNION ALL

SELECT
    DATE_TRUNC(MONTH, sua.signup_tstamp)                AS signup_month,
    DATE_TRUNC(MONTH, fcb.booking_completed_date)       AS event_month,
    CASE
        WHEN fcb.territory IN ('TB_BE-FR', 'TB-BE_FR', 'BE', 'TB-BE_NL') THEN 'BE'
        WHEN fcb.territory IN ('DACH', 'DE') THEN 'DE'
        WHEN fcb.territory IN ('TB-NL', 'NL') THEN 'NL'
        WHEN fcb.territory IN ('CH', 'DK', 'IT', 'NO', 'SE', 'UK') THEN fcb.territory
        ELSE 'Other'
        END                                             AS territory,
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
        END                                             AS affiliate_category_group,
    'Non-attributed'                                    AS channel,
    COUNT(DISTINCT fcb.booking_id)                      AS bookings,
    SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp
FROM se.data.fact_complete_booking fcb
    INNER JOIN se.data.se_user_attributes sua ON fcb.shiro_user_id = sua.shiro_user_id
WHERE fcb.territory NOT IN ('TL', 'PL')
  AND fcb.booking_completed_date < '2018-01-01'
GROUP BY 1, 2, 3, 4, 5;


------------------------------------------------------------------------------------------------------------------------
-- investigate margin discrepancy

WITH lobster_margin AS (
    SELECT
        DATE_TRUNC(MONTH, lpb.signup_date) AS signup_month,
        DATE_TRUNC(MONTH, lpb.event_date)  AS event_month,
        CASE
            WHEN lpb.booking_territory IN ('TB_BE-FR', 'TB-BE_FR', 'BE', 'TB-BE_NL') THEN 'BE'
            WHEN lpb.booking_territory IN ('DACH', 'DE') THEN 'DE'
            WHEN lpb.booking_territory IN ('TB-NL', 'NL') THEN 'NL'
            WHEN lpb.booking_territory IN ('CH', 'DK', 'IT', 'NO', 'SE', 'UK') THEN lpb.booking_territory
            ELSE 'Other'
            END                            AS territory,
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
            END                            AS affiliate_category_group,
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
            END                            AS channel,
        SUM(lpb.bookings)                  AS bookings,
        SUM(lpb.margin_gbp)                AS margin_gbp
    FROM data_vault_mvp.bi.cohort_v4_last_paid_bookings lpb
    WHERE lpb.booking_territory NOT IN ('TL', 'PL')
    GROUP BY 1, 2, 3, 4, 5

    UNION ALL

    SELECT
        DATE_TRUNC(MONTH, sua.signup_tstamp)                AS signup_month,
        DATE_TRUNC(MONTH, fcb.booking_completed_date)       AS event_month,
        CASE
            WHEN fcb.territory IN ('TB_BE-FR', 'TB-BE_FR', 'BE', 'TB-BE_NL') THEN 'BE'
            WHEN fcb.territory IN ('DACH', 'DE') THEN 'DE'
            WHEN fcb.territory IN ('TB-NL', 'NL') THEN 'NL'
            WHEN fcb.territory IN ('CH', 'DK', 'IT', 'NO', 'SE', 'UK') THEN fcb.territory
            ELSE 'Other'
            END                                             AS territory,
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
            END                                             AS affiliate_category_group,

        'Non-attributed'                                    AS channel,
        COUNT(DISTINCT fcb.booking_id)                      AS bookings,
        SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp
    FROM se.data.fact_complete_booking fcb
        INNER JOIN se.data.se_user_attributes sua ON fcb.shiro_user_id = sua.shiro_user_id
    WHERE fcb.territory NOT IN ('TL', 'PL')
      AND fcb.booking_completed_date < '2018-01-01'
    GROUP BY 1, 2, 3, 4, 5
)
SELECT
    lm.event_month,
    SUM(lm.margin_gbp)
FROM lobster_margin lm
GROUP BY 1;


--investigate the month of jul 22
SELECT
    lpb.event_date,
    SUM(lpb.margin_gbp) AS margin
FROM data_vault_mvp.bi.cohort_v4_last_paid_bookings lpb
WHERE lpb.booking_territory NOT IN ('TL', 'PL')
  AND DATE_TRUNC(MONTH, lpb.event_date) = '2022-07-01'
GROUP BY 1;


SELECT
    fcb.booking_completed_date,
    SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin
FROM se.data.fact_complete_booking fcb
WHERE fcb.territory NOT IN ('TL', 'PL')
  AND DATE_TRUNC(MONTH, fcb.booking_completed_date) = '2022-07-01'
  AND fcb.shiro_user_id IS NOT NULL
GROUP BY 1;


CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.lobster_bookings AS (
    SELECT
        sua.signup_tstamp::DATE          AS signup_date,
        fcb.shiro_user_id,
        sua.original_affiliate_territory,
        sua.original_affiliate_id,
        sua.original_affiliate_name,
        sua.member_original_affiliate_classification,
        stmc.touch_mkt_channel,
        fcb.booking_completed_date::DATE AS event_date,
        fcb.territory                    AS booking_territory,
        fcb.margin_gross_of_toms_gbp_constant_currency,
        fcb.booking_id
    FROM data_vault_mvp.dwh.fact_booking fcb
        INNER JOIN data_vault_mvp.dwh.user_attributes sua ON fcb.shiro_user_id = sua.shiro_user_id
        INNER JOIN data_vault_mvp.single_customer_view_stg.module_touched_transactions stt ON fcb.booking_id = stt.booking_id
        INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_attribution sta ON stt.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
        INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id
    WHERE fcb.booking_completed_date::DATE = '2022-07-30'
      AND fcb.booking_status_type = 'live'
);
--dbt model
SELECT
    SUM(lpb.margin_gbp) AS margin_gbp,
    SUM(lpb.bookings)   AS bookings
FROM data_vault_mvp.bi.cohort_v4_last_paid_bookings lpb
WHERE lpb.booking_territory NOT IN ('TL', 'PL')
  AND lpb.event_date = '2022-07-30';

--replicated dbt code in scratch
SELECT
    SUM(lb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp,
    COUNT(DISTINCT lb.booking_id)                      AS bookings
FROM scratch.robinpatel.lobster_bookings lb
WHERE lb.event_date = '2022-07-30';

--fact booking
SELECT
    SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp,
    COUNT(DISTINCT booking_id)                          AS bookings
FROM se.data.fact_complete_booking fcb
WHERE fcb.territory NOT IN ('TL', 'PL')
  AND fcb.booking_completed_date = '2022-07-30'
  AND fcb.shiro_user_id IS NOT NULL;


SELECT
    fcb.booking_id
FROM se.data.fact_complete_booking fcb
WHERE fcb.territory NOT IN ('TL', 'PL')
  AND fcb.booking_completed_date = '2022-07-30'
  AND fcb.shiro_user_id IS NOT NULL

EXCEPT

SELECT
    lb.booking_id
FROM scratch.robinpatel.lobster_bookings lb
WHERE lb.event_date = '2022-07-30';



CREATE OR REPLACE TABLE scratch.robinpatel.tableau_bookings
(
    booking_id VARCHAR,
    margin_gbp VARCHAR

);

USE SCHEMA scratch.robinpatel;

PUT file:///Users/robin/myrepos/sql_files/project_lobster/tableau_bookings_30072022.csv @%tableau_bookings;

COPY INTO scratch.robinpatel.tableau_bookings
    FILE_FORMAT = (
        TYPE = CSV
            FIELD_DELIMITER = ','
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
            RECORD_DELIMITER = '\\n'
        );

SELECT
    tableau_bookings.booking_id
FROM scratch.robinpatel.tableau_bookings

EXCEPT

SELECT
    lb.booking_id
FROM scratch.robinpatel.lobster_bookings lb
WHERE lb.event_date = '2022-07-30'
;


SELECT *
FROM se.data.fact_booking fb
WHERE fb.booking_id IN (
                        'A10279325',
                        '55452179',
                        'A10275311',
                        'A10282321',
                        'A10283300',
                        'A10284015',
                        'A10281857',
                        'A10283304',
                        'A10279946',
                        'A10283868',
                        'A10283770',
                        'A10279515',
                        'A10273984',
                        'A10279322',
                        'A10282491',
                        '55452407',
                        'A10275140',
                        '55451805',
                        'A10276778',
                        'A10277596'
    );



SELECT *
FROM se.data.scv_touched_transactions stt
WHERE stt.booking_id IN (
                         'A10279325',
                         '55452179',
                         'A10275311',
                         'A10282321',
                         'A10283300',
                         'A10284015',
                         'A10281857',
                         'A10283304',
                         'A10279946',
                         'A10283868',
                         'A10283770',
                         'A10279515',
                         'A10273984',
                         'A10279322',
                         'A10282491',
                         '55452407',
                         'A10275140',
                         '55451805',
                         'A10276778',
                         'A10277596'
    );


SELECT
    DATE_TRUNC(MONTH, lpb.event_date),
    SUM(lpb.margin_gbp) AS margin
FROM data_vault_mvp.bi.cohort_v4_last_paid_bookings lpb
WHERE lpb.booking_territory NOT IN ('TL', 'PL')
GROUP BY 1;


------------------------------------------------------------------------------------------------------------------------
--transaction model rec to fact booking
--https://eu-west-1a.online.tableau.com/t/secretescapes/authoring/lobster_reconciliation/Sheet12#1
--filters:
-- tech platform se and tb
-- shiro user id is present
-- territory doesn't include TL, PL
-- booking status type = live
-- reporting status = gross <-- overkill


--query to match on fact booking:
SELECT
    DATE_TRUNC(MONTH, fb.booking_completed_date),
    SUM(fb.margin_gross_of_toms_gbp_constant_currency) AS margin,
    COUNT(DISTINCT fb.booking_id)                      AS bookings
FROM se.data.fact_booking fb
WHERE fb.territory NOT IN ('TL', 'PL')
  AND fb.booking_status_type = 'live'
  AND fb.shiro_user_id IS NOT NULL
  AND fb.tech_platform IN ('SECRET_ESCAPES', 'TRAVELBIRD')
  AND fb.booking_completed_date >= '2018-01-01'
GROUP BY 1;

-- tableau matches fact booking

-- query on cohort model
SELECT
    DATE_TRUNC(MONTH, lpb.event_date),
    SUM(lpb.margin_gbp) AS margin,
    SUM(lpb.bookings)   AS bookings
FROM data_vault_mvp.bi.cohort_v4_last_paid_bookings lpb
WHERE lpb.booking_territory NOT IN ('TL', 'PL')
GROUP BY 1;

-- large variance for late 2019 indicating its something to do with covid canx jank
--code for last click from dbt
SELECT
        COALESCE(sua.original_affiliate_territory, '') ||
        COALESCE(sua.original_affiliate_id, 0) ||
        COALESCE(sua.original_affiliate_name, '') ||
        COALESCE(sua.member_original_affiliate_classification, '') ||
        COALESCE(stmc.touch_mkt_channel, '') ||
        COALESCE(fcb.booking_completed_date::DATE, '1970-01-01') ||
        COALESCE(sua.signup_tstamp::DATE, '1970-01-01') ||
        COALESCE(fcb.territory, '')                         AS id,

        sua.signup_tstamp::DATE                             AS signup_date,
        sua.original_affiliate_territory,
        sua.original_affiliate_id,
        sua.original_affiliate_name,
        sua.member_original_affiliate_classification,
        stmc.touch_mkt_channel,
        fcb.booking_completed_date::DATE                    AS event_date,
        fcb.territory                                       AS booking_territory,
        SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp,
        COUNT(DISTINCT fcb.booking_id)                      AS bookings
FROM data_vault_mvp.dwh.fact_booking fcb
    INNER JOIN data_vault_mvp.dwh.user_attributes sua ON fcb.shiro_user_id = sua.shiro_user_id
    INNER JOIN data_vault_mvp.single_customer_view_stg.module_touched_transactions stt ON fcb.booking_id = stt.booking_id
    INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel stmc ON stt.touch_id = stmc.touch_id
WHERE fcb.booking_completed_date >= '2018-01-01'
  AND fcb.booking_status_type = 'live'

GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9;

--adjusted dbt code to show monthly aggregates
SELECT
    DATE_TRUNC(MONTH, fcb.booking_completed_date)       AS booking_month,
    SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp,
    COUNT(DISTINCT fcb.booking_id)                      AS bookings
FROM data_vault_mvp.dwh.fact_booking fcb
    INNER JOIN data_vault_mvp.dwh.user_attributes sua ON fcb.shiro_user_id = sua.shiro_user_id
    INNER JOIN data_vault_mvp.single_customer_view_stg.module_touched_transactions stt ON fcb.booking_id = stt.booking_id
    INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel stmc ON stt.touch_id = stmc.touch_id
WHERE fcb.booking_completed_date >= '2018-01-01'
  AND fcb.booking_status_type = 'live'
  AND fcb.territory NOT IN ('TL', 'PL')
GROUP BY 1

-- it would appear that we don't have much scv tracking information for bookings over the late 2019 period
-- suggests that perhaps artificial insemination hasn't properly inserted bookings from that period

--fact bookings from dec 2019

SELECT
    fb.booking_id
FROM se.data.fact_booking fb
WHERE fb.territory NOT IN ('TL', 'PL')
  AND fb.booking_status_type = 'live'
  AND fb.shiro_user_id IS NOT NULL
  AND fb.tech_platform IN ('SECRET_ESCAPES', 'TRAVELBIRD')
  AND DATE_TRUNC(MONTH, fb.booking_completed_date) >= '2019-12-01'
GROUP BY 1;

WITH dec_2019_bookings AS (
    SELECT
        fb.booking_id
    FROM se.data.fact_booking fb
    WHERE fb.territory NOT IN ('TL', 'PL')
      AND fb.booking_status_type = 'live'
      AND fb.shiro_user_id IS NOT NULL
      AND fb.tech_platform IN ('SECRET_ESCAPES', 'TRAVELBIRD')
      AND DATE_TRUNC(MONTH, fb.booking_completed_date) >= '2019-12-01'
    GROUP BY 1
)
SELECT *
FROM dec_2019_bookings d
    LEFT JOIN data_vault_mvp.single_customer_view_stg.module_touched_transactions mtt ON d.booking_id = mtt.booking_id
WHERE mtt.booking_id IS NULL;



------------------------------------------------------------------------------------------------------------------------
-- category investigation
SELECT
    sa.affiliate_name,
    sa.category
FROM se.data.se_affiliate sa;

SELECT
    sua.original_affiliate_name,
    sua.member_original_affiliate_classification
FROM se.data.se_user_attributes sua;


SELECT
    sa.affiliate_name,
    COUNT(*)
FROM se.data.se_affiliate sa
WHERE sa.category = 'OTHER'
GROUP BY 1
;


SELECT
    sua.original_affiliate_name,
    sua.member_original_affiliate_classification,
    COUNT(*)
FROM se.data.se_user_attributes sua
GROUP BY 1, 2;


SELECT *
FROM se.data.se_user_attributes sua;


SELECT *
FROM data_vault_mvp.bi.cohort_v4_member_signups cv4ms

------------------------------------------------------------------------------------------------------------------------
--investigate high DE 'Other' signups in Jul 22

SELECT
    DATE_TRUNC(MONTH, ms.signup_date) AS signup_month,
    CASE
        WHEN ms.member_original_affiliate_classification
            IN (
                'PPC_NON_BRAND_CPA',
                'AFFILIATE_PROGRAM',
                'PAID_SOCIAL_CPA',
                'DISPLAY_CPA'
                 ) THEN 'CPA'
        WHEN ms.member_original_affiliate_classification
            IN (
                'DISPLAY_CPL',
                'PAID_SOCIAL_CPL',
                'PPC_NON_BRAND_CPL'
                 ) THEN 'CPL'
        ELSE 'Other'
        END                           AS affiliate_category_group,
    CASE
        WHEN ms.original_affiliate_territory IN ('TB_BE-FR', 'TB-BE_FR', 'BE', 'TB-BE_NL') THEN 'BE'
        WHEN ms.original_affiliate_territory IN ('DACH', 'DE') THEN 'DE'
        WHEN ms.original_affiliate_territory IN ('UK', 'Guardian - UK') THEN 'UK'
        WHEN ms.original_affiliate_territory IN ('CH', 'DK', 'IT', 'NO', 'SE') THEN ms.original_affiliate_territory
        WHEN ms.original_affiliate_territory IN ('TB-NL', 'NL') THEN 'NL'
        ELSE 'Other'
        END                           AS territory,
    SUM(ms.members)                   AS members
FROM data_vault_mvp.bi.cohort_v4_member_signups ms
WHERE ms.original_affiliate_territory = 'DE'
GROUP BY 1, 2, 3;

SELECT
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
        END AS affiliate_category_group,
    *
FROM se.data.se_user_attributes sua
WHERE DATE_TRUNC(MONTH, sua.signup_tstamp) = '2022-07-01'
  AND sua.original_affiliate_territory = 'DE'
  AND CASE
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
          END = 'Other'
;


SELECT
    sua.original_affiliate_name,
    sua.member_original_affiliate_classification,
    COUNT(*)
FROM se.data.se_user_attributes sua
WHERE DATE_TRUNC(MONTH, sua.signup_tstamp) = '2022-07-01'
  AND sua.original_affiliate_territory = 'DE'
  AND CASE
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
          END = 'Other'
GROUP BY 1, 2;



SELECT
    DATE_TRUNC(MONTH, sua.signup_tstamp)                AS signup_month,
    DATE_TRUNC(MONTH, fcb.booking_completed_date)       AS event_month,
    CASE
        WHEN fcb.territory IN ('TB_BE-FR', 'TB-BE_FR', 'BE', 'TB-BE_NL') THEN 'BE'
        WHEN fcb.territory IN ('DACH', 'DE') THEN 'DE'
        WHEN fcb.territory IN ('TB-NL', 'NL') THEN 'NL'
        WHEN fcb.territory IN ('CH', 'DK', 'IT', 'NO', 'SE', 'UK') THEN fcb.territory
        ELSE 'Other'
        END                                             AS territory,
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
        END                                             AS affiliate_category_group,
    'Non-attributed'                                    AS channel,
    COUNT(DISTINCT fcb.booking_id)                      AS bookings,
    SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp
FROM se.data.fact_complete_booking fcb
    INNER JOIN se.data.se_user_attributes sua ON fcb.shiro_user_id = sua.shiro_user_id
WHERE fcb.territory NOT IN ('TL', 'PL')
  AND (
            fcb.booking_completed_date < '2018-01-01'
        OR
            (fcb.tech_platform = 'TRAVELBIRD' AND fcb.booking_completed_date < '2022-03-01') -- to account for travelbird bookings before tracking
    )
GROUP BY 1, 2, 3, 4, 5;


SELECT
    DATE_TRUNC(MONTH, lpb.signup_date) AS signup_month,
    DATE_TRUNC(MONTH, lpb.event_date)  AS event_month,
    CASE
        WHEN lpb.booking_territory IN ('TB_BE-FR', 'TB-BE_FR', 'BE', 'TB-BE_NL') THEN 'BE'
        WHEN lpb.booking_territory IN ('DACH', 'DE') THEN 'DE'
        WHEN lpb.booking_territory IN ('TB-NL', 'NL') THEN 'NL'
        WHEN lpb.booking_territory IN ('CH', 'DK', 'IT', 'NO', 'SE', 'UK') THEN lpb.booking_territory
        ELSE 'Other'
        END                            AS territory,
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
        END                            AS affiliate_category_group,
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
        END                            AS channel,
    SUM(lpb.bookings)                  AS bookings,
    SUM(lpb.margin_gbp)                AS margin_gbp
FROM data_vault_mvp.bi.cohort_v4_last_click_bookings lpb
WHERE lpb.booking_territory NOT IN ('TL', 'PL')
GROUP BY 1, 2, 3, 4, 5

UNION ALL

SELECT
    DATE_TRUNC(MONTH, sua.signup_tstamp)                AS signup_month,
    DATE_TRUNC(MONTH, fcb.booking_completed_date)       AS event_month,
    CASE
        WHEN fcb.territory IN ('TB_BE-FR', 'TB-BE_FR', 'BE', 'TB-BE_NL') THEN 'BE'
        WHEN fcb.territory IN ('DACH', 'DE') THEN 'DE'
        WHEN fcb.territory IN ('TB-NL', 'NL') THEN 'NL'
        WHEN fcb.territory IN ('CH', 'DK', 'IT', 'NO', 'SE', 'UK') THEN fcb.territory
        ELSE 'Other'
        END                                             AS territory,
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
        END                                             AS affiliate_category_group,
    'Non-attributed'                                    AS channel,
    COUNT(DISTINCT fcb.booking_id)                      AS bookings,
    SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp
FROM se.data.fact_complete_booking fcb
    INNER JOIN se.data.se_user_attributes sua ON fcb.shiro_user_id = sua.shiro_user_id
WHERE fcb.territory NOT IN ('TL', 'PL')
  AND (
            fcb.booking_completed_date < '2018-01-01'
        OR
            (fcb.tech_platform = 'TRAVELBIRD' AND fcb.booking_completed_date < '2022-03-01') -- to account for travelbird bookings before tracking
    )
GROUP BY 1, 2, 3, 4, 5;

SELECT *
FROM data_vault_mvp.bi.cohort_v4_monthy_member_signups cv4mms;
SELECT *
FROM data_vault_mvp.bi.cohort_v4_monthy_last_click_bookings cv4mlcb;
SELECT *
FROM data_vault_mvp.bi.cohort_v4_monthy_last_paid_bookings cv4mlpb;
SELECT *
FROM data_vault_mvp.bi.cohort_v4_monthy_last_non_direct_bookings cv4mlndb;



SELECT
    cv4mlpb.event_month,
    SUM(cv4mlpb.margin_gbp)

FROM data_vault_mvp.bi.cohort_v4_monthy_last_paid_bookings cv4mlpb
GROUP BY 1

------------------------------------------------------------------------------------------------------------------------


SELECT
    sua.original_affiliate_name,
    sua.member_original_affiliate_classification,
    DATE_TRUNC(MONTH, sua.signup_tstamp) AS month,
    COUNT(*)
FROM se.data.se_user_attributes sua
WHERE CASE
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
          END = 'Other'
GROUP BY 1, 2, 3;



SELECT
    cv4mlpb.event_month,
    SUM(cv4mlpb.margin_gbp)
FROM data_vault_mvp.bi.cohort_v4_monthy_last_paid_bookings cv4mlpb
WHERE cv4mlpb.event_month >= '2018-01-01'
GROUP BY 1;


SELECT
    DATE_TRUNC(MONTH, lpb.event_date),
    SUM(lpb.margin_gbp) AS margin,
    SUM(lpb.bookings)   AS bookings
FROM data_vault_mvp.bi.cohort_v4_last_paid_bookings lpb
WHERE lpb.booking_territory NOT IN ('TL', 'PL')
GROUP BY 1;



SELECT
    DATE_TRUNC(MONTH, fcb.booking_completed_date)::DATE AS event_month,
    SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp
FROM data_vault_mvp.dwh.fact_booking fcb
    INNER JOIN data_vault_mvp.dwh.user_attributes sua ON fcb.shiro_user_id = sua.shiro_user_id
WHERE fcb.territory NOT IN ('TL', 'PL')
  AND fcb.booking_status_type = 'live'
  AND (
            fcb.booking_completed_date < '2018-01-01'
        OR
            (fcb.tech_platform = 'TRAVELBIRD' AND fcb.booking_completed_date < '2020-03-01') -- to account for travelbird bookings before tracking
    )
GROUP BY 1
LIMIT 500
/* limit added automatically by dbt cloud */


------------------------------------------------------------------------------------------------------------------------

--mau

--signup month
--event month
--original territory
--affiliate category group
--channel (attributed vs non attributed)

USE WAREHOUSE pipe_xlarge;

WITH user_agg AS (
    -- categorise sessions up to common grain by user
    SELECT
        mtba.attributed_user_id,
        IFF(ua.shiro_user_id IS NOT NULL, 'member', 'non member')                 AS member_status,
        DATE_TRUNC('month', COALESCE(ua.signup_tstamp, mtba.touch_start_tstamp))  AS signup_month,
        DATE_TRUNC('month', mtba.touch_start_tstamp)                              AS event_month,
        COALESCE(ua.original_affiliate_territory, mtmc.touch_affiliate_territory) AS session_territory,
        CASE
            WHEN session_territory IN ('TB_BE-FR', 'TB-BE_FR', 'BE', 'TB-BE_NL') THEN 'BE'
            WHEN session_territory IN ('DE', 'CH', 'AT') THEN 'BE'
            WHEN session_territory IN ('TB-NL', 'NL') THEN 'NL'
            WHEN session_territory IN ('CH', 'DK', 'IT', 'NO', 'SE', 'UK')
                THEN session_territory
            ELSE 'Other'
            END                                                                   AS user_session_territory,
        COALESCE(ua.original_affiliate_name, mtmc.touch_mkt_channel)              AS user_original_affiliate,
        CASE
            WHEN user_original_affiliate
                IN (
                    'PPC_NON_BRAND_CPA',
                    'AFFILIATE_PROGRAM',
                    'PAID_SOCIAL_CPA',
                    'DISPLAY_CPA',
                     --non member channels
                    'Affiliate Program',
                    'Display CPA',
                    'Paid Social CPA',
                    'PPC - Non Brand CPA'
                     ) THEN 'CPA'
            WHEN user_original_affiliate
                IN (
                    'DISPLAY_CPL',
                    'PAID_SOCIAL_CPL',
                    'PPC_NON_BRAND_CPL',
                     --non member channels
                    'Display CPL',
                    'Paid Social CPL',
                    'PPC - Non Brand CPL'
                     ) THEN 'CPL'
            ELSE 'Other'
            END                                                                   AS affiliate_category_group,
        CASE
            WHEN mtmc.touch_mkt_channel
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
            END                                                                   AS channel,
        COUNT(DISTINCT mtba.touch_id)                                             AS sessions
    FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
        INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc ON mtba.touch_id = mtmc.touch_id
        LEFT JOIN  data_vault_mvp.dwh.user_attributes ua ON TRY_TO_NUMBER(mtba.attributed_user_id) = ua.shiro_user_id AND mtba.stitched_identity_type = 'se_user_id'
--     WHERE mtba.touch_start_tstamp >= '2022-01-01' -- TODO remove
    WHERE DATE_TRUNC(MONTH, mtba.touch_start_tstamp) = DATE_TRUNC(MONTH, CURRENT_DATE - 1) -- incremental step
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),
     user_channel AS (
         -- aggregate session data up to user by common grain and calculate more prominent channel type
         SELECT
             ua.attributed_user_id,
             ua.member_status,
             ua.signup_month,
             ua.event_month,
             ua.user_session_territory,
             ua.affiliate_category_group,
             COALESCE(SUM(IFF(ua.channel = 'Attributed', ua.sessions, NULL)), 0)                 AS attributed_sessions,
             COALESCE(SUM(IFF(ua.channel = 'Non-attributed', ua.sessions, NULL)), 0)             AS non_attributed_sessions,
             --channel is based on most frequent type of sessions and favours attributed if equal
             IFF(attributed_sessions >= non_attributed_sessions, 'Attributed', 'Non-attributed') AS channel
         FROM user_agg ua
         GROUP BY 1, 2, 3, 4, 5, 6
     )
SELECT
        uc.signup_month ||
        uc.event_month ||
        uc.user_session_territory ||
        uc.affiliate_category_group ||
        uc.channel                                                                        AS id,
        uc.signup_month,
        uc.event_month,
        uc.user_session_territory,
        uc.affiliate_category_group,
        uc.channel,
        COUNT(DISTINCT IFF(uc.member_status = 'member', uc.attributed_user_id, NULL))     AS member_mau,
        COUNT(DISTINCT IFF(uc.member_status = 'non member', uc.attributed_user_id, NULL)) AS non_member_mau
FROM user_channel uc
GROUP BY 1, 2, 3, 4, 5, 6
;

USE ROLE personal_role__dbt_prod;
USE WAREHOUSE dbt_pipe_large;

SELECT * FROM data_vault_mvp.bi.cohort_v4_monthly_active_users;