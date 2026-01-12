WITH opt_in_status AS (
    SELECT
        shiro_user_id,
        signup_tstamp :: DATE,
        SUM(CASE WHEN email_opt_in = 2 THEN 1 ELSE 0 END) AS weekly,
        SUM(CASE WHEN email_opt_in = 1 THEN 1 ELSE 0 END) AS daily,
        SUM(CASE WHEN email_opt_in = 0 THEN 1 ELSE 0 END) AS opted_out

    FROM se.data.se_user_attributes
    WHERE signup_tstamp :: DATE BETWEEN '2022-04-01' AND '2022-04-27'
    GROUP BY 1, 2
)

SELECT
    ua.signup_tstamp :: DATE AS date,
    ua.original_affiliate_id,
    ua.original_affiliate_name,
    sa.url_string            AS utm_content,
    ua.original_affiliate_territory_id,
    ua.current_affiliate_territory,
    COUNT(ua.shiro_user_id)  AS signups,
    SUM(os.weekly)           AS weekly,
    SUM(os.daily)            AS daily,
    SUM(os.opted_out)        AS opted_out

FROM se.data.se_user_attributes ua
    INNER JOIN se.data.se_affiliate sa ON ua.original_affiliate_id = sa.id
    INNER JOIN opt_in_status os
               ON os.shiro_user_id = ua.shiro_user_id
GROUP BY 1, 2, 3, 4, 5, 6;

SELECT *
FROM se.data.se_affiliate sa;

USE WAREHOUSE pipe_xlarge;

SELECT
    TO_VARCHAR(date, 'YYYY/MM/DD')                  AS date,
    utm_campaign,
    utm_content,
    touch_affiliate_territory,
    SUM(transactions)                               AS transactions,
    SUM(margin_gross_of_toms_gbp_constant_currency) AS margin
FROM se.bi.external_affiliate_metrics
WHERE utm_campaign IN ('clicktripz_de_cpl', 'clicktripz_uk_cpl')
  AND touch_affiliate_territory IN ('UK', 'DE')
  AND date >= CURRENT_DATE - 180
GROUP BY 1, 2, 3, 4;

SELECT *
FROM se.data.se_affiliate sa
WHERE sa.url_string LIKE ANY (
                        'clicktripz-de%',
                        'clicktripz-uk%'
    )

SELECT GET_DDL('table', 'se.bi.external_affiliate_metrics');


SELECT GET_DDL('table', 'data_vault_mvp.bi.external_affiliate_metrics');


WITH model_bookings_per_session AS (
    SELECT
        tt.touch_id,
        COUNT(DISTINCT fb.booking_id)                      AS bookings,
        SUM(fb.margin_gross_of_toms_gbp_constant_currency) AS margin_gross_of_toms_gbp_constant_currency
    FROM data_vault_mvp.single_customer_view_stg.module_touched_transactions tt
        INNER JOIN data_vault_mvp.dwh.fact_booking fb ON tt.booking_id = fb.booking_id
    WHERE fb.booking_status_type = 'live'
      AND event_tstamp::date >= CURRENT_DATE() - 30
    GROUP BY 1
),
     model_spvs_per_session AS (
         SELECT
             ts.touch_id,
             COUNT(*) AS spvs
         FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs ts
         WHERE event_tstamp::date >= CURRENT_DATE() - 30
         GROUP BY 1
     )
SELECT
    tba.touch_start_tstamp::DATE                                                                        AS date,
    tmc.touch_mkt_channel,
    tmc.affiliate,
    tmc.sub_affiliate_name,
    tmc.utm_content,
    tmc.awcampaignid,
    tmc.utm_campaign,
    tmc.utm_source,
    tba.touch_experience,
    tmc.touch_affiliate_territory,
    tmc.touch_hostname_territory,
    ta.attribution_model,
    COALESCE(SUM(s.spvs), 0)                                                                            AS spvs,
    COUNT(DISTINCT CASE WHEN tba.stitched_identity_type = 'se_user_id' THEN tba.attributed_user_id END) AS logged_in_users,
    COALESCE(SUM(b.bookings), 0)                                                                        AS transactions,
    COALESCE(SUM(b.margin_gross_of_toms_gbp_constant_currency), 0)                                      AS margin_gross_of_toms_gbp_constant_currency
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes tba
    INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_attribution ta ON tba.touch_id = ta.touch_id AND ta.attribution_model = 'last paid'
    INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel tmc ON ta.attributed_touch_id = tmc.touch_id
    LEFT JOIN  model_bookings_per_session b ON tba.touch_id = b.touch_id
    LEFT JOIN  model_spvs_per_session s ON tba.touch_id = s.touch_id
WHERE tba.touch_start_tstamp::DATE >= CURRENT_DATE() - 30
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12;


SELECT
    stba.touch_landing_page,
    PARSE_URL(stba.touch_landing_page)
FROM se.data.scv_touch_basic_attributes stba;



COALESCE
(DATE_TRUNC('month', MIN (fb.booking_completed_date)), '1970-01-01'
)
AS