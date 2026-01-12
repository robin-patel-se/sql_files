SELECT cohort_year_month
     --, ORIGINAL_AFFILIATE_TERRITORY
     --, ACQUISITION_PLATFORM
     -- , EMAIL_OPT_IN
     -- , APP_COHORT_ID
     --, booker_status
     , COUNT(shiro_user_id) AS users
     , SUM(m1)              AS m1
     , SUM(m2)              AS m2
     , SUM(m3)              AS m3
     , SUM(m4)              AS m4
     , SUM(m5)              AS m5
     , SUM(m6)              AS m6
     , SUM(m7)              AS m7
     , SUM(m8)              AS m8
     , SUM(m9)              AS m9
     , SUM(m10)             AS m10
     , SUM(m11)             AS m11
     , SUM(m12)             AS m12
     , SUM(m13)             AS m13
     , SUM(m14)             AS m14
     , SUM(m15)             AS m15
     , SUM(m16)             AS m16
     , SUM(m17)             AS m17
     , SUM(m18)             AS m18
     , SUM(m19)             AS m19
     , SUM(m20)             AS m20
     , SUM(m21)             AS m21
     , SUM(m22)             AS m22
     , SUM(m23)             AS m23
     , SUM(m24)             AS m24
FROM (
         SELECT shiro_user_id
              , cohort_year_month
              , original_affiliate_territory
              , CASE WHEN last_complete_booking_tstamp IS NULL THEN 'non-booker' ELSE 'booker' END                          AS booker_status
              , MAX(CASE WHEN TO_CHAR(a.touch_start_tstamp, 'YYYY-MM') = cohort_year_month THEN 1 ELSE 0 END)               AS m1
              , MAX(CASE WHEN TO_CHAR(DATEADD('month', -1, o.event_date), 'YYYY-MM') = cohort_year_month THEN 1 ELSE 0 END) AS m2
              , MAX(CASE WHEN TO_CHAR(DATEADD('month', -2, o.event_date), 'YYYY-MM') = cohort_year_month THEN 1 ELSE 0 END) AS m3
              , MAX(CASE WHEN TO_CHAR(DATEADD('month', -3, o.event_date), 'YYYY-MM') = cohort_year_month THEN 1 ELSE 0 END) AS m4
              , MAX(CASE WHEN TO_CHAR(DATEADD('month', -4, o.event_date), 'YYYY-MM') = cohort_year_month THEN 1 ELSE 0 END) AS m5
              , MAX(CASE WHEN TO_CHAR(DATEADD('month', -5, o.event_date), 'YYYY-MM') = cohort_year_month THEN 1 ELSE 0 END) AS m6
              , MAX(CASE WHEN TO_CHAR(DATEADD('month', -6, o.event_date), 'YYYY-MM') = cohort_year_month THEN 1 ELSE 0 END) AS m7
              , MAX(CASE WHEN TO_CHAR(DATEADD('month', -7, o.event_date), 'YYYY-MM') = cohort_year_month THEN 1 ELSE 0 END) AS m8
              , MAX(CASE WHEN TO_CHAR(DATEADD('month', -8, o.event_date), 'YYYY-MM') = cohort_year_month THEN 1 ELSE 0 END) AS m9
              , MAX(CASE
                        WHEN TO_CHAR(DATEADD('month', -9, o.event_date), 'YYYY-MM') = cohort_year_month THEN 1
                        ELSE 0 END)                                                                                         AS m10
              , MAX(CASE
                        WHEN TO_CHAR(DATEADD('month', -10, o.event_date), 'YYYY-MM') = cohort_year_month THEN 1
                        ELSE 0 END)                                                                                         AS m11
              , MAX(CASE
                        WHEN TO_CHAR(DATEADD('month', -11, o.event_date), 'YYYY-MM') = cohort_year_month THEN 1
                        ELSE 0 END)                                                                                         AS m12
              , MAX(CASE
                        WHEN TO_CHAR(DATEADD('month', -12, o.event_date), 'YYYY-MM') = cohort_year_month THEN 1
                        ELSE 0 END)                                                                                         AS m13
              , MAX(CASE
                        WHEN TO_CHAR(DATEADD('month', -13, o.event_date), 'YYYY-MM') = cohort_year_month THEN 1
                        ELSE 0 END)                                                                                         AS m14
              , MAX(CASE
                        WHEN TO_CHAR(DATEADD('month', -14, o.event_date), 'YYYY-MM') = cohort_year_month THEN 1
                        ELSE 0 END)                                                                                         AS m15
              , MAX(CASE
                        WHEN TO_CHAR(DATEADD('month', -15, o.event_date), 'YYYY-MM') = cohort_year_month THEN 1
                        ELSE 0 END)                                                                                         AS m16
              , MAX(CASE
                        WHEN TO_CHAR(DATEADD('month', -16, o.event_date), 'YYYY-MM') = cohort_year_month THEN 1
                        ELSE 0 END)                                                                                         AS m17
              , MAX(CASE
                        WHEN TO_CHAR(DATEADD('month', -17, o.event_date), 'YYYY-MM') = cohort_year_month THEN 1
                        ELSE 0 END)                                                                                         AS m18
              , MAX(CASE
                        WHEN TO_CHAR(DATEADD('month', -18, o.event_date), 'YYYY-MM') = cohort_year_month THEN 1
                        ELSE 0 END)                                                                                         AS m19
              , MAX(CASE
                        WHEN TO_CHAR(DATEADD('month', -19, o.event_date), 'YYYY-MM') = cohort_year_month THEN 1
                        ELSE 0 END)                                                                                         AS m20
              , MAX(CASE
                        WHEN TO_CHAR(DATEADD('month', -20, o.event_date), 'YYYY-MM') = cohort_year_month THEN 1
                        ELSE 0 END)                                                                                         AS m21
              , MAX(CASE
                        WHEN TO_CHAR(DATEADD('month', -21, o.event_date), 'YYYY-MM') = cohort_year_month THEN 1
                        ELSE 0 END)                                                                                         AS m22
              , MAX(CASE
                        WHEN TO_CHAR(DATEADD('month', -22, o.event_date), 'YYYY-MM') = cohort_year_month THEN 1
                        ELSE 0 END)                                                                                         AS m23
              , MAX(CASE
                        WHEN TO_CHAR(DATEADD('month', -23, o.event_date), 'YYYY-MM') = cohort_year_month THEN 1
                        ELSE 0 END)                                                                                         AS m24
         FROM se.data.se_user_attributes e
                  LEFT JOIN se.data_pii.scv_touch_basic_attributes a ON e.shiro_user_id:: VARCHAR = a.attributed_user_id
                  LEFT JOIN "RAW_VAULT_PUBLIC_MVP"."SFMC"."EVENTS_OPENS" o
                            ON e.shiro_user_id:: VARCHAR = o.subscriber_key:: VARCHAR
         WHERE e.shiro_user_id = 62972247
           AND cohort_year_month >= '2019-01'
           --AND TOUCH_EXPERIENCE = 'native app'
           AND TO_DATE(o.event_date) < '2021-01-01'
           AND original_affiliate_territory IN ('DE', 'UK')
           AND TO_DATE(event_date) BETWEEN '2019-01-01' AND '2021-01-01'
         GROUP BY 1, 2, 3, 4
     )
GROUP BY 1;

USE WAREHOUSE pipe_xlarge;

WITH event_month_offset AS (
    --compute the date diff of events for event and user signup
    SELECT ue.shiro_user_id,
           sua.signup_tstamp::DATE                     AS signup_date,
           ue.date,
           DATEDIFF(MONTH, sua.signup_tstamp, ue.date) AS event_diff_month,
           ue.opens
    FROM se.data.user_emails ue
             INNER JOIN se.data.se_user_attributes sua ON ue.shiro_user_id = sua.shiro_user_id
    WHERE ue.date <= DATEADD('month', 24, sua.signup_tstamp) --filter to only return events that are within 24 months of signup;
),
     agg_to_month AS (
         --aggregate up open events to month level
         SELECT emo.shiro_user_id,
                emo.event_diff_month,
                COALESCE(SUM(emo.opens), 0) AS email_opens
         FROM event_month_offset emo
         GROUP BY 1, 2
     )
SELECT agm.shiro_user_id,
       SUM(IFF(agm.event_diff_month = 0, agm.email_opens, 0))  AS m0,
       SUM(IFF(agm.event_diff_month = 1, agm.email_opens, 0))  AS m1,
       SUM(IFF(agm.event_diff_month = 2, agm.email_opens, 0))  AS m2,
       SUM(IFF(agm.event_diff_month = 3, agm.email_opens, 0))  AS m3,
       SUM(IFF(agm.event_diff_month = 4, agm.email_opens, 0))  AS m4,
       SUM(IFF(agm.event_diff_month = 5, agm.email_opens, 0))  AS m5,
       SUM(IFF(agm.event_diff_month = 6, agm.email_opens, 0))  AS m6,
       SUM(IFF(agm.event_diff_month = 7, agm.email_opens, 0))  AS m7,
       SUM(IFF(agm.event_diff_month = 8, agm.email_opens, 0))  AS m8,
       SUM(IFF(agm.event_diff_month = 9, agm.email_opens, 0))  AS m9,
       SUM(IFF(agm.event_diff_month = 10, agm.email_opens, 0)) AS m10,
       SUM(IFF(agm.event_diff_month = 11, agm.email_opens, 0)) AS m11,
       SUM(IFF(agm.event_diff_month = 12, agm.email_opens, 0)) AS m12,
       SUM(IFF(agm.event_diff_month = 13, agm.email_opens, 0)) AS m13,
       SUM(IFF(agm.event_diff_month = 14, agm.email_opens, 0)) AS m14,
       SUM(IFF(agm.event_diff_month = 15, agm.email_opens, 0)) AS m15,
       SUM(IFF(agm.event_diff_month = 16, agm.email_opens, 0)) AS m16,
       SUM(IFF(agm.event_diff_month = 17, agm.email_opens, 0)) AS m17,
       SUM(IFF(agm.event_diff_month = 18, agm.email_opens, 0)) AS m18,
       SUM(IFF(agm.event_diff_month = 19, agm.email_opens, 0)) AS m19,
       SUM(IFF(agm.event_diff_month = 20, agm.email_opens, 0)) AS m20,
       SUM(IFF(agm.event_diff_month = 21, agm.email_opens, 0)) AS m21,
       SUM(IFF(agm.event_diff_month = 22, agm.email_opens, 0)) AS m22,
       SUM(IFF(agm.event_diff_month = 23, agm.email_opens, 0)) AS m23,
       SUM(IFF(agm.event_diff_month = 24, agm.email_opens, 0)) AS m24
FROM agg_to_month agm
GROUP BY 1
;


airflow backfill --start_date '2021-01-28 01:00:00' --end_date '2021-01-28 01:00:00' --task_regex '.*' hygiene_snapshots__finance_gsheets__manual_refunds__daily_at_01h00