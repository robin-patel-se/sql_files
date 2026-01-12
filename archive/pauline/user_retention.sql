SELECT app_cohort_year_month,
       COUNT(shiro_user_id) AS users,
       SUM(m1)              AS m1,
       SUM(m2)              AS m2,
       SUM(m3)              AS m3,
       SUM(m4)              AS m4,
       SUM(m5)              AS m5,
       SUM(m6)              AS m6,
       SUM(m7)              AS m7,
       SUM(m8)              AS m8,
       SUM(m9)              AS m9,
       SUM(m10)             AS m10,
       SUM(m11)             AS m11,
       SUM(m12)             AS m12,
       SUM(m13)             AS m13,
       SUM(m14)             AS m14,
       SUM(m15)             AS m15,
       SUM(m16)             AS m16,
       SUM(m17)             AS m17,
       SUM(m18)             AS m18,
       SUM(m19)             AS m19,
       SUM(m20)             AS m20
FROM (
         SELECT shiro_user_id,
                app_cohort_year_month,
                MAX(CASE WHEN TO_CHAR(a.touch_start_tstamp, 'YYYY-MM') = app_cohort_year_month THEN 1 ELSE 0 END)                        AS m1,
                MAX(CASE WHEN TO_CHAR(DATEADD('month', -1, a.touch_start_tstamp), 'YYYY-MM') = app_cohort_year_month THEN 1 ELSE 0 END)  AS m2,
                MAX(CASE WHEN TO_CHAR(DATEADD('month', -2, a.touch_start_tstamp), 'YYYY-MM') = app_cohort_year_month THEN 1 ELSE 0 END)  AS m3,
                MAX(CASE WHEN TO_CHAR(DATEADD('month', -3, a.touch_start_tstamp), 'YYYY-MM') = app_cohort_year_month THEN 1 ELSE 0 END)  AS m4,
                MAX(CASE WHEN TO_CHAR(DATEADD('month', -4, a.touch_start_tstamp), 'YYYY-MM') = app_cohort_year_month THEN 1 ELSE 0 END)  AS m5,
                MAX(CASE WHEN TO_CHAR(DATEADD('month', -5, a.touch_start_tstamp), 'YYYY-MM') = app_cohort_year_month THEN 1 ELSE 0 END)  AS m6,
                MAX(CASE WHEN TO_CHAR(DATEADD('month', -6, a.touch_start_tstamp), 'YYYY-MM') = app_cohort_year_month THEN 1 ELSE 0 END)  AS m7,
                MAX(CASE WHEN TO_CHAR(DATEADD('month', -7, a.touch_start_tstamp), 'YYYY-MM') = app_cohort_year_month THEN 1 ELSE 0 END)  AS m8,
                MAX(CASE WHEN TO_CHAR(DATEADD('month', -8, a.touch_start_tstamp), 'YYYY-MM') = app_cohort_year_month THEN 1 ELSE 0 END)  AS m9,
                MAX(CASE WHEN TO_CHAR(DATEADD('month', -9, a.touch_start_tstamp), 'YYYY-MM') = app_cohort_year_month THEN 1 ELSE 0 END)  AS m10,
                MAX(CASE WHEN TO_CHAR(DATEADD('month', -10, a.touch_start_tstamp), 'YYYY-MM') = app_cohort_year_month THEN 1 ELSE 0 END) AS m11,
                MAX(CASE WHEN TO_CHAR(DATEADD('month', -11, a.touch_start_tstamp), 'YYYY-MM') = app_cohort_year_month THEN 1 ELSE 0 END) AS m12,
                MAX(CASE WHEN TO_CHAR(DATEADD('month', -12, a.touch_start_tstamp), 'YYYY-MM') = app_cohort_year_month THEN 1 ELSE 0 END) AS m13,
                MAX(CASE WHEN TO_CHAR(DATEADD('month', -13, a.touch_start_tstamp), 'YYYY-MM') = app_cohort_year_month THEN 1 ELSE 0 END) AS m14,
                MAX(CASE WHEN TO_CHAR(DATEADD('month', -14, a.touch_start_tstamp), 'YYYY-MM') = app_cohort_year_month THEN 1 ELSE 0 END) AS m15,
                MAX(CASE WHEN TO_CHAR(DATEADD('month', -15, a.touch_start_tstamp), 'YYYY-MM') = app_cohort_year_month THEN 1 ELSE 0 END) AS m16,
                MAX(CASE WHEN TO_CHAR(DATEADD('month', -16, a.touch_start_tstamp), 'YYYY-MM') = app_cohort_year_month THEN 1 ELSE 0 END) AS m17,
                MAX(CASE WHEN TO_CHAR(DATEADD('month', -17, a.touch_start_tstamp), 'YYYY-MM') = app_cohort_year_month THEN 1 ELSE 0 END) AS m18,
                MAX(CASE WHEN TO_CHAR(DATEADD('month', -18, a.touch_start_tstamp), 'YYYY-MM') = app_cohort_year_month THEN 1 ELSE 0 END) AS m19,
                MAX(CASE WHEN TO_CHAR(DATEADD('month', -19, a.touch_start_tstamp), 'YYYY-MM') = app_cohort_year_month THEN 1 ELSE 0 END) AS m20
         FROM se.data.se_user_attributes e
                  LEFT JOIN se.data.scv_touch_basic_attributes a ON e.shiro_user_id:: VARCHAR = a.attributed_user_id_hash
         WHERE app_cohort_year_month >= '2019-01'
           AND touch_experience = 'native app'
           AND TO_DATE(a.touch_start_tstamp) < '2020-10-01'
         GROUP BY 1, 2
     )
GROUP BY 1
ORDER BY 1 ASC;

-----------------------------------------------------------------------------------------------------------------------

WITH sessions AS (
    SELECT shiro_user_id,
           app_cohort_year_month,
           MAX(CASE WHEN TO_CHAR(a.touch_start_tstamp, 'YYYY-MM') = app_cohort_year_month THEN 1 ELSE 0 END)                        AS m1,
           MAX(CASE WHEN TO_CHAR(DATEADD('month', -1, a.touch_start_tstamp), 'YYYY-MM') = app_cohort_year_month THEN 1 ELSE 0 END)  AS m2,
           MAX(CASE WHEN TO_CHAR(DATEADD('month', -2, a.touch_start_tstamp), 'YYYY-MM') = app_cohort_year_month THEN 1 ELSE 0 END)  AS m3,
           MAX(CASE WHEN TO_CHAR(DATEADD('month', -3, a.touch_start_tstamp), 'YYYY-MM') = app_cohort_year_month THEN 1 ELSE 0 END)  AS m4,
           MAX(CASE WHEN TO_CHAR(DATEADD('month', -4, a.touch_start_tstamp), 'YYYY-MM') = app_cohort_year_month THEN 1 ELSE 0 END)  AS m5,
           MAX(CASE WHEN TO_CHAR(DATEADD('month', -5, a.touch_start_tstamp), 'YYYY-MM') = app_cohort_year_month THEN 1 ELSE 0 END)  AS m6,
           MAX(CASE WHEN TO_CHAR(DATEADD('month', -6, a.touch_start_tstamp), 'YYYY-MM') = app_cohort_year_month THEN 1 ELSE 0 END)  AS m7,
           MAX(CASE WHEN TO_CHAR(DATEADD('month', -7, a.touch_start_tstamp), 'YYYY-MM') = app_cohort_year_month THEN 1 ELSE 0 END)  AS m8,
           MAX(CASE WHEN TO_CHAR(DATEADD('month', -8, a.touch_start_tstamp), 'YYYY-MM') = app_cohort_year_month THEN 1 ELSE 0 END)  AS m9,
           MAX(CASE WHEN TO_CHAR(DATEADD('month', -9, a.touch_start_tstamp), 'YYYY-MM') = app_cohort_year_month THEN 1 ELSE 0 END)  AS m10,
           MAX(CASE WHEN TO_CHAR(DATEADD('month', -10, a.touch_start_tstamp), 'YYYY-MM') = app_cohort_year_month THEN 1 ELSE 0 END) AS m11,
           MAX(CASE WHEN TO_CHAR(DATEADD('month', -11, a.touch_start_tstamp), 'YYYY-MM') = app_cohort_year_month THEN 1 ELSE 0 END) AS m12,
           MAX(CASE WHEN TO_CHAR(DATEADD('month', -12, a.touch_start_tstamp), 'YYYY-MM') = app_cohort_year_month THEN 1 ELSE 0 END) AS m13,
           MAX(CASE WHEN TO_CHAR(DATEADD('month', -13, a.touch_start_tstamp), 'YYYY-MM') = app_cohort_year_month THEN 1 ELSE 0 END) AS m14,
           MAX(CASE WHEN TO_CHAR(DATEADD('month', -14, a.touch_start_tstamp), 'YYYY-MM') = app_cohort_year_month THEN 1 ELSE 0 END) AS m15,
           MAX(CASE WHEN TO_CHAR(DATEADD('month', -15, a.touch_start_tstamp), 'YYYY-MM') = app_cohort_year_month THEN 1 ELSE 0 END) AS m16,
           MAX(CASE WHEN TO_CHAR(DATEADD('month', -16, a.touch_start_tstamp), 'YYYY-MM') = app_cohort_year_month THEN 1 ELSE 0 END) AS m17,
           MAX(CASE WHEN TO_CHAR(DATEADD('month', -17, a.touch_start_tstamp), 'YYYY-MM') = app_cohort_year_month THEN 1 ELSE 0 END) AS m18,
           MAX(CASE WHEN TO_CHAR(DATEADD('month', -18, a.touch_start_tstamp), 'YYYY-MM') = app_cohort_year_month THEN 1 ELSE 0 END) AS m19,
           MAX(CASE WHEN TO_CHAR(DATEADD('month', -19, a.touch_start_tstamp), 'YYYY-MM') = app_cohort_year_month THEN 1 ELSE 0 END) AS m20
    FROM se.data.se_user_attributes e
             LEFT JOIN se.data_pii.scv_touch_basic_attributes a ON e.shiro_user_id::VARCHAR = a.attributed_user_id
    WHERE app_cohort_year_month >= '2019-01'
      AND touch_experience IN ('native app ios', 'native app android')
      AND TO_DATE(a.touch_start_tstamp) < '2020-10-01'
    GROUP BY 1, 2
)
SELECT app_cohort_year_month,
       COUNT(shiro_user_id) AS users,
       SUM(m1)              AS m1,
       SUM(m2)              AS m2,
       SUM(m3)              AS m3,
       SUM(m4)              AS m4,
       SUM(m5)              AS m5,
       SUM(m6)              AS m6,
       SUM(m7)              AS m7,
       SUM(m8)              AS m8,
       SUM(m9)              AS m9,
       SUM(m10)             AS m10,
       SUM(m11)             AS m11,
       SUM(m12)             AS m12,
       SUM(m13)             AS m13,
       SUM(m14)             AS m14,
       SUM(m15)             AS m15,
       SUM(m16)             AS m16,
       SUM(m17)             AS m17,
       SUM(m18)             AS m18,
       SUM(m19)             AS m19,
       SUM(m20)             AS m20
FROM sessions s
GROUP BY 1
ORDER BY 1 ASC
