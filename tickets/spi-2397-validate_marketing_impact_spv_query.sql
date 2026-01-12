SET (from_date, to_date)= ('2022-01-01', '2022-06-01');

WITH sess_bookings AS (
    SELECT
        stt.touch_id,
        COUNT(DISTINCT fcb.booking_id)                      AS bookings,
        SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin
    FROM se.data.scv_touch_basic_attributes stba
        INNER JOIN se.data.scv_touched_transactions stt ON stba.touch_id = stt.touch_id
        INNER JOIN se.data.fact_booking fcb ON stt.booking_id = fcb.booking_id
    WHERE stba.touch_start_tstamp >= $from_date
      AND stba.touch_start_tstamp <= $to_date
      AND booking_status IN ('COMPLETE', 'REFUNDED')
    GROUP BY 1
)
   , sess_spvs AS (
    SELECT
        stba.touch_id,
        COUNT(*) AS spvs
    FROM se.data.scv_touch_basic_attributes stba
        JOIN se.data.scv_touched_spvs sts ON stba.touch_id = sts.touch_id
    WHERE stba.touch_start_tstamp >= $from_date
      AND stba.touch_start_tstamp <= $to_date
    GROUP BY 1
)
SELECT
    DATE_TRUNC(MONTH, stba.touch_start_tstamp::DATE)                                                      AS month,
    stmc.touch_mkt_channel,
    se.data.member_recency_status(sua.signup_tstamp, stba.touch_start_tstamp)                             AS member_receny_status,
    DATE_TRUNC(MONTH, sua.signup_tstamp::DATE)                                                            AS signup_month,
    es.engagement_segment                                                                                 AS engagement_segment,
    stmc.touch_affiliate_territory                                                                        AS touch_hostname_territory,
    stba.stitched_identity_type = 'se_user_id'                                                            AS is_member,
    es2.engagement_segment                                                                                AS previously_active_more_than_90,
    COALESCE(SUM(s.spvs), 0)                                                                              AS spvs,
    COUNT(DISTINCT CASE WHEN stba.stitched_identity_type = 'se_user_id' THEN stba.attributed_user_id END) AS logged_in_users,
    COALESCE(SUM(b.bookings), 0)                                                                          AS bookings,
    COALESCE(SUM(b.margin), 0)                                                                            AS margin

FROM se.data_pii.scv_touch_basic_attributes stba
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
    LEFT JOIN  se.data.user_segmentation es ON stba.attributed_user_id = es.shiro_user_id::VARCHAR AND es.date = '2022-06-16'
    LEFT JOIN  se.data.user_segmentation es2
               ON es2.shiro_user_id::VARCHAR = es.shiro_user_id::VARCHAR
                   AND es.engagement_segment IN ('last_active_1d', 'last_active_7d', 'last_active_30d')
                   AND es2.date = '2022-05-16' AND es2.engagement_segment = 'last_active_90d+'
    LEFT JOIN  se.data.se_user_attributes sua ON TRY_TO_NUMBER(stba.attributed_user_id) = sua.shiro_user_id
    LEFT JOIN  sess_bookings b ON stba.touch_id = b.touch_id
    LEFT JOIN  sess_spvs s ON stba.touch_id = s.touch_id

WHERE stba.touch_start_tstamp >= $from_date
  AND stba.touch_start_tstamp <= $to_date
  AND stmc.touch_affiliate_territory IN ('UK', 'DE', 'IT', 'NL', 'SE')

GROUP BY 1, 2, 3, 4, 5, 6, 7, 8;


------------------------------------------------------------------------------------------------------------------------
--new query 2022-06-21

SET (from_date, to_date)= ('2022-01-01', '2022-06-01');

WITH sess_bookings AS (
    SELECT
        stt.touch_id,
        COUNT(DISTINCT fcb.booking_id)                      AS bookings,
        SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin
    FROM se.data.scv_touch_basic_attributes stba
        INNER JOIN se.data.scv_touched_transactions stt ON stba.touch_id = stt.touch_id
        INNER JOIN se.data.fact_booking fcb ON stt.booking_id = fcb.booking_id
    WHERE stba.touch_start_tstamp >= $from_date
      AND stba.touch_start_tstamp <= $to_date
      AND booking_status IN ('COMPLETE', 'REFUNDED')
    GROUP BY 1
)
   , sess_spvs AS (
    SELECT
        stba.touch_id,
        COUNT(*) AS spvs
    FROM se.data.scv_touch_basic_attributes stba
        JOIN se.data.scv_touched_spvs sts ON stba.touch_id = sts.touch_id
    WHERE stba.touch_start_tstamp >= $from_date
      AND stba.touch_start_tstamp <= $to_date
    GROUP BY 1
)
SELECT
    DATE_TRUNC(MONTH, stba.touch_start_tstamp::DATE)                                                      AS month,
    stmc.touch_mkt_channel,
    se.data.member_recency_status(sua.signup_tstamp, stba.touch_start_tstamp)                             AS member_receny_status,
    DATE_TRUNC(MONTH, sua.signup_tstamp::DATE)                                                            AS signup_month,
    es.engagement_segment                                                                                 AS engagement_segment,
    stmc.touch_affiliate_territory                                                                        AS touch_hostname_territory,
    stba.stitched_identity_type = 'se_user_id'                                                            AS is_member,
    es2.engagement_segment                                                                                AS previously_active_more_than_90,
    COALESCE(SUM(s.spvs), 0)                                                                              AS spvs,
    COUNT(DISTINCT CASE WHEN stba.stitched_identity_type = 'se_user_id' THEN stba.attributed_user_id END) AS logged_in_users,
    COALESCE(SUM(b.bookings), 0)                                                                          AS bookings,
    COALESCE(SUM(b.margin), 0)                                                                            AS margin

FROM se.data_pii.scv_touch_basic_attributes stba
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
    LEFT JOIN  se.data.user_segmentation es ON stba.attributed_user_id = es.shiro_user_id::VARCHAR AND es.date = DATE_TRUNC(MONTH, stba.touch_start_tstamp) - 1
    LEFT JOIN  se.data.user_segmentation es2
               ON es2.shiro_user_id::VARCHAR = es.shiro_user_id::VARCHAR AND es.engagement_segment IN ('last_active_1d', 'last_active_7d', 'last_active_14d', 'last_active_30d')
                   AND es2.date = DATEADD(DAY, -31, DATE_TRUNC(MONTH, stba.touch_start_tstamp) - 1) AND es2.engagement_segment = 'last_active_90d+' -- Dynamic date 31 days from yesterday
    LEFT JOIN  se.data.se_user_attributes sua ON TRY_TO_NUMBER(stba.attributed_user_id) = sua.shiro_user_id
    LEFT JOIN  sess_bookings b ON stba.touch_id = b.touch_id
    LEFT JOIN  sess_spvs s ON stba.touch_id = s.touch_id

WHERE stba.touch_start_tstamp >= $from_date
  AND stba.touch_start_tstamp <= $to_date
  AND stmc.touch_affiliate_territory IN ('UK', 'DE', 'IT', 'NL', 'SE')

GROUP BY 1, 2, 3, 4, 5, 6, 7, 8;


SELECT
    sts.event_tstamp::DATE,
    COUNT(*) AS spvs
FROM se.data.scv_touched_spvs sts
WHERE sts.event_tstamp >= CURRENT_DATE - 30
GROUP BY 1