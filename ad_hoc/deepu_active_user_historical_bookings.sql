-- QUERY_HISTORICAL_BOOKINGS_INFERENCE = """

WITH bookings AS
         (
             SELECT
                 user_id,
                 COUNT(*) AS num_bookings
             FROM data_science.predictive_modeling.user_deal_events
             WHERE (evt_date < TO_DATE($inference_run_date) - $feature_lookback)
               AND (evt_name = 'order')
               AND (territory_id = $territory_id_var)
             GROUP BY user_id
         ),
     active_users AS
         (
             SELECT DISTINCT
                 (user_id)
             FROM data_science.predictive_modeling.user_deal_events
             WHERE (evt_date >= TO_DATE($inference_run_date) - $feature_lookback)
               AND (evt_date < TO_DATE($inference_run_date))
               AND (territory_id = $territory_id_var)
         )
SELECT
    bookings.user_id,
    bookings.num_bookings
FROM bookings
    INNER JOIN active_users
WHERE bookings.user_id = active_users.user_id;


SET inference_run_date = CURRENT_DATE;
SET feature_lookback = 7;
SET territory_id_var = 4;
SELECT
    ura.shiro_user_id             AS user_id,
    COUNT(DISTINCT fb.booking_id) AS num_bookings
FROM data_vault_mvp.dwh.user_recent_activities ura
    INNER JOIN data_vault_mvp.dwh.user_attributes ua ON ura.shiro_user_id = ua.shiro_user_id
    INNER JOIN data_vault_mvp.dwh.fact_booking fb ON ura.shiro_user_id = fb.shiro_user_id
    -- bookings that occurred before the lookback date
    AND fb.booking_completed_timestamp < $inference_run_date - $feature_lookback
    -- filter to only complete bookings
    AND fb.booking_status_type = 'live'
WHERE
  -- user has activity within lookback date
    ura.last_session_end_tstamp >= $inference_run_date - $feature_lookback
  --filter for user territory
  AND ua.current_affiliate_territory_id = $territory_id_var
GROUP BY 1

