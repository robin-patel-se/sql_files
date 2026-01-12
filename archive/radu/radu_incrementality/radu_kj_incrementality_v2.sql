WITH users_bookings AS (
    SELECT fcb.shiro_user_id,
           fcb.booking_completed_date,
           COALESCE(sb.transaction_id, tb.se_sale_id || '-' || tb.reference_id)                        AS transaction_id,
           ROW_NUMBER() OVER (PARTITION BY fcb.shiro_user_id ORDER BY fcb.booking_completed_date DESC) AS rank
    FROM se.data.fact_complete_booking fcb
             LEFT JOIN se.data.se_booking sb ON fcb.booking_id = sb.booking_id
             LEFT JOIN se.data.tb_booking tb ON fcb.booking_id = tb.booking_id
    WHERE fcb.booking_completed_date IS NOT NULL --because deposit bookings in catalogue aren't yet complete
      AND fcb.shiro_user_id IS NOT NULL
      --last two most recent bookings
        QUALIFY ROW_NUMBER() OVER (PARTITION BY fcb.shiro_user_id ORDER BY fcb.booking_completed_date DESC) < 3
),


     last_30_day_sessions AS (
         SELECT stba.attributed_user_id,
                count(*) AS sessions
         FROM se.data_pii.scv_touch_basic_attributes stba
         WHERE stba.stitched_identity_type = 'se_user_id'
           AND stba.touch_start_tstamp >= CURRENT_DATE - 31
         GROUP BY 1
     )
SELECT sua.shiro_user_id,
       sua.signup_tstamp::DATE                                               AS date_user_joined,
       COALESCE(lds.sessions, 0)                                             AS sessions_last_30_days,
       ub2.booking_completed_date::DATE                                      AS prev_booking_date,        -- penultimate booking date
       ub2.transaction_id                                                    AS prev_transaction_id,      -- penultimate transaction id
       ub1.booking_completed_date::DATE                                      AS last_booking_date,        -- last booking date
       ub1.transaction_id                                                    AS last_transaction_id,      -- last transaction id
       CASE
           WHEN prev_booking_date IS NULL THEN 1
           WHEN prev_booking_date < CURRENT_DATE - 365 THEN 1
           ELSE 0 END                                                        AS previous_booking_rule,    -- one time booker OR previous booking was more than a year ago
       CASE
           WHEN sessions_last_30_days = 0 THEN 0
           WHEN sessions_last_30_days >= 100 THEN 0
           ELSE 1 END                                                        AS browser_count_rule,
       CASE WHEN last_booking_date - date_user_joined >= 3 THEN 1 ELSE 0 END AS join_date_to_booking_rule --their last booking wasn't within 3 days of them joining
FROM se.data.se_user_attributes sua
         LEFT JOIN users_bookings ub1 ON sua.shiro_user_id = ub1.shiro_user_id AND ub1.rank = 1
         LEFT JOIN users_bookings ub2 ON sua.shiro_user_id = ub2.shiro_user_id AND ub2.rank = 2
         LEFT JOIN last_30_day_sessions lds ON sua.shiro_user_id = lds.attributed_user_id;

------------------------------------------------------------------------------------------------------------------------
WITH users_bookings AS (
    SELECT fcb.shiro_user_id,
           fcb.booking_completed_date,
           COALESCE(sb.transaction_id, tb.se_sale_id || '-' || tb.reference_id)                        AS transaction_id,
           fcb.margin_gross_of_toms_gbp_constant_currency,
           ROW_NUMBER() OVER (PARTITION BY fcb.shiro_user_id ORDER BY fcb.booking_completed_date DESC) AS rank
    FROM se.data.fact_complete_booking fcb
             LEFT JOIN se.data.se_booking sb ON fcb.booking_id = sb.booking_id
             LEFT JOIN se.data.tb_booking tb ON fcb.booking_id = tb.booking_id
    WHERE fcb.booking_completed_date IS NOT NULL --because deposit bookings in catalogue aren't yet complete
      AND fcb.shiro_user_id IS NOT NULL
--                         AND fcb.booking_completed_date >= '2020-08-01' --booking date range
      --last two most recent bookings
        QUALIFY ROW_NUMBER() OVER (PARTITION BY fcb.shiro_user_id ORDER BY fcb.booking_completed_date DESC) < 3
),
     bookings_with_session AS (
         SELECT ub.*,
                ua.date,
                ua.web_sessions_30d,
                ua.app_sessions_30d,
                ua.web_sessions_30d + ua.app_sessions_30d AS user_sessions
         FROM users_bookings ub
                  LEFT JOIN se.data.user_activity ua ON ub.shiro_user_id = ua.shiro_user_id
             AND ub.booking_completed_date::DATE = ua.date
     )
SELECT sua.shiro_user_id,
       sua.signup_tstamp::DATE                                               AS date_user_joined,
       ub1.booking_completed_date::DATE                                      AS prev_booking_date,
       ub1.transaction_id                                                    AS prev_transaction_id,
       ub1.margin_gross_of_toms_gbp_constant_currency                        AS prev_margin_gbp_constant_currency,
       ub1.web_sessions_30d                                                  AS web_sessions_30d_prior_to_prev_booking,
       ub1.app_sessions_30d                                                  AS app_sessions_30d_prior_to_prev_booking,
       ub1.user_sessions                                                     AS sessions_30d_prior_to_prev_booking,
       ub2.booking_completed_date::DATE                                      AS last_booking_date,
       ub2.transaction_id                                                    AS last_transaction_id,
       ub2.margin_gross_of_toms_gbp_constant_currency                        AS last_margin_gbp_constant_currency,
       ub2.web_sessions_30d                                                  AS web_sessions_30d_prior_to_last_booking,
       ub2.app_sessions_30d                                                  AS app_sessions_30d_prior_to_last_booking,
       ub2.user_sessions                                                     AS sessions_30d_prior_to_last_booking,

       CASE
           WHEN prev_booking_date IS NULL THEN 1
           WHEN prev_booking_date < CURRENT_DATE - 365 THEN 1
           ELSE 0 END                                                        AS previous_booking_rule,    -- one time booker OR previous booking was more than a year ago
       CASE
           WHEN sessions_30d_prior_to_prev_booking = 0 THEN 0
           WHEN sessions_30d_prior_to_prev_booking >= 100 THEN 0
           ELSE 1 END                                                        AS browser_count_rule,
       CASE WHEN last_booking_date - date_user_joined >= 3 THEN 1 ELSE 0 END AS join_date_to_booking_rule --their last booking wasn't within 3 days of them joining
FROM se.data.se_user_attributes sua
         INNER JOIN bookings_with_session ub1 ON sua.shiro_user_id = ub1.shiro_user_id AND ub1.rank = 1
         LEFT JOIN bookings_with_session ub2 ON sua.shiro_user_id = ub2.shiro_user_id AND ub2.rank = 2


------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM (
         WITH users_bookings AS (
             SELECT fcb.shiro_user_id,
                    fcb.booking_completed_date,
                    sb.transaction_id,
                    fcb.margin_gross_of_toms_gbp_constant_currency,
                    ROW_NUMBER() OVER (PARTITION BY fcb.shiro_user_id ORDER BY fcb.booking_completed_date DESC) AS rank
             FROM se.data.fact_complete_booking fcb
                      LEFT JOIN se.data.se_booking sb ON fcb.booking_id = sb.booking_id
                      LEFT JOIN se.data.tb_booking tb ON fcb.booking_id = tb.booking_id
             WHERE fcb.booking_completed_date IS NOT NULL --because deposit bookings in catalogue aren't yet complete
               AND fcb.shiro_user_id IS NOT NULL
               AND fcb.booking_completed_date >= '2019-03-01'
               AND fcb.booking_completed_date <= '2020-08-30'--booking date range
         ),
              bookings_with_session AS (
                  SELECT ub.*,
                         ua.date,
                         ua.web_sessions_30d,
                         ua.app_sessions_30d,
                         ua.web_sessions_30d + ua.app_sessions_30d AS user_sessions
                  FROM users_bookings ub
                           LEFT JOIN se.data.user_activity ua
                                     ON ub.shiro_user_id = ua.shiro_user_id AND ub.booking_completed_date::DATE = ua.date
              )


         SELECT sua.shiro_user_id,
                sua.signup_tstamp::DATE                                          AS date_user_joined,
                ub1.booking_completed_date::DATE                                 AS booking_date,
                ub1.transaction_id                                               AS transaction_id,
                ub1.margin_gross_of_toms_gbp_constant_currency                   AS margin_gbp_constant_currency,
                ub1.web_sessions_30d                                             AS web_sessions_30d_prior_to_prev_booking,
                ub1.app_sessions_30d                                             AS app_sessions_30d_prior_to_prev_booking,
                ub1.user_sessions                                                AS sessions_30d_prior_to_prev_booking,
                ub2.booking_completed_date::DATE                                 AS prev_booking_date,
                ub2.transaction_id                                               AS prev_transaction_id,
                ub2.margin_gross_of_toms_gbp_constant_currency                   AS prev_margin_gbp_constant_currency,
                ub2.web_sessions_30d                                             AS web_sessions_30d_prior_to_last_booking,
                ub2.app_sessions_30d                                             AS app_sessions_30d_prior_to_last_booking,
                ub2.user_sessions                                                AS sessions_30d_prior_to_last_booking,
                CASE
                    WHEN prev_booking_date IS NULL THEN 1
                    WHEN prev_booking_date < CURRENT_DATE - 365 THEN 1
                    ELSE 0 END                                                   AS previous_booking_rule,    -- one time booker OR previous booking was more than a year ago
                CASE
                    WHEN sessions_30d_prior_to_prev_booking = 0 THEN 0
                    WHEN sessions_30d_prior_to_prev_booking >= 100 THEN 0
                    ELSE 1 END                                                   AS browser_count_rule,
                CASE WHEN booking_date - date_user_joined >= 3 THEN 1 ELSE 0 END AS join_date_to_booking_rule --their last booking wasn't within 3 days of them joining

         FROM bookings_with_session ub1
                  LEFT JOIN bookings_with_session ub2
                            ON ub1.shiro_user_id = ub2.shiro_user_id AND (ub2.rank = ub1.rank + 1)
                  LEFT JOIN se.data.se_user_attributes sua ON ub1.shiro_user_id = sua.shiro_user_id
     )
WHERE transaction_id IN ('100125-852707-49871740',
                         '100128-852799-49642313',
                         '100221-853301-49568286',
                         '100268-853475-49872263',
                         '100323-853710-49703826',
                         '100362-853894-50168643',
                         '100362-853894-50189584',
                         '100542-854745-49935150',
                         '100653-855327-49859587',
                         '100685-855444-50249785',
                         '100825-856022-50176566',
                         '100836-856068-50192155',
                         '100942-856533-49768642',
                         '101016-857696-50191078',
                         '101169-860998-50111558',
                         '101294-858172-50032924',
                         '101300-858218-50155915',
                         '101510-859540-50553278',
                         '101520-859583-50492766',
                         '101544-859668-50428053',
                         '101550-859696-50035510',
                         '101640-860092-50422139',
                         '101741-860608-50388524',
                         '101764-860752-50610569',
                         '102040-862127-50265058',
                         '102171-862818-50429679',
                         '102192-862925-50685935'
    );



WITH users_bookings AS (
    SELECT fcb.shiro_user_id,
           fcb.booking_completed_date,
           sb.transaction_id                                                                           AS transaction_id,
           fcb.margin_gross_of_toms_gbp_constant_currency,
           ROW_NUMBER() OVER (PARTITION BY fcb.shiro_user_id ORDER BY fcb.booking_completed_date DESC) AS rank
    FROM se.data.fact_complete_booking fcb
             LEFT JOIN se.data.se_booking sb ON fcb.booking_id = sb.booking_id
             LEFT JOIN se.data.tb_booking tb ON fcb.booking_id = tb.booking_id
    WHERE fcb.booking_completed_date IS NOT NULL --because deposit bookings in catalogue aren't yet complete
      AND fcb.shiro_user_id IS NOT NULL
      AND fcb.booking_completed_date >= '2019-03-01'
      AND fcb.booking_completed_date <= '2020-08-30'--booking date range
    --last two most recent bookings
    /*QUALIFY ROW_NUMBER() OVER (PARTITION BY fcb.shiro_user_id ORDER BY fcb.booking_completed_date DESC) < 3)*/
),


     bookings_with_session AS (
         SELECT ub.*,
                ua.date,
                ua.web_sessions_30d,
                ua.app_sessions_30d,
                ua.web_sessions_30d + ua.app_sessions_30d AS user_sessions
         FROM users_bookings ub
                  LEFT JOIN se.data.user_activity ua
                            ON ub.shiro_user_id = ua.shiro_user_id AND ub.booking_completed_date::DATE = ua.date
     )


SELECT sua.shiro_user_id,
       sua.signup_tstamp::DATE                                          AS date_user_joined,
       ub1.booking_completed_date::DATE                                 AS booking_date,
       ub1.transaction_id                                               AS transaction_id,
       ub1.margin_gross_of_toms_gbp_constant_currency                   AS margin_gbp_constant_currency,
       ub1.web_sessions_30d                                             AS web_sessions_30d_prior_to_prev_booking,
       ub1.app_sessions_30d                                             AS app_sessions_30d_prior_to_prev_booking,
       ub1.user_sessions                                                AS sessions_30d_prior_to_prev_booking,
       ub2.booking_completed_date::DATE                                 AS prev_booking_date,
       ub2.transaction_id                                               AS prev_transaction_id,
       ub2.margin_gross_of_toms_gbp_constant_currency                   AS prev_margin_gbp_constant_currency,
       ub2.web_sessions_30d                                             AS web_sessions_30d_prior_to_last_booking,
       ub2.app_sessions_30d                                             AS app_sessions_30d_prior_to_last_booking,
       ub2.user_sessions                                                AS sessions_30d_prior_to_last_booking,
       CASE
           WHEN prev_booking_date IS NULL THEN 1
           WHEN prev_booking_date < CURRENT_DATE - 365 THEN 1
           ELSE 0 END                                                   AS previous_booking_rule,    -- one time booker OR previous booking was more than a year ago
       CASE
           WHEN sessions_30d_prior_to_prev_booking = 0 THEN 0
           WHEN sessions_30d_prior_to_prev_booking >= 100 THEN 0
           ELSE 1 END                                                   AS browser_count_rule,
       CASE WHEN booking_date - date_user_joined >= 3 THEN 1 ELSE 0 END AS join_date_to_booking_rule --their last booking wasn't within 3 days of them joining

FROM bookings_with_session ub1
         LEFT JOIN bookings_with_session ub2 ON ub1.shiro_user_id = ub2.shiro_user_id AND (ub2.rank = ub1.rank + 1)
         JOIN se.data.se_user_attributes sua ON sua.shiro_user_id = ub1.shiro_user_id


