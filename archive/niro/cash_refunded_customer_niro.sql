WITH union_list AS (
    --create an event list of cancellations and bookings
    SELECT bl.shiro_user_id,
           bl.booking_id,
           bl.booking_completed_timestamp AS tstamp,
           'COMPLETE'                     AS status --manufacture a status
    FROM se.data.se_booking bl
    WHERE bl.booking_status IN ('COMPLETE', 'REFUNDED')
    UNION ALL
    SELECT bl.shiro_user_id,
           bl.booking_id,
           bl.cancellation_tstamp AS tstamp,
           'REFUNDED'             AS status --manufacture a status
    FROM se.data.se_booking bl
    WHERE bl.booking_status = 'REFUNDED'
),
     canx_partition AS (
         --cte required becauase can't nest window functions
         --this cte creates the cancellation partition
         SELECT ul.shiro_user_id,
                ul.booking_id,
                ul.tstamp,
                ul.status,
                IFF(ul.status = 'REFUNDED', 1, NULL) AS canx_flag,
                SUM(canx_flag)
                    OVER (PARTITION BY ul.shiro_user_id ORDER BY tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                                                     AS canx_partition
         FROM union_list ul
     ),
     canx_booking_id AS (
         --compute the first cancelled booking id within the cancellation partition
         SELECT cp.shiro_user_id,
                cp.booking_id,
                cp.tstamp,
                cp.status,
                cp.canx_flag,
                cp.canx_partition,
                IFF(canx_partition IS NULL,
                    NULL,
                    FIRST_VALUE(cp.booking_id)
                                OVER (PARTITION BY cp.shiro_user_id, canx_partition ORDER BY cp.tstamp)) AS canx_booking_id
         FROM canx_partition cp
     ),
     canx_details AS (
         --cancellation side
         SELECT sb.booking_id,
                sb.cancellation_date,
                sb.gross_revenue_gbp_constant_currency,
                sb.margin_gross_of_toms_gbp_constant_currency
         FROM se.data.se_booking sb
         WHERE sb.booking_status = 'REFUNDED'
           AND sb.cancellation_date BETWEEN '2020-05-01' AND '2020-09-30' --criteria of cancellations that occurred
           AND LOWER(cancellation_refund_channel) = 'payment_method'
           AND (LOWER(cancellation_reason) = 'member_cancellation_request'
             OR LOWER(cancellation_reason) = 'covid_19_cancellation')
     ),
     booking_details AS (
         --aggregate bookings up to cancellation
         SELECT cbi.canx_booking_id,
                sb.shiro_user_id,
                LISTAGG(sb.booking_id, ', ') WITHIN GROUP (ORDER BY sb.booking_completed_timestamp) AS booking_ids,
                COUNT(DISTINCT sb.booking_id)                                                       AS bookings,
                SUM(sb.gross_revenue_gbp_constant_currency)                                         AS gross_revenue_gbp_constant_currency,
                SUM(sb.margin_gross_of_toms_gbp_constant_currency)                                  AS margin_gross_of_toms_gbp_constant_currency
         FROM canx_booking_id cbi
                  LEFT JOIN se.data.se_booking sb ON cbi.booking_id = sb.booking_id
         WHERE cbi.status = 'COMPLETE'        -- to remove cancellation events
           AND sb.booking_completed_date >= '2020-05-01'
           AND sb.booking_status = 'COMPLETE' --to filter for only complete bookings
         GROUP BY 1, 2
     )
SELECT canx_booking_id,
       bd.shiro_user_id,
       cd.booking_id                                 AS canx_booking_id,
       cd.cancellation_date                          AS canx_cancellation_date,
       cd.gross_revenue_gbp_constant_currency        AS canx_gross_revenue_gbp_constant_currency,
       cd.margin_gross_of_toms_gbp_constant_currency AS canx_margin_gross_of_toms_gbp_constant_currency,
       bd.bookings,
       bd.booking_ids,
       bd.gross_revenue_gbp_constant_currency,
       bd.margin_gross_of_toms_gbp_constant_currency,
       SPLIT_PART(bd.booking_ids, ',', 0)            AS first_booking_id,
       s.gross_revenue_gbp_constant_currency,
       s.margin_gross_of_toms_gbp_constant_currency
FROM booking_details bd
         INNER JOIN canx_details cd ON bd.canx_booking_id = cd.booking_id
         LEFT JOIN se.data.se_booking s ON SPLIT_PART(bd.booking_ids, ',', 0) = s.booking_id
WHERE s.shiro_user_id = 47049944;
;
------------------------------------------------------------------------------------------------------------------------
--cash refunded bookings
WITH union_list AS (
    --create an event list of cancellations and bookings
    SELECT bl.shiro_user_id,
           bl.booking_id,
           bl.booking_completed_date AS date,
           'COMPLETE'                AS status --manufacture a status
    FROM se.data.se_booking bl
    WHERE bl.booking_status IN ('COMPLETE', 'REFUNDED')
    UNION ALL
    SELECT bl.shiro_user_id,
           bl.booking_id,
           bl.cancellation_date,
           'REFUNDED' AS status --manufacture a status
    FROM se.data.se_booking bl
    WHERE bl.booking_status = 'REFUNDED'
),
     canx_partition AS (
         --cte required becauase can't nest window functions
         --this cte creates the cancellation partition
         SELECT ul.shiro_user_id,
                ul.booking_id,
                ul.date,
                ul.status,
                IFF(ul.status = 'REFUNDED', 1, NULL) AS canx_flag,
                SUM(canx_flag)
                    OVER (PARTITION BY ul.shiro_user_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                                                     AS canx_partition
         FROM union_list ul
     ),
     canx_booking_id AS (
         --compute the first cancelled booking id within the cancellation partition
         SELECT cp.shiro_user_id,
                cp.booking_id,
                cp.date,
                cp.status,
                cp.canx_flag,
                cp.canx_partition,
                IFF(canx_partition IS NULL,
                    NULL,
                    FIRST_VALUE(cp.booking_id)
                                OVER (PARTITION BY cp.shiro_user_id, canx_partition ORDER BY cp.date)) AS canx_booking_id
         FROM canx_partition cp
     ),
     canx_details AS (
         --cancellation side
         SELECT sb.booking_id,
                sb.cancellation_date,
                sb.gross_revenue_gbp_constant_currency,
                sb.margin_gross_of_toms_gbp_constant_currency
         FROM se.data.se_booking sb
         WHERE sb.booking_status = 'REFUNDED'
           AND sb.cancellation_date BETWEEN '2020-12-10' AND '2021-02-17' --criteria of cancellations that occurred
           AND LOWER(cancellation_refund_channel) = 'payment_method'
           AND (LOWER(cancellation_reason) = 'member_cancellation_request'
             OR LOWER(cancellation_reason) = 'covid_19_cancellation')
     ),
     booking_details AS (
         --aggregate bookings up to cancellation
         SELECT cbi.canx_booking_id,
                sb.shiro_user_id,
                LISTAGG(sb.booking_id, ', ') WITHIN GROUP (ORDER BY sb.booking_completed_date) AS booking_ids,
                COUNT(DISTINCT sb.booking_id)                                                  AS bookings,
                SUM(sb.gross_revenue_gbp_constant_currency)                                    AS gross_revenue_gbp_constant_currency,
                SUM(sb.margin_gross_of_toms_gbp_constant_currency)                             AS margin_gross_of_toms_gbp_constant_currency
         FROM canx_booking_id cbi
                  LEFT JOIN se.data.se_booking sb ON cbi.booking_id = sb.booking_id
             AND sb.booking_completed_date >= '2020-05-01'
             AND sb.booking_status = 'COMPLETE' --to filter for only complete bookings
         WHERE cbi.status = 'COMPLETE' -- to remove cancellation events
         GROUP BY 1, 2
     )
SELECT canx_booking_id,
       bd.shiro_user_id,
       cd.booking_id                                 AS canx_booking_id,
       cd.cancellation_date                          AS canx_cancellation_date,
       cd.gross_revenue_gbp_constant_currency        AS canx_gross_revenue_gbp_constant_currency,
       cd.margin_gross_of_toms_gbp_constant_currency AS canx_margin_gross_of_toms_gbp_constant_currency,
       bd.bookings,
       bd.booking_ids,
       bd.gross_revenue_gbp_constant_currency,
       bd.margin_gross_of_toms_gbp_constant_currency,
       SPLIT_PART(bd.booking_ids, ',', 0)            AS first_booking_id,
       s.gross_revenue_gbp_constant_currency,
       s.margin_gross_of_toms_gbp_constant_currency
FROM booking_details bd
         INNER JOIN canx_details cd ON bd.canx_booking_id = cd.booking_id
         LEFT JOIN se.data.se_booking s ON SPLIT_PART(bd.booking_ids, ',', 0) = s.booking_id
;

------------------------------------------------------------------------------------------------------------------------
--cash refunded bookings
WITH union_list AS (
    --create an event list of cancellations and bookings
    SELECT COALESCE(bl.shiro_user_id::VARCHAR, 'Aff-' || bl.affiliate_user_id) AS user_id,
           bl.booking_id,
           bl.booking_completed_date                                           AS date,
           'COMPLETE'                                                          AS status --manufacture a status
    FROM se.data.se_booking bl
    WHERE bl.booking_status IN ('COMPLETE', 'REFUNDED')
    UNION ALL
    SELECT COALESCE(bl.shiro_user_id::VARCHAR, 'Aff-' || bl.affiliate_user_id) AS user_id,
           bl.booking_id,
           bl.cancellation_date,
           'REFUNDED'                                                          AS status --manufacture a status
    FROM se.data.se_booking bl
    WHERE bl.booking_status = 'REFUNDED'
),
     canx_partition AS (
         --cte required becauase can't nest window functions
         --this cte creates the cancellation partition
         SELECT ul.user_id,
                ul.booking_id,
                ul.date,
                ul.status,
                IFF(ul.status = 'REFUNDED', 1, NULL) AS canx_flag,
                SUM(canx_flag)
                    OVER (PARTITION BY ul.user_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                                                     AS canx_partition
         FROM union_list ul
     ),
     canx_booking_id AS (
         --compute the first cancelled booking id within the cancellation partition
         SELECT cp.user_id,
                cp.booking_id,
                cp.date,
                cp.status,
                cp.canx_flag,
                cp.canx_partition,
                IFF(canx_partition IS NULL,
                    NULL,
                    FIRST_VALUE(cp.booking_id)
                                OVER (PARTITION BY cp.user_id, canx_partition ORDER BY cp.date)) AS canx_booking_id
         FROM canx_partition cp
     ),
     canx_details AS (
         --cancellation side
         SELECT sb.booking_id,
                COALESCE(sb.shiro_user_id::VARCHAR, 'Aff-' || sb.affiliate_user_id) AS user_id,
                sb.cancellation_date,
                sb.gross_revenue_gbp_constant_currency,
                sb.margin_gross_of_toms_gbp_constant_currency
         FROM se.data.se_booking sb
         WHERE sb.booking_status = 'REFUNDED'
           AND sb.cancellation_date BETWEEN '2020-12-10' AND '2021-02-17' --criteria of cancellations that occurred
           AND LOWER(cancellation_refund_channel) = 'payment_method'
           AND (LOWER(cancellation_reason) = 'member_cancellation_request'
             OR LOWER(cancellation_reason) = 'covid_19_cancellation')
     ),
     booking_details AS (
         --aggregate bookings up to cancellation
         SELECT cbi.canx_booking_id,
                cbi.user_id,
                LISTAGG(sb.booking_id, ', ') WITHIN GROUP (ORDER BY sb.booking_completed_date) AS booking_ids,
                COUNT(DISTINCT sb.booking_id)                                                  AS bookings,
                SUM(sb.gross_revenue_gbp_constant_currency)                                    AS gross_revenue_gbp_constant_currency,
                SUM(sb.margin_gross_of_toms_gbp_constant_currency)                             AS margin_gross_of_toms_gbp_constant_currency
         FROM se.data.se_booking sb
                  INNER JOIN canx_booking_id cbi ON sb.booking_id = cbi.booking_id
             AND cbi.status = 'COMPLETE' -- to remove cancellation events
         WHERE sb.booking_completed_date >= '2020-05-01'
           AND sb.booking_status = 'COMPLETE' --to filter for only complete bookings
         GROUP BY 1, 2
     )
SELECT cd.booking_id                                 AS canx_booking_id,
       cd.user_id,
       cd.cancellation_date                          AS canx_cancellation_date,
       cd.gross_revenue_gbp_constant_currency        AS canx_gross_revenue_gbp_constant_currency,
       cd.margin_gross_of_toms_gbp_constant_currency AS canx_margin_gross_of_toms_gbp_constant_currency,
       bd.bookings,
       bd.booking_ids,
       bd.gross_revenue_gbp_constant_currency,
       bd.margin_gross_of_toms_gbp_constant_currency,
       SPLIT_PART(bd.booking_ids, ',', 0)            AS first_booking_id,
       s.gross_revenue_gbp_constant_currency,
       s.margin_gross_of_toms_gbp_constant_currency
FROM canx_details cd
         LEFT JOIN booking_details bd ON cd.booking_id = bd.canx_booking_id
         LEFT JOIN se.data.se_booking s ON SPLIT_PART(bd.booking_ids, ',', 0) = s.booking_id;

------------------------------------------------------------------------------------------------------------------------


WITH canx_details AS (
    --cancellation side
    SELECT sb.booking_id,
           COALESCE(sb.shiro_user_id::VARCHAR, 'Aff-' || sb.affiliate_user_id) AS user_id,
           sb.cancellation_date,
           sb.gross_revenue_gbp_constant_currency,
           sb.margin_gross_of_toms_gbp_constant_currency
    FROM se.data.se_booking sb
    WHERE sb.booking_status = 'REFUNDED'
      AND sb.cancellation_date BETWEEN '2020-12-10' AND '2021-02-17' --criteria of cancellations that occurred
      AND LOWER(cancellation_refund_channel) = 'payment_method'
      AND (LOWER(cancellation_reason) = 'member_cancellation_request'
        OR LOWER(cancellation_reason) = 'covid_19_cancellation')
)
   , booking_rank AS (
    SELECT s.booking_id,
           s.booking_status,
           s.booking_completed_date,
           s.margin_gross_of_toms_gbp_constant_currency,
           s.gross_revenue_gbp_constant_currency,
           COALESCE(s.shiro_user_id::VARCHAR, 'Aff-' || s.affiliate_user_id)                AS user_id,
           ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY s.booking_completed_timestamp ) AS booking_rank,
           ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY s.booking_completed_timestamp DESC) =
           1                                                                                AS is_users_most_recent_booking
    FROM se.data.se_booking s
    WHERE s.booking_status IN ('COMPLETE', 'REFUNDED')
      AND user_id IN (
        SELECT DISTINCT user_id
        FROM canx_details
    )
)
   , subsequent_bookings AS (
    SELECT cd.booking_id,
           cd.user_id,
           cd.cancellation_date,
           cd.gross_revenue_gbp_constant_currency,
           cd.margin_gross_of_toms_gbp_constant_currency,
           COALESCE(br.is_users_most_recent_booking, FALSE)                         AS is_most_recent,
           br.booking_rank                                                          AS user_booking_rank,
           MAX(br2.booking_rank)                                                    AS user_max_booking_rank,
           LISTAGG(DISTINCT br2.booking_status, ', ')                               AS subsequent_distinct_booking_status,
           LISTAGG(br2.booking_id, ', ') WITHIN GROUP ( ORDER BY br2.booking_rank ) AS subsequent_booking_id,
           LISTAGG(br2.booking_id::VARCHAR || ' - ' || br2.booking_status, ', ')
                   WITHIN GROUP ( ORDER BY br2.booking_rank )                       AS subsequent_booking_id_status
    FROM canx_details cd
             INNER JOIN booking_rank br
                        ON cd.booking_id = br.booking_id AND br.is_users_most_recent_booking = FALSE
             LEFT JOIN booking_rank br2 ON br.user_id = br2.user_id AND br.booking_rank < br2.booking_rank
    GROUP BY 1, 2, 3, 4, 5, 6, 7
)

SELECT *
FROM subsequent_bookings;


------------------------------------------------------------------------------------------------------------------------

WITH canx_details AS (
    --cancellation side
    SELECT sb.booking_id,
           COALESCE(sb.shiro_user_id::VARCHAR, 'Aff-' || sb.affiliate_user_id) AS user_id,
           sb.cancellation_date,
           sb.gross_revenue_gbp_constant_currency,
           sb.margin_gross_of_toms_gbp_constant_currency
    FROM se.data.se_booking sb
    WHERE sb.booking_status = 'REFUNDED'
      AND sb.cancellation_date BETWEEN '2020-05-01' AND '2020-09-30' --criteria of cancellations that occurred
      AND LOWER(cancellation_refund_channel) = 'payment_method'
      AND (LOWER(cancellation_reason) = 'member_cancellation_request'
        OR LOWER(cancellation_reason) = 'covid_19_cancellation')
)
   , booking_rank AS (
    SELECT s.booking_id,
           s.booking_status,
           s.booking_completed_date,
           s.margin_gross_of_toms_gbp_constant_currency,
           s.gross_revenue_gbp_constant_currency,
           COALESCE(s.shiro_user_id::VARCHAR, 'Aff-' || s.affiliate_user_id)                AS user_id,
           ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY s.booking_completed_timestamp ) AS booking_rank,
           ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY s.booking_completed_timestamp DESC) =
           1                                                                                AS is_users_most_recent_booking
    FROM se.data.se_booking s
    WHERE s.booking_status IN ('COMPLETE', 'REFUNDED')
      AND user_id IN (
        SELECT DISTINCT user_id
        FROM canx_details
    )
)
   , subsequent_bookings AS (
    SELECT cd.booking_id,
           cd.user_id,
           cd.cancellation_date,
           cd.gross_revenue_gbp_constant_currency,
           cd.margin_gross_of_toms_gbp_constant_currency,
           COALESCE(br.is_users_most_recent_booking, FALSE)                         AS is_most_recent,
           br.booking_rank                                                          AS user_booking_rank,
           MAX(br2.booking_rank)                                                    AS user_max_booking_rank,
           LISTAGG(DISTINCT br2.booking_status, ', ')                               AS subsequent_distinct_booking_status,
           LISTAGG(br2.booking_id, ', ') WITHIN GROUP ( ORDER BY br2.booking_rank ) AS subsequent_booking_id,
           LISTAGG(br2.booking_id::VARCHAR || ' - ' || br2.booking_status, ', ')
                   WITHIN GROUP ( ORDER BY br2.booking_rank )                       AS subsequent_booking_id_status
    FROM canx_details cd
             INNER JOIN booking_rank br
                        ON cd.booking_id = br.booking_id AND br.is_users_most_recent_booking = FALSE
             LEFT JOIN booking_rank br2 ON br.user_id = br2.user_id AND br.booking_rank < br2.booking_rank
    GROUP BY 1, 2, 3, 4, 5, 6, 7
)

SELECT *
FROM subsequent_bookings
WHERE subsequent_bookings.subsequent_distinct_booking_status = 'COMPLETE';