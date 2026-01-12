--differentiate whether or not an agent has cancelled a booking
--note some bookings may be from secret escapes users themselves
--self serve was approx 8 months ago
--number of canx from CS agents vs non CS agent

SELECT sb.booking_status,
       sb.booking_id,
       sb.transaction_id,
       sb.cancellation_tstamp,
       sb.cancellation_requested_by_domain,
       sb.cancellation_reason
FROM data_vault_mvp.dwh.se_booking sb
WHERE sb.booking_status IN ('REFUNDED', 'CANCELLED')
  AND sb.cancellation_reason = 'MEMBER_CANCELLATION_REQUEST'
;
--including staff bookings
SELECT sb.cancellation_tstamp::DATE                                                                                                                         AS cancellation_date,
       SUM(IFF(sb.cancellation_reason = 'MEMBER_CANCELLATION_REQUEST'
                   AND sb.cancellation_requested_by_domain = 'secretescapes.com' OR sb.cancellation_requested_by_domain IS NULL, 1, 0))                     AS internal_cancellations,
       SUM(IFF(sb.cancellation_reason = 'MEMBER_CANCELLATION_REQUEST'
                   AND sb.cancellation_requested_by_domain IS DISTINCT FROM 'secretescapes.com' AND sb.cancellation_requested_by_domain IS NOT NULL, 1, 0)) AS member_cancellations,
       SUM(IFF(sb.cancellation_reason IS DISTINCT FROM 'MEMBER_CANCELLATION_REQUEST', 1, 0))                                                                AS other_cancellations
FROM data_vault_mvp.dwh.se_booking sb
WHERE sb.booking_status IN ('REFUNDED', 'CANCELLED')
AND sb.cancellation_tstamp >= '2020-01-01'
GROUP BY 1;

--excluding staff bookings
SELECT sb.cancellation_tstamp::DATE                                                                                                                         AS cancellation_date,
       SUM(IFF(sb.cancellation_reason = 'MEMBER_CANCELLATION_REQUEST'
                   AND sb.cancellation_requested_by_domain = 'secretescapes.com' OR sb.cancellation_requested_by_domain IS NULL, 1, 0))                     AS internal_cancellations,
       SUM(IFF(sb.cancellation_reason = 'MEMBER_CANCELLATION_REQUEST'
                   AND sb.cancellation_requested_by_domain IS DISTINCT FROM 'secretescapes.com' AND sb.cancellation_requested_by_domain IS NOT NULL, 1, 0)) AS member_cancellations,
       SUM(IFF(sb.cancellation_reason IS DISTINCT FROM 'MEMBER_CANCELLATION_REQUEST', 1, 0))                                                                AS other_cancellations
FROM data_vault_mvp.dwh.se_booking sb
    INNER JOIN data_vault_mvp.dwh.user_attributes ua ON sb.shiro_user_id = ua.shiro_user_id
WHERE sb.booking_status IN ('REFUNDED', 'CANCELLED')
  AND SPLIT_PART(ua.email, '@', -1) IS DISTINCT FROM 'secretescapes.com' --remove staff bookings
AND sb.cancellation_tstamp >= '2020-01-01'
GROUP BY 1;

SELECT ua.email,
       SPLIT_PART(ua.email, '@', -1) AS email_domain
FROM data_vault_mvp.dwh.user_attributes ua;

SELECT * FROM se.data.tb_order_item_changelog toic ;