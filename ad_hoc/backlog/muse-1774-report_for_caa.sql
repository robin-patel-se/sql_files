-- As every month I would need to reply to some questions like:
-- cancellations (need to split SE and TB due to cancellation and atol amounts)
-- 1. Number of refunds paid immediately with no ATOL COVID-19 RCN issue
-- 2. Number of Passengers included on these refunded bookings
-- 3. Actual Cash Value of the refunds made (£)
-- 4. Gross Invoice Value of these refunded bookings (£)

-- 5. Total gross invoice value of all ATOL protected bookings due to depart (exclude unredeemed RCNs) (£)
-- 6. Total value of RCNs redeemed against forward ATOL protected bookings (£)
-- 7. Total value of customer payments received against forward ATOL protected bookings (exclude RCNs redeemed) (£)
-- 8. Total value of customer balances outstanding in respect of forward ATOL protected bookings (£)
-- 9. Total value of payments already made to suppliers in respect of forward ATOL protected bookings (£)
-- 10. Total balances that are/will fall due to suppliers in respect of forward ATOL protected bookings (£)

--SE cancellations
SELECT sb.booking_id,
       sb.currency,
       sb.territory,
       ssa.posu_country,
       sb.gross_revenue_cc,
       sb.gross_revenue_gbp,
       sb.cancellation_date,
       sb.cancellation_reason,
--        sb.cancellation_refund_channel,
--        sb.cancellation_fault,
--        sb.cancellation_requested_by_domain,
       sb.total_refunded_cc,
       sb.total_refunded_gbp,
       IFF(sb.atol_fee_gbp > 0, TRUE, FALSE) AS is_atol_protected_booking,
       sb.atol_fee_gbp,
       sb.has_flights,
       sb.adult_guests,
       sb.child_guests

FROM se.data.se_booking sb
    LEFT JOIN se.data.se_sale_attributes ssa ON sb.se_sale_id = ssa.se_sale_id
WHERE
  --cancellations that occurred last month
    sb.cancellation_date BETWEEN DATE_TRUNC('month', DATEADD(DAY, -30, CURRENT_DATE))
        AND DATEADD(DAY, -1, DATE_TRUNC('month', CURRENT_DATE))
  AND sb.has_flights
;

SELECT tb.booking_id,
       tb.sold_price_currency,

FROM se.data.tb_booking tb
    LEFT JOIN se.data.tb_offer t ON tb.se_sale_id = t.se_sale_id
WHERE
  --cancellations that occurred last month
    tb.cancellation_date BETWEEN DATE_TRUNC('month', DATEADD(DAY, -30, CURRENT_DATE))
        AND DATEADD(DAY, -1, DATE_TRUNC('month', CURRENT_DATE))
  AND tb.booking_includes_flight

SELECT *
FROM se.data.tb_booking tb
WHERE order_status = 'CANCELLED';

SELECT *
FROM se.data.tb_order_item_changelog toic
WHERE toic.booking_id = 'TB-21936234'

;
self_describing_task --include 'dv/dwh/transactional/se_booking.py'  --method 'run' --start '2022-02-15 00:00:00' --end '2022-02-15 00:00:00'
self_describing_task --include 'dv/dwh/transactional/fact_booking.py'  --method 'run' --start '2022-02-15 00:00:00' --end '2022-02-15 00:00:00'
self_describing_task --include 'se/data/dwh/fact_booking.py'  --method 'run' --start '2022-02-15 00:00:00' --end '2022-02-15 00:00:00'
self_describing_task --include 'se/data/dwh/fact_complete_booking.py'  --method 'run' --start '2022-02-15 00:00:00' --end '2022-02-15 00:00:00'

CREATE VIEW data_vault_mvp_dev_robin.dwh.dim_sale as SELE


