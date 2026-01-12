SELECT sb.transaction_id,
       sb.booking_id,
       sb.currency, -- customer/transaction currency
       sb.gross_revenue_cc,
       sb.sale_base_currency, -- supplier currency
       sb.gross_revenue_sc,
       sb.gross_revenue_gbp
FROM se.data.se_booking sb
WHERE sb.booking_status = 'COMPLETE'
  AND sb.booking_completed_date >= '2020-01-01'
  AND sb.booking_completed_date <= '2020-01-31'