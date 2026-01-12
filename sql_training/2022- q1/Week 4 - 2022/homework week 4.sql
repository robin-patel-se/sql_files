-- homework functions

SELECT sb.booking_id,
       sb.booking_completed_date,
       MONTH(sb.booking_completed_date)                                                  AS booking_month,
       YEAR(sb.booking_completed_date)                                                   AS booking_year,
       sb.check_in_date,
       sb.check_out_date,
       DATEDIFF(DAY, sb.booking_completed_date, sb.check_out_date)                       AS book_to_check_out_days,
       CASE
           WHEN book_to_check_out_days < 7 THEN 'within 1 week'
           WHEN book_to_check_out_days < 14 THEN 'within 2 weeks'
           ELSE 'more than 2 weeks'
           END                                                                           AS book_to_check_out_buckets,
       IFF(sb.margin_gross_of_toms_gbp / sb.gross_booking_value_gbp > 0.18, TRUE, FALSE) AS more_than_15_perc_margin
FROM se.data.se_booking sb
WHERE sb.booking_status = 'COMPLETE'
  AND sb.booking_completed_date >= CURRENT_DATE - 1
;
