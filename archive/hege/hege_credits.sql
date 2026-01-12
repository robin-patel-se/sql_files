SELECT *
FROM se.data.se_credit_model sc
WHERE sc.credit_date_created >= '2020-04-01'
  AND sc.credit_type = 'EXTRA_REFUND_CREDIT';

--% breakage of non covid time credits

SELECT *
FROM se.data.se_booking_summary_extended sbse;

SELECT *
FROM se.data.se_booking sb
WHERE sb.booking_status IN ('COMPLETE', 'REFUNDED');

WITH credit AS (
    SELECT sc.credit_id,
           sc.credit_type,
           sc.credit_amount,
           sc.credit_currency,
           sc.credit_amount_gbp,
           sb.booking_id,
           sb.gross_revenue_cc,
           sb.currency,
           sb.gross_revenue_gbp
    FROM se.data.se_credit sc
        INNER JOIN se.data.se_booking sb ON sc.redeemed_se_booking_id = sb.booking_id
    WHERE sc.credit_date_created <= CURRENT_DATE
      AND sc.credit_date_created >= '2020-03-01'
      AND sc.credit_amount_gbp >= 100
)
SELECT c.credit_type,
       COUNT(DISTINCT c.credit_id) AS credits,
       SUM(c.credit_amount_gbp)    AS credit_amount_gbp,
       SUM(c.gross_revenue_gbp)    AS redeemed_gross_revenue
FROM credit c
GROUP BY 1;

--breakage metrics
WITH breakage AS (
    SELECT sc.credit_id,
           sc.credit_date_created,
           sc.credit_type,
           sc.credit_amount,
           sc.credit_currency,
           sc.credit_amount_gbp
    FROM se.data.se_credit sc
    WHERE DATEADD(DAY, 365, sc.credit_date_created) < CURRENT_DATE
      AND sc.credit_date_created <= CURRENT_DATE
      AND sc.credit_date_created >= '2020-03-01'
      AND sc.credit_status = 'ACTIVE'
      AND sc.credit_type NOT IN ('GIFT_VOUCHER', 'VOUCHER_CREDIT')
)
SELECT b.credit_type,
       COUNT(DISTINCT b.credit_id) AS credits,
       SUM(b.credit_amount_gbp)    AS credit_amount_gbp
FROM breakage b
GROUP BY 1;


--
-- SELECT sc.credit_id,
--        sc.credit_type,
--        sc.credit_amount,
--        sc.credit_currency,
--        sc.credit_status,
--        sc.cc_rate_to_gbp,
--        sc.credit_amount_gbp,
--        sc.cc_rate_to_gbp_constant_currency,
--        sc.credit_amount_gbp_constant_currency,
--        sb.booking_id,
--        sb.gross_revenue_cc,
--        sb.currency,
--        sb.gross_revenue_gbp
-- FROM se.data.se_credit sc
--          LEFT JOIN se.data.se_booking sb ON sc.redeemed_se_booking_id = sb.booking_id
-- WHERE sc.credit_date_created < '2020-01-01'
--   AND sc.credit_date_created >= '2019-01-01';


SELECT ubr.booking_id,
       ubr.customer_score,
       ubr.follow_up_answer
FROM se.data.user_booking_review ubr
WHERE ubr.survey_source = 'survey_sparrow'
  AND ubr.follow_up_answer IS NOT NULL
