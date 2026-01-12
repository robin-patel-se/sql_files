--credits prior to covid can't be assigned to an original booking

--booking cancellations
--se.data.credit_model

--we need to know how much we are liable for, for credits assigned from bookings and how much is active
--for bookings that originally transacted on worldpay, connect with

--first from_booking or from_reservation was populated 2020-03-31 14:50:52.000000000

SELECT scm.credit_status, count(*), sum(scm.credit_amount)
FROM se.data.se_credit_model scm
GROUP BY 1
;



SELECT scm.credit_type,
       scm.credit_currency,
       count(*)               AS credits,
       sum(scm.credit_amount) AS credit_amount
FROM se.data.se_credit_model scm
WHERE scm.credit_status = 'ACTIVE'
GROUP BY 1, 2
;

SELECT *
FROM se.data.se_credit_model scm
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.booking_cancellation_snapshot bcs
                   ON scm.original_booking_id = bcs.booking_id
WHERE scm.original_booking_id IS NOT NULL;

SELECT scm.credit_type,
       bcs.reason,
       count(*)
FROM se.data.se_credit_model scm
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.booking_cancellation_snapshot bcs
                   ON COALESCE(scm.original_booking_id::VARCHAR, 'A' || scm.original_reservation_id) =
                      COALESCE(bcs.booking_id::VARCHAR, 'A' || bcs.reservation_id)
WHERE scm.original_booking_id IS NOT NULL
   OR scm.original_reservation_id IS NOT NULL
GROUP BY 1, 2;

SELECT DISTINCT sbse.paymenttype
FROM se.data_pii.se_booking_summary_extended sbse;

SELECT c.id                                                                                       AS credit_id,
       c.billing_id                                                                               AS billing_id,
       c.version                                                                                  AS credit_version,
       su.id                                                                                      AS user_id,
       su.date_created                                                                            AS user_join_date,
       c.from_refunded_booking_id                                                                 AS original_booking_id,
       c.from_refunded_external_booking_id                                                        AS original_external_id,
       c.from_refunded_reservation_id                                                             AS original_reservation_id,
       COALESCE(c.from_refunded_booking_id::VARCHAR, CONCAT('A', c.from_refunded_reservation_id)) AS original_se_booking_id,
       v.id                                                                                       AS original_voucher_id,
       bc.booking_credits_used_id                                                                 AS redeemed_booking_id,
       rc.reservation_credits_used_id                                                             AS redeemed_reservation_id,
       COALESCE(redeemed_booking_id::VARCHAR, CONCAT('A', redeemed_reservation_id))               AS redeemed_se_booking_id,
       c.date_created                                                                             AS credit_date_created,
       tlc.expires_on                                                                             AS credit_expires_on,
       c.last_updated                                                                             AS credit_last_updated,
       c.type                                                                                     AS credit_type,
       c.status                                                                                   AS credit_status,
       c.reason                                                                                   AS credit_reason,
       t.name                                                                                     AS credit_territory,
       c.currency                                                                                 AS credit_currency,
       c.amount                                                                                   AS credit_amount
FROM data_vault_mvp.cms_mysql_snapshots.credit_snapshot c
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.booking_credit_snapshot bc
                   ON bc.credit_id = c.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.reservation_credit_snapshot rc
                   ON rc.credit_id = c.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.billing_snapshot bi
                   ON bi.id = c.billing_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot su
                   ON su.billing_id = bi.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.affiliate_snapshot a
                   ON a.id = su.affiliate_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot t
                   ON t.id = a.territory_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.time_limited_credit_snapshot tlc
                   ON tlc.id = c.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.voucher_snapshot v
                   ON v.credit_id = c.id
;

------------------------------------------------------------------------------------------------------------------------
--from the 1st of april 2020 we have joins to original booking so filtering to credits created since that date
--later will look at credits created before that date.

SELECT scm.credit_type,
       scm.credit_currency,
       SUM(IFF(scm.credit_status = 'ACTIVE', 1, 0))                 AS active_credits,
       SUM(IFF(scm.credit_status = 'ACTIVE', scm.credit_amount, 0)) AS active_credit_amount
FROM se.data.se_credit_model scm
WHERE scm.credit_date_created >= '2020-04-01'
  AND scm.original_se_booking_id IS NOT NULL
GROUP BY 1, 2;

--looks like credits are only given against bookings in credit_type: CANCELLATION_CREDIT, HOLD, REFUND
-- ^^ use this for looking at credits prior to 1st april 2020

SELECT status, count(*)
FROM hygiene_snapshot_vault_mvp.worldpay.transaction_summary
GROUP BY 1;


--booking ids that transacted using worldpay
SELECT sb.booking_id
FROM data_vault_mvp.dwh.se_booking sb
WHERE sb.unique_transaction_reference IN (
    SELECT DISTINCT order_code
    FROM hygiene_snapshot_vault_mvp.worldpay.transaction_summary
    WHERE LOWER(status) = 'authorised'
)
  AND sb.booking_status = 'COMPLETE'
GROUP BY 1;

--1,028,425 complete bookings that match worldpay transactions

WITH worldpay_bookings AS (
    SELECT sb.booking_id
    FROM data_vault_mvp.dwh.se_booking sb
    WHERE sb.unique_transaction_reference IN (
        SELECT DISTINCT order_code
        FROM hygiene_snapshot_vault_mvp.worldpay.transaction_summary
        WHERE LOWER(status) = 'authorised'
    )
)
SELECT scm.credit_currency,
       SUM(scm.credit_amount) AS total_credit_amount
FROM se.data.se_credit_model scm
         INNER JOIN worldpay_bookings wpb ON scm.original_se_booking_id = wpb.booking_id
WHERE scm.credit_status = 'ACTIVE'
GROUP BY 1;

------------------------------------------------------------------------------------------------------------------------
--for credits prior to 1st Apr 2020

SELECT cs.currency,
       fr.fx_rate,
       SUM(cs.amount)              AS total_amount,
       SUM(IFF(cs.currency='GBP',cs.amount,cs.amount * fr.fx_rate)) AS total_amount_gbp
FROM data_vault_mvp.cms_mysql_snapshots.credit_snapshot cs
         LEFT JOIN se.data.fx_rates fr
                   ON fr.fx_date = CURRENT_DATE AND cs.currency = fr.source_currency AND fr.target_currency = 'GBP'
WHERE cs.date_created < '2020-04-01'
  AND cs.type IN ('CANCELLATION_CREDIT', 'HOLD', 'REFUND')
  AND cs.status = 'ACTIVE'
GROUP BY 1, 2;


