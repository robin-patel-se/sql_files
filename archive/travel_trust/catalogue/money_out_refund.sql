-- Stripe Refund

SELECT sr.transaction_id,
       sr.transaction_tstamp,
       sr.payment_service_provider,
       sr.payment_service_provider_transaction_type,
       sr.cashflow_direction,
       sr.cashflow_type,
       'refund'                AS money_out_type,
       sr.transaction_amount   AS settlement_amount,
       sr.transaction_currency AS settlement_currency,
       tb.booking_id,
       tb.travel_date,
       tb.return_date,
       tb.booking_includes_flight
FROM data_vault_mvp.finance.stripe_refund sr
         LEFT JOIN data_vault_mvp.dwh.tb_booking tb ON sr.booking_id = tb.booking_id
WHERE se.finance.travel_trust_booking(sr.tb_order_id)
;

-- Stripe Chargeback
SELECT sc.transaction_id,
       sc.transaction_tstamp,
       sc.payment_service_provider,
       sc.payment_service_provider_transaction_type,
       sc.cashflow_direction,
       sc.cashflow_type,
       'refund'                AS money_out_type,
       sc.transaction_amount   AS settlement_amount,
       sc.transaction_currency AS settlement_currency,
       tb.booking_id,
       tb.travel_date,
       tb.return_date,
       tb.booking_includes_flight
FROM data_vault_mvp.finance.stripe_chargeback sc
         LEFT JOIN data_vault_mvp.dwh.tb_booking tb ON sc.booking_id = tb.booking_id
WHERE se.finance.travel_trust_booking(sc.tb_order_id)
  AND sc.transaction_dispute_status = 'lost'
;

-- SVB Manual Refund
SELECT smr.transaction_id,
       smr.transaction_tstamp,
       smr.payment_service_provider,
       smr.payment_service_provider_transaction_type,
       smr.cashflow_direction,
       smr.cashflow_type,
       'refund'                 AS money_out_type,
       smr.transaction_amount   AS settlement_amount,
       smr.transaction_currency AS settlement_currency,
       tb.booking_id,
       tb.travel_date,
       tb.return_date,
       tb.booking_includes_flight
FROM data_vault_mvp.finance.svb_manual_refund smr
         LEFT JOIN data_vault_mvp.dwh.tb_booking tb ON smr.booking_id = tb.booking_id
WHERE se.finance.travel_trust_booking(smr.tb_order_id)
;

self_describing_task --include 'dv/finance/cash_flow/travel_trust_money_out_refund.py'  --method 'run' --start '2021-06-15 00:00:00' --end '2021-06-15 00:00:00'

airflow backfill --start_date '2021-06-15 03:00:00' --end_date '2021-06-15 03:00:00' --task_regex '.*' finance__cash_flow__travel_trust_money_out_refund__daily_at_03h00