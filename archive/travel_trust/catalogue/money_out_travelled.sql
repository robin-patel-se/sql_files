SELECT *
FROM se.finance.travel_trust_money_out ttmo;


WITH booking_money_in AS (
    SELECT ttmi.booking_id,
           ttmi.transaction_currency AS money_in_currency,
           SUM(ttmi.transaction_amount) AS total_travel_trust_money_in
    FROM data_vault_mvp.finance.travel_trust_money_in ttmi
    GROUP BY 1, 2
)


SELECT tb.transaction_id,
       tb.return_date::TIMESTAMP AS transaction_tstamp,
       'travelbird cms'          AS payment_service_provider,
       'order'                   AS payment_service_provider_transaction_type,
       'money out'               AS cashflow_direction,
       'booking travelled'       AS cashflow_type,
       'travelled'               AS money_out_type,
       bmi.total_travel_trust_money_in,
       bmi.money_in_currency,
       tb.sold_price_total_cc,
       tb.sold_price_currency,
       tb.booking_id,
       tb.travel_date,
       tb.return_date,
       tb.booking_includes_flight
FROM data_vault_mvp.dwh.tb_booking tb
         LEFT JOIN booking_money_in bmi ON tb.booking_id = bmi.booking_id
WHERE tb.payment_status IN ('AUTHORISED', 'PARTIAL_PAID', 'LATE')
  AND se.finance.travel_trust_booking(tb.order_id);


self_describing_task --include 'dv/finance/cash_flow/travel_trust_money_out_travelled.py'  --method 'run' --start '2021-06-15 00:00:00' --end '2021-06-15 00:00:00'

