SELECT sa.airline_id,
       a.airline_name,
       MAX(sa.tickets)                   AS ticket_limit,
       COUNT(DISTINCT toi.order_item_id) AS order_items,
       sa.maximum_policy_currency,
       MAX(sa.maximum_policy_limit)      AS maximum_policy_limit,
       SUM(toi.sold_price_incl_vat_gbp)  AS total_sold_price_gbp
FROM se.finance.aviate_transactions a
         INNER JOIN hygiene_snapshot_vault_mvp.finance_gsheets.safi_airlines sa ON
        a.arl_id = sa.airline_id AND
        a.transaction_tstamp::DATE BETWEEN sa.safi_start_date AND sa.safi_end_date
         INNER JOIN data_vault_mvp.dwh.tb_order_item toi ON a.pnr_ref = toi.flight_reservation_number

WHERE se.finance.travel_trust_booking(a.tb_order_id)
  AND se.finance.safi_protected_airline(a.arl_id, a.transaction_tstamp::DATE)
  AND a.cashflow_direction = 'money out'
  AND toi.end_date < CURRENT_DATE
  AND toi.order_item_created_tstamp >= '2021-06-14'
GROUP BY sa.airline_id, a.airline_name, sa.maximum_policy_currency;



SELECT sa.airline_id,
       a.airline_name,
       MAX(sa.tickets)                   AS ticket_limit,
       COUNT(DISTINCT toi.order_item_id) AS order_items,
       sa.maximum_policy_currency,
       MAX(sa.maximum_policy_limit)      AS maximum_policy_limit,
       SUM(toi.sold_price_incl_vat_gbp)  AS total_sold_price_gbp
FROM se.finance.aviate_transactions a
         INNER JOIN hygiene_snapshot_vault_mvp.finance_gsheets.safi_airlines sa ON
        a.arl_id = sa.airline_id AND
        a.transaction_tstamp::DATE BETWEEN sa.safi_start_date AND sa.safi_end_date
         INNER JOIN data_vault_mvp.dwh.tb_order_item toi ON a.pnr_ref = toi.flight_reservation_number

WHERE se.finance.travel_trust_booking(a.tb_order_id)
  AND se.finance.safi_protected_airline(a.arl_id, a.transaction_tstamp::DATE)
  AND a.cashflow_direction = 'money out'
  AND toi.end_date < CURRENT_DATE
GROUP BY sa.airline_id, a.airline_name, sa.maximum_policy_currency;



SELECT sa.airline_id,
       a.airline_name,
       MAX(sa.tickets)                   AS ticket_limit,
       COUNT(DISTINCT toi.order_item_id) AS order_items,
       sa.maximum_policy_currency,
       MAX(sa.maximum_policy_limit)      AS maximum_policy_limit,
       SUM(toi.sold_price_incl_vat_gbp)  AS total_sold_price_gbp
FROM se.finance.aviate_transactions a
         INNER JOIN hygiene_snapshot_vault_mvp.finance_gsheets.safi_airlines sa ON
        a.arl_id = sa.airline_id AND
        a.transaction_tstamp::DATE BETWEEN sa.safi_start_date AND sa.safi_end_date
         INNER JOIN data_vault_mvp.dwh.tb_order_item toi ON a.pnr_ref = toi.flight_reservation_number

WHERE se.finance.travel_trust_booking(a.tb_order_id)
  AND se.finance.safi_protected_airline(a.arl_id, a.transaction_tstamp::DATE)
  AND a.cashflow_direction = 'money out'
  AND toi.end_date < CURRENT_DATE
GROUP BY sa.airline_id, a.airline_name, sa.maximum_policy_currency;

SELECT safi_airlines.tickets,
       safi_airlines.turnover,
       safi_airlines.maximum_policy_limit,
       safi_airlines.airline_id,
       safi_airlines.airline_name,
       safi_airlines.safi_start_date,
       safi_airlines.safi_end_date,
       safi_airlines.tickets__o,
       safi_airlines.turnover_currency,
       safi_airlines.turnover__o,
       safi_airlines.maximum_policy_currency,
       safi_airlines.maximum_policy_limit__o
FROM hygiene_snapshot_vault_mvp.finance_gsheets.safi_airlines;



SELECT *
FROM se.finance.aviate_transactions a
         INNER JOIN data_vault_mvp.dwh.tb_order_item toi ON a.pnr_ref = toi.flight_reservation_number

WHERE se.finance.travel_trust_booking(a.tb_order_id)
  AND se.finance.safi_protected_airline(a.arl_id, a.transaction_tstamp::DATE)
  AND a.cashflow_direction = 'money out'
  AND toi.end_date < CURRENT_DATE;

self_describing_task --include '/se/data/udfs/udf_functions.py'  --method 'run' --start '2021-06-17 00:00:00' --end '2021-06-17 00:00:00'

SELECT sts.event_tstamp::DATE,
       COUNT(*)
FROM se.data.scv_touched_spvs sts
WHERE sts.event_tstamp >= CURRENT_DATE - 10
GROUP BY 1;


SELECT toi.flight_validating_airline_id,
       a.airline_name,
       se.data.safi_protected_airline(a.arl_id, a.transaction_tstamp::DATE) AS safi_protected,
       MAX(sa.tickets)                   AS ticket_limit,
       COUNT(DISTINCT a.document_number) AS tickets_sold,
       sa.maximum_policy_currency,
       MAX(sa.maximum_policy_limit)      AS maximum_policy_limit,
       SUM(toi.sold_price_incl_vat_gbp)  AS total_sold_price_gbp
FROM data_vault_mvp.finance.aviate_transactions a
         LEFT JOIN hygiene_snapshot_vault_mvp.finance_gsheets.safi_airlines sa ON
        a.arl_id = sa.airline_id AND
        a.transaction_tstamp::DATE BETWEEN sa.safi_start_date AND sa.safi_end_date
         INNER JOIN data_vault_mvp.dwh.tb_order_item toi ON a.pnr_ref = toi.flight_reservation_number

WHERE se.data.travel_trust_booking(a.tb_order_id)
  AND a.cashflow_direction = 'money out'
  AND toi.start_date > CURRENT_DATE --not yet flown
  AND toi.order_item_created_tstamp >= '2021-06-14'
GROUP BY 1,2,3,6;

SELECT * FROM data_vault_mvp.finance.aviate_transactions a

airflow backfill --start_date '2021-06-17 00:00:00' --end_date '2021-06-18 00:00:00' --task_regex '.*' dwh__cash_flow__aviate_transactions__daily_at_03h00