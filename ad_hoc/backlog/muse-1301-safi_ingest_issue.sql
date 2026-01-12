SELECT *
FROM hygiene_snapshot_vault_mvp.finance_gsheets.safi_airlines sa;
------------------------------------------------------------------------------------------------------------------------
--original code
-- SELECT toi.flight_validating_airline_id,
--        a.airline_name,
--        se.data.safi_protected_airline(toi.flight_validating_airline_id, a.transaction_tstamp::DATE) AS safi_protected,
--        MAX(sa.tickets)                                                                              AS ticket_limit,
--        COUNT(DISTINCT a.document_number)                                                            AS tickets_sold,
--        sa.maximum_policy_currency,
--        MAX(sa.maximum_policy_limit)                                                                 AS maximum_policy_limit,
--        SUM(IFF(a.transaction_currency = 'GBP', a.transaction_amount, 0))                            AS total_sold_price_gbp
-- FROM data_vault_mvp.finance.aviate_transactions a
--     LEFT JOIN  hygiene_snapshot_vault_mvp.finance_gsheets.safi_airlines sa ON
--             a.arl_id = sa.airline_id AND
--             a.transaction_tstamp::DATE BETWEEN sa.safi_start_date AND sa.safi_end_date
--     INNER JOIN data_vault_mvp.dwh.tb_order_item toi ON a.pnr_ref = toi.flight_reservation_number
--
-- WHERE se.data.travel_trust_booking(a.tb_order_id)
--   AND se.data.safi_protected_airline(toi.flight_validating_airline_id, a.transaction_tstamp::DATE)
--   AND a.cashflow_direction = 'money out'
--   AND toi.start_date > CURRENT_DATE --not yet flown
--   AND toi.order_item_created_tstamp >= '2021-06-14'
-- GROUP BY 1, 2, 3, 6

------------------------------------------------------------------------------------------------------------------------

SELECT toi.flight_validating_airline_id,
       sa.airline_name,
       se.data.safi_protected_airline(toi.flight_validating_airline_id, a.transaction_tstamp::DATE) AS safi_protected,
       MAX(sa.tickets)                                                                              AS ticket_limit,
       COUNT(DISTINCT a.document_number)                                                            AS tickets_sold,
       sa.maximum_policy_currency,
       MAX(sa.maximum_policy_limit)                                                                 AS maximum_policy_limit,
       SUM(IFF(a.transaction_currency = 'GBP', a.transaction_amount, 0))                            AS total_sold_price_gbp
FROM data_vault_mvp.finance.aviate_transactions a
    INNER JOIN data_vault_mvp.dwh.tb_order_item toi ON a.pnr_ref = toi.flight_reservation_number
    LEFT JOIN hygiene_snapshot_vault_mvp.finance_gsheets.safi_airlines sa ON
            toi.flight_validating_airline_id = sa.airline_id AND
            a.transaction_tstamp::DATE BETWEEN sa.safi_start_date AND sa.safi_end_date

WHERE se.data.travel_trust_booking(a.tb_order_id)
  AND se.data.safi_protected_airline(toi.flight_validating_airline_id, a.transaction_tstamp::DATE)
  AND a.cashflow_direction = 'money out'
  AND toi.start_date > CURRENT_DATE --not yet flown
  AND toi.order_item_created_tstamp >= '2021-06-14'
GROUP BY 1, 2, 3, 6


------------------------------------------------------------------------------------------------------------------------
SELECT toi.flight_validating_airline_id,
       toi.order_id,
       sa.airline_name,
       se.data.safi_protected_airline(toi.flight_validating_airline_id, a.transaction_tstamp::DATE) AS safi_protected,
       sa.tickets,
       a.document_number,
       sa.maximum_policy_currency,
       sa.maximum_policy_limit,
       IFF(a.transaction_currency = 'GBP', a.transaction_amount, 0) AS total_sold_price_gbp
FROM data_vault_mvp.finance.aviate_transactions a
    INNER JOIN data_vault_mvp.dwh.tb_order_item toi ON a.pnr_ref = toi.flight_reservation_number
    LEFT JOIN hygiene_snapshot_vault_mvp.finance_gsheets.safi_airlines sa ON
            toi.flight_validating_airline_id = sa.airline_id AND
            a.transaction_tstamp::DATE BETWEEN sa.safi_start_date AND sa.safi_end_date
WHERE se.data.travel_trust_booking(a.tb_order_id)
  AND se.data.safi_protected_airline(toi.flight_validating_airline_id, a.transaction_tstamp::DATE)
  AND a.cashflow_direction = 'money out'
  AND toi.start_date > CURRENT_DATE --not yet flown
  AND toi.order_item_created_tstamp >= '2021-06-14'
AND toi.order_id = '21913255';


SELECT * FROM se.data.tb_order_item toi WHERE toi.order_id = '21913255';

SELECT * FROM data_vault_mvp.finance.aviate_transactions a WHERE a.pnr_ref = 'WCAHKO';

