SELECT toi.flight_validating_airline_id,
       a.airline_name,
       se.data.safi_protected_airline(toi.flight_validating_airline_id, a.transaction_tstamp::DATE) AS safi_protected,
       MAX(sa.tickets)                                                                              AS ticket_limit,
       COUNT(DISTINCT a.document_number)                                                            AS tickets_sold,
       sa.maximum_policy_currency,
       MAX(sa.maximum_policy_limit)                                                                 AS maximum_policy_limit,
       SUM(IFF(a.transaction_currency = 'GBP', a.transaction_amount, 0))                            AS total_sold_price_gbp
FROM data_vault_mvp.finance.aviate_transactions a
    LEFT JOIN  hygiene_snapshot_vault_mvp.finance_gsheets.safi_airlines sa ON
            a.arl_id = sa.airline_id AND
            a.transaction_tstamp::DATE BETWEEN sa.safi_start_date AND sa.safi_end_date
    INNER JOIN data_vault_mvp.dwh.tb_order_item toi ON a.pnr_ref = toi.flight_reservation_number

WHERE se.data.travel_trust_booking(a.tb_order_id)
  AND se.finance.safi_protected_airline(toi.flight_validating_airline_id, a.transaction_tstamp::DATE)
  AND a.cashflow_direction = 'money out'
  AND toi.start_date > CURRENT_DATE --not yet flown
  AND toi.order_item_created_tstamp >= '2021-06-14'
GROUP BY 1, 2, 3, 6;

SELECT a.transaction_id,
       a.transaction_amount,
       toi.changelog_id,
       toi.booking_id,
       toi.order_id,
       toi.event_id,
       toi.event_type,
       toi.event_type_category,
       toi.event_created_tstamp,
       toi.event_data,
       toi.order_adjustment_type,
       toi.adjustment_reason,
       toi.order_item_event,
       toi.order_item_change_type,
       toi.order_item_id,
       toi.order_item_created_tstamp,
       toi.order_item_updated_tstamp,
       toi.sold_price_incl_vat,
       toi.sold_price_currency,
       toi.sold_price_to_eur_exchange_rate,
       toi.sold_price_incl_vat_eur,
       toi.sold_price_incl_vat_gbp,
       toi.vat_percentage,
       toi.commission_sold_price_incl_vat_in_cost_currency,
       toi.cost_price_excl_vat,
       toi.cost_price_currency,
       toi.cost_currency_to_sold_currency_exchange_rate,
       toi.cost_price_excl_vat_sold_currency,
       toi.cost_price_excl_vat_eur,
       toi.cost_price_excl_vat_gbp,
       toi.start_date,
       toi.end_date,
       toi.reason,
       toi.order_item_type,
       toi.order_item_type_id,
       toi.main_order_item_type,
       toi.main_order_item_type_id,
       toi.partner_id,
       toi.partner_name,
       toi.is_cancellable_with_partner,
       toi.allocation_board_id,
       toi.allocation_unit_id,
       toi.supplier_reference,
       toi.flight_reservation_number,
       toi.within_event_index,
       toi.flight_provider,
       toi.flight_departure_datetime,
       toi.flight_departure_arrival_datetime,
       toi.flight_return_datetime,
       toi.flight_return_arrival_datetime,
       toi.flight_number_of_adults,
       toi.flight_number_of_children,
       toi.flight_number_of_infants,
       toi.flight_origin_airport_id,
       toi.flight_destination_airport_id,
       toi.flight_validating_airline_id,
       toi.flight_release_datetime,
       toi.flight_released,
       toi.flight_connection_hash
FROM data_vault_mvp.finance.aviate_transactions a
    LEFT JOIN  hygiene_snapshot_vault_mvp.finance_gsheets.safi_airlines sa ON
            a.arl_id = sa.airline_id AND
            a.transaction_tstamp::DATE BETWEEN sa.safi_start_date AND sa.safi_end_date
    INNER JOIN data_vault_mvp.dwh.tb_order_item toi ON a.pnr_ref = toi.flight_reservation_number
WHERE se.data.travel_trust_booking(a.tb_order_id)
  AND a.cashflow_direction = 'money out'
  AND a.arl_id = 'FI';


SELECT a.transaction_id,
       a.transaction_tstamp,
       a.payment_service_provider,
       a.payment_service_provider_transaction_type,
       a.cashflow_direction,
       a.cashflow_type,
       a.transaction_amount,
       a.transaction_currency,
       a.transaction_type,
       a.booking_ref_date,
       a.stm_id,
       a.trading_name,
       a.external_gds_name,
       a.airline_name,
       a.arl_id,
       a.pnr_ref,
       a.booking_id,
       a.tb_order_id,
       a.order_item_id,
       a.document_number,
       a.emd,
       a.pax_count,
       a.pty_id,
       a.tour_op_ref,
       a.pnr_created_on,
       a.ticket_issue_date,
       a.pnr_departure_date,
       a.pnr_return_date,
       a.pnr_return_arrival_day,
       a.sectors,
       a.atol_fees,
       a.total_taxes,
       a.tax_breakdown,
       a.total_net_fare,
       a.luggage_fee,
       a.ancillary_fees,
       a.service_fee,
       a.ticketing_deadline,
       a.external_status_name,
       a.invoice_date,
       a.invoice_ref
FROM data_vault_mvp.finance.aviate_transactions a
WHERE a.pnr_ref = 'VLILQP'