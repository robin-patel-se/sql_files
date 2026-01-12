SELECT *
FROM hygiene_snapshot_vault_mvp.aviate.tig_transaction_report ttr
WHERE ttr.pnr_ref = 'JQI34F'

SELECT *
FROM se.data.tb_order_item toi
WHERE toi.order_item_type = 'FLIGHT';

--if transaction has document number it means we've paid for the flight
--transaction type 'TICKETD' only
SELECT DISTINCT transaction_type
FROM hygiene_snapshot_vault_mvp.aviate.tig_transaction_report ttr;

-- TRANSACTION_TYPE
-- BOOKING -ignore
-- TICKETD
-- TREFUND
-- REISSUE
-- PTAMEND

SELECT *
FROM hygiene_snapshot_vault_mvp.aviate.tig_transaction_report ttr
WHERE ttr.pnr_ref = 'NF4WAT';

WITH most_recent_order_item AS (
    --retrieve the most recent order item state
    SELECT oic.*
    FROM data_vault_mvp.dwh.tb_order_item_changelog oic
             LEFT JOIN data_vault_mvp.dwh.tb_booking tb ON oic.order_id = tb.order_id
    WHERE tb.payment_status IS DISTINCT FROM 'CANCELLED' -- PNR's can be reused if the original booking cancels
      AND oic.flight_reservation_number IS NOT NULL      -- can only join aviate data on pnr
        QUALIFY ROW_NUMBER()
                        OVER (PARTITION BY oic.order_item_id ORDER BY oic.order_item_updated_tstamp DESC, oic.within_event_index DESC) =
                1
),
     flatten_multiple_pnrs AS (
         SELECT REGEXP_REPLACE(REGEXP_REPLACE(mroi.flight_reservation_number, ' '), '[-|,|&]', '/') AS raw_pnr,
                pnr_split.value::VARCHAR                                                            AS pnr,
                mroi.order_id,
                mroi.order_item_id
         FROM most_recent_order_item mroi,
              LATERAL FLATTEN(INPUT =>
                              SPLIT(REGEXP_REPLACE(REGEXP_REPLACE(mroi.flight_reservation_number, ' '), '[-|,|&]', '/'), '/'),
                              OUTER => TRUE) pnr_split
     )
SELECT 'avi_' || ttr.transaction_id                                          AS transaction_id,
       ttr.transaction_date                                                  AS transaction_tstamp,
       'aviate'                                                              AS payment_service_provider,
       'transaction'                                                         AS payment_service_provider_transaction_type,
       IFF(ttr.total_transaction_fee < 0, 'money in', 'money out')           AS cashflow_direction,
       IFF(ttr.total_transaction_fee < 0, 'flight refund', 'flight payment') AS cashflow_type,
       ttr.total_transaction_fee                                             AS transaction_amount,
       ttr.cur_id                                                            AS transaction_currency,
       ttr.transaction_type,
       ttr.booking_ref_date,
       ttr.stm_id,
       ttr.trading_name,
       ttr.external_gds_name,
       ttr.airline_name,
       ttr.arl_id,
       ttr.pnr_ref,
       'TB-' || pnrs.order_item_id                                           AS booking_id,
       pnrs.order_id,
       pnrs.order_item_id,
       ttr.document_number,
       ttr.emd,
       ttr.pax_count,
       ttr.pty_id,
       ttr.tour_op_ref,
       ttr.pnr_created_on,
       ttr.ticket_issue_date,
       ttr.pnr_departure_date,
       ttr.pnr_return_date,
       ttr.pnr_return_arrival_day,
       ttr.sectors,

       ttr.atol_fees,
       ttr.total_taxes,
       ttr.tax_breakdown,
       ttr.total_net_fare,
       ttr.luggage_fee,
       ttr.ancillary_fees,
       ttr.service_fee,

       ttr.ticketing_deadline,
       ttr.external_status_name,
       ttr.invoice_date,
       ttr.invoice_ref
FROM hygiene_snapshot_vault_mvp.aviate.tig_transaction_report ttr
         LEFT JOIN flatten_multiple_pnrs pnrs ON ttr.pnr_ref = pnrs.pnr
WHERE ttr.transaction_type IS DISTINCT FROM 'BOOKING'
;


SELECT GET_DDL('table', 'scratch.robinpatel.aviate_test');

CREATE OR REPLACE TABLE aviate_test
(
    transaction_id                            VARCHAR,
    transaction_tstamp                        TIMESTAMP,
    payment_service_provider                  VARCHAR,
    payment_service_provider_transaction_type VARCHAR,
    cashflow_direction                        VARCHAR,
    cashflow_type                             VARCHAR,
    transaction_amount                        NUMBER,
    transaction_currency                      VARCHAR,
    transaction_type                          VARCHAR,
    booking_ref_date                          TIMESTAMP,
    stm_id                                    VARCHAR,
    trading_name                              VARCHAR,
    external_gds_name                         VARCHAR,
    airline_name                              VARCHAR,
    arl_id                                    VARCHAR,
    pnr_ref                                   VARCHAR,
    booking_id                                VARCHAR,
    order_id                                  NUMBER,
    document_number                           NUMBER,
    emd                                       VARCHAR,
    pax_count                                 NUMBER,
    pty_id                                    VARCHAR,
    tour_op_ref                               VARCHAR,
    pnr_created_on                            TIMESTAMP,
    ticket_issue_date                         TIMESTAMP,
    pnr_departure_date                        TIMESTAMP,
    pnr_return_date                           TIMESTAMP,
    pnr_return_arrival_day                    VARCHAR,
    sectors                                   VARCHAR,
    atol_fees                                 NUMBER,
    total_taxes                               NUMBER,
    tax_breakdown                             VARCHAR,
    total_net_fare                            NUMBER,
    luggage_fee                               NUMBER,
    ancillary_fees                            NUMBER,
    service_fee                               NUMBER,
    ticketing_deadline                        TIMESTAMP,
    external_status_name                      VARCHAR,
    invoice_date                              TIMESTAMP,
    invoice_ref                               VARCHAR
);

WITH most_recent AS (
    SELECT *
    FROM data_vault_mvp.dwh.tb_order_item_changelog oic
        QUALIFY ROW_NUMBER()
                        OVER (PARTITION BY oic.order_item_id ORDER BY oic.order_item_updated_tstamp DESC, oic.within_event_index DESC) =
                1
)
SELECT mr.flight_reservation_number,
       COUNT(*)
FROM most_recent mr
GROUP BY 1
HAVING COUNT(*) > 1
ORDER BY 2 DESC;


SELECT *
FROM se.data.tb_order_item toi
         LEFT JOIN se.data.tb_booking tb ON tb.order_id = toi.order_id
WHERE toi.flight_reservation_number = 'UMXRFR';

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_booking CLONE data_vault_mvp.dwh.tb_booking;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_order_item_changelog CLONE data_vault_mvp.dwh.tb_order_item_changelog;
CREATE SCHEMA hygiene_snapshot_vault_mvp_dev_robin.aviate;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.aviate.tig_transaction_report CLONE hygiene_snapshot_vault_mvp.aviate.tig_transaction_report;

self_describing_task --include 'dv/finance/aviate/transactions.py'  --method 'run' --start '2021-05-05 00:00:00' --end '2021-05-05 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.finance.aviate_transactions;

SELECT *
FROM data_vault_mvp.dwh.tb_order_item_changelog toic
WHERE toic.flight_reservation_number LIKE '%J4AN5E%';


WITH most_recent_order_item AS (
    --retrieve the most recent order item state
    SELECT oic.*
    FROM data_vault_mvp.dwh.tb_order_item_changelog oic
             LEFT JOIN data_vault_mvp.dwh.tb_booking tb ON oic.order_id = tb.order_id
    WHERE tb.payment_status IS DISTINCT FROM 'CANCELLED' -- PNR's can be reused if the original booking cancels
      AND oic.flight_reservation_number IS NOT NULL
        QUALIFY ROW_NUMBER()
                        OVER (PARTITION BY oic.order_item_id ORDER BY oic.order_item_updated_tstamp DESC, oic.within_event_index DESC) =
                1
)
SELECT REGEXP_REPLACE(REGEXP_REPLACE(mroi.flight_reservation_number, ' '), '[-|,|&]', '/') AS raw_pnr,
       pnr_split.value::VARCHAR                                                            AS pnr,
       mroi.order_id,
       mroi.order_item_id
FROM most_recent_order_item mroi,
     LATERAL FLATTEN(INPUT => SPLIT(REGEXP_REPLACE(REGEXP_REPLACE(mroi.flight_reservation_number, ' '), '[-|,|&]', '/'), '/'),
                     OUTER => TRUE) pnr_split;



SELECT *
FROM data_vault_mvp_dev_robin.finance.aviate_transactions__step01__most_recent_order_item
WHERE flight_reservation_number = 'NF4WAT';

SELECT * FROM data_vault_mvp_dev_robin.finance.aviate_transactions__step02__flatten_multple_pnrs WHERE pnr = 'NF4WAT';

SELECT * FROM data_vault_mvp_dev_robin.finance.aviate_transactions a
LEFT JOIN data_vault_mvp_dev_robin.finance.aviate_transactions__step02__flatten_multple_pnrs pnr ON a.pnr_ref = pnr.pnr
WHERE a.pnr_ref = 'NF4WAT';


SELECT * FROM data_vault_mvp_dev_robin.finance.aviate_transactions a WHERE a.pnr_ref = 'NF4WAT'

self_describing_task --include 'se/finance/travel_trust/aviate_transactions.py'  --method 'run' --start '2021-05-05 00:00:00' --end '2021-05-05 00:00:00'

SELECT * FROM se_dev_robin.finance.aviate_transactions;