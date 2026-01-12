--Master list
--Loads of sf requests to cancel rebook during covid
--one use case one: (is csvs):
--create a dataset that would automatically create csvs
--these csvs are split by hotel and then sorted into country folders
--the master list is a list of bookings regardless of if the customer has requested anything in sf
--checkins greater than or equal to the 17th of march and booked date less than or equal to the first of may

--to ask Jan do we still need these running? is it still just hotel only?

--use case two:
--Tech applied credits to package bookings, some customers don't want the credit, we can only give them the money back once we've
--recieved it back from the flight provider and the hotel, once we've got the money back we can refund the customer back. CS can
--use this dataset to see if they can refund the customer back.

--use case three:
--financial and management reporting, how many cancels, how many refunds, how many credits, how many enquiries.

--this is used in steerco (steer the company) reporting and CS are using it for ad hoc reporting.

--the current baked version (is used for csvs) only contains HO, the new baked version will contain everything can can be used for
--reporting/analytics and then discussion point regarding necessity of existing csv ho version.

--Jan is the super user,


--covid_master_list_ho_packages
--master table of this cms booking summary extended
--booking summary extended is the cms booking summary but enriched with dwh


--need to create assertions especially for gsheets

CREATE OR REPLACE VIEW collab.covid_pii.covid_master_list_ho_packages
    COPY GRANTS
AS
WITH combine_sales_force
         AS (
        SELECT *
        FROM raw_vault_mvp.sfsc.rebooking_request_cases_pkg
        UNION
        SELECT *
        FROM raw_vault_mvp.sfsc.rebooking_request_cases_ho
    ),
     step01__filter_internal
         AS (
         SELECT transaction_id,
                booking_id,
                booking_lookup_check_in_date, -- some records used to come in without :SS portion
                booking_lookup_check_out_date,
                booking_lookup_store_id,
                booking_lookup_supplier_territory,
                case_number::INT                               AS case_number,
                case_origin,
                case_owner_full_name,
                contact_reason,
                opportunity_sale_id,
                LOWER(status)                                  AS status,
                CASE
                    WHEN LOWER(status) = 'hold' THEN 1
                    WHEN LOWER(status) = 'pending' THEN 1
                    WHEN LOWER(status) = 'open' THEN 1
                    WHEN LOWER(status) = 'new' THEN 2
                    WHEN LOWER(status) = 'solved' THEN 3
                    WHEN LOWER(status) = 'closed' THEN 4
                    ELSE 99
                    END                                        AS status_rank,
                LOWER(subject)                                 AS subject,
                CASE
                    WHEN LOWER(subject) LIKE '%amend%' AND requested_rebooking_date IS NOT NULL
                        THEN 'Member asked for rebooking with date'
                    WHEN LOWER(subject) LIKE '%amend%' AND requested_rebooking_date IS NULL
                        THEN 'Member asked for rebooking without date'
                    WHEN LOWER(subject) LIKE '%refund%' THEN 'Member asked for refund'
                    WHEN LOWER(subject) LIKE '%storn%' THEN 'Member asked for refund'
                    WHEN LOWER(subject) LIKE '%cxl%' THEN 'Member asked for refund'
                    WHEN LOWER(subject) LIKE '%cancel%' THEN 'Member asked for refund'
                    ELSE 'Unknown' END                         AS status_se,
                "VIEW",
                TRY_CAST(postponed_booking_request AS BOOLEAN) AS postponed_booking_request,
                requested_rebooking_date,
                LOWER(last_modified_by_full_name)              AS last_modified_by_full_name,
                LOWER(overbooking_rebooking_stage)             AS overbooking_rebooking_stage,
                LOWER(reason)                                  AS reason,
                case_id,
                date_time_opened,
                case_name::INT                                 AS case_name,
                last_modified_date,
                last_modified_by_case_overview,
                priority_type,
                covid19_member_resolution_cs,
                case_overview_id,
                case_thread_id,
                priority,
                -- CS sometimes put booking id in transaction id field
                COALESCE(transaction_id, booking_id)           AS unique_transaction_id,
                -- hygiene flags
                CASE
                    WHEN unique_transaction_id IS NULL
                        THEN 1
                    END                                        AS fails_validation__unique_transaction_id__expected_nonnull,
                CASE
                    WHEN fails_validation__unique_transaction_id__expected_nonnull = 1
                        THEN 1
                    END                                        AS failed_some_validation,
                ROW_NUMBER() OVER (
                    PARTITION BY unique_transaction_id
                    ORDER BY
                        status_rank ASC,
                        case_number DESC,
                        case_name DESC
                    )                                          AS rank
         FROM combine_sales_force
         WHERE lower(last_modified_by_full_name) NOT IN ('dylan hone', 'kate donaghy', 'jessica ho')
           AND NOT (
                 lower(last_modified_by_full_name) = 'marta lagut'
                 AND case_name IS NULL
                 AND lower(status) = 'solved'
             )
           AND lower(status) != 'closed'
     ),
     duplicated_solved_count AS
         (
             SELECT unique_transaction_id,
                    SUM(CASE WHEN rank > 1 THEN 1 ELSE 0 END) AS number_dup_cases_solved
             FROM step01__filter_internal
             WHERE lower(status) = 'solved'
             GROUP BY 1
         ),
     salesforce_data_clean AS (
         SELECT *
         FROM step01__filter_internal
         WHERE rank = 1
     ),
     se_booking_adj AS (
         SELECT
             -- a booking can have multiple adjustments but we needs the dates associated with
             -- the most recent one
             -- TODO: if we need this elsewhere too we should define it in a `dv` module
             -- or change `cms_mysql_snapshot_bulk_wave2.booking_adjustment` to deduplicate on `booking_id` instead of `id`
             booking_id,
             check_in_date::DATE  AS adjusted_check_in_date,
             check_out_date::DATE AS adjusted_check_out_date,
             stay_by_date         AS voucher_stay_by_date
         FROM data_vault_mvp.cms_mysql_snapshots.amendment_snapshot
             QUALIFY ROW_NUMBER() OVER (PARTITION BY booking_id ORDER BY date_created DESC) = 1
     ),
     credit_data AS (
         SELECT original_se_booking_id,
                SUM(CASE WHEN upper(credit_status) = 'ACTIVE' THEN credit_amount ELSE 0 END)        AS credit_active,
                SUM(CASE WHEN upper(credit_status) = 'DELETED' THEN credit_amount ELSE 0 END)       AS credit_deleted,
                SUM(CASE WHEN upper(credit_status) = 'USED' THEN credit_amount ELSE 0 END)          AS credit_used,
                SUM(CASE WHEN upper(credit_status) = 'USED_TB' THEN credit_amount ELSE 0 END)       AS credit_used_tb,
                SUM(CASE WHEN upper(credit_status) = 'REFUNDED_CASH' THEN credit_amount ELSE 0 END) AS credit_refunded_cash,
                SUM(credit_amount)                                                                  AS credit_amount_all
         FROM se.data.se_credit_model
         GROUP BY 1
     ),
     --temporary fix to dedup so i can validate the rest of the SQL is not duplicating, I have asked Mike in tech support, and will replace witht the fixed version
     booking_cancellation_temp_fix AS
         (
             SELECT *
             FROM data_vault_mvp.cms_mysql_snapshots.booking_cancellation_snapshot
                 QUALIFY ROW_NUMBER() OVER (PARTITION BY COALESCE(booking_id, reservation_id) ORDER BY last_updated DESC) = 1
         ),
     world_pay_by_order_code_cur
         AS
         (
             SELECT order_code,
                    currency_code,
                    MIN(event_date::DATE)                                      AS min_event_date,
                    MAX(event_date::DATE)                                      AS max_event_date,
                    COUNT(event_date)                                          AS number_events,
                    sum(CASE WHEN amount < 0 THEN amount * -1 ELSE amount END) AS amount
             FROM raw_vault_mvp.worldpay.transaction_summary
             WHERE lower(status) IN ('refunded', 'refunded_by_merchant')
             GROUP BY 1, 2
         ),
     ratepay_by_shopsorder_cur
         AS
         (
             SELECT shopsorder_id,
                    currency,
                    sum(amount_gross)           AS amount_gross,
                    sum(disagio_gross)          AS disagio_gross,
                    sum(transactionfee_gross)   AS transactionfee_gross,
                    sum(paymentchangefee_gross) AS paymentchangefee_gross
             FROM raw_vault_mvp.ratepay.clearing
             WHERE lower(entry_type) IN ('5', '6', 'return', 'credit')
             GROUP BY 1, 2
         ),
     chargeback_catalogue AS
         (
             SELECT SPLIT(reference, '-')[1]                                        AS reference,
                    ccy,
                    SUM(CASE WHEN lower(result) = 'lost' THEN amount ELSE 0 END)    AS lost_amount,
                    SUM(CASE WHEN lower(result) = 'won' THEN amount ELSE 0 END)     AS won_amount,
                    SUM(CASE WHEN lower(result) = 'pending' THEN amount ELSE 0 END) AS pending_amount,
                    sum(amount)                                                     AS amount
             FROM raw_vault_mvp.finance_gsheets.chargebacks_catalogue
             GROUP BY 1, 2
         ),
     carrier_data_flight_report
         AS
         (
             SELECT external_reference,
                    airline_name,
                    supplier,
                    overall_booking_status,
                    flight_booking_status,
                    cost_in_buying_currency,
                    cost_in_gbp,
                    member_refund_type,
                    booking_system,
                    ROW_NUMBER() OVER (
                        PARTITION BY external_reference
                        ORDER BY
                            flight_pnr
                        ) AS rank
             FROM raw_vault_mvp.finance_gsheets.cash_refunds_airline_refund_report
         ),
     carrier_data_flight_report_pnr
         AS
         (
             SELECT external_reference,
                    listagg(flight_pnr, '/') AS flight_pnr
             FROM raw_vault_mvp.finance_gsheets.cash_refunds_airline_refund_report
             GROUP BY 1
         )
SELECT bs.*,
       sf.case_number,
       sf.case_owner_full_name,
       sf.transaction_id,
       sf.unique_transaction_id                  AS sf_unique_transaction_id,
       sf.subject,
       sf.opportunity_sale_id,
       sf.status,
       sfdc.number_dup_cases_solved,
       sf.case_origin,
       sf."VIEW",
       sf.booking_lookup_check_in_date,
       sf.booking_lookup_check_out_date,
       sf.requested_rebooking_date,
       sf.postponed_booking_request,
       sf.booking_lookup_store_id,
       sf.booking_lookup_supplier_territory,
       sf.contact_reason,
       sf.last_modified_by_full_name,
       sf.overbooking_rebooking_stage,
       sf.reason,
       sf.case_id,
       sf.date_time_opened,
       sf.case_name,
       sf.last_modified_date,
       sf.last_modified_by_case_overview,
       sf.priority_type,
       sf.covid19_member_resolution_cs,
       sf.case_overview_id,
       sf.case_thread_id,
       sf.priority,
       CASE
           WHEN LOWER(sf.subject) LIKE '%refund%' THEN 'Member asked for refund'
           WHEN LOWER(sf.subject) LIKE '%storn%' THEN 'Member asked for refund'
           WHEN LOWER(sf.subject) LIKE '%cxl%' THEN 'Member asked for refund'
           WHEN LOWER(sf.subject) LIKE '%cancel%' THEN 'Member asked for refund'
           WHEN lower("VIEW") LIKE ('%rebook%') AND requested_rebooking_date IS NOT NULL
               THEN 'Member asked for rebooking with date'
           WHEN lower("VIEW") LIKE ('%rebook%') AND requested_rebooking_date IS NULL
               THEN 'Member asked for rebooking without date'
           ELSE 'Unknown' END                    AS status_se_ho,
       CASE
           WHEN badj.adjusted_check_in_date IS NOT NULL THEN 'IHP/H+ rebooked'
           WHEN sf.reason = 'epidemic packages- cash 100%' AND sf.covid19_member_resolution_cs = 'Resolution accepted' AND
                lower(fin.flight_and_non_flight_components_held) = 'yes' THEN 'Refund Cash - Actioned from booking'
           WHEN sf.reason = 'epidemic packages- cash 100%' AND sf.covid19_member_resolution_cs = 'Resolution accepted' AND
                lower(fin.flight_and_non_flight_components_held) = 'no' THEN 'Refund Cash - Actioned from fund'
           WHEN sf."VIEW" IN
                ('**COVID-19 DACH P1/P2 Refusal View**', '**COVID-19 UK&INTL P1/P2 Refusal View**', '**Exec Complaints View**',
                 '**Social Media View**', '**COVID-19 UK/US and INTL Parked View**', '**COVID-19 DACH Parked View**')
               THEN 'Refund Cash - Requested (Credit refusal)'
           WHEN sf.covid19_member_resolution_cs IN ('Resolution accepted') AND cr.credit_active > 0 THEN 'Credit Accepted'
           WHEN sf."VIEW" NOT IN
                ('**COVID-19 DACH P1/P2 Refusal View**', '**COVID-19 UK&INTL P1/P2 Refusal View**', '**Exec Complaints View**',
                 '**Social Media View**', '**COVID-19 UK/US and INTL Parked View**', '**COVID-19 DACH Parked View**')
               AND lower(status) IN ('open', 'pending', 'hold') THEN 'in-progress'
           WHEN sf."VIEW" NOT IN
                ('**COVID-19 DACH P1/P2 Refusal View**', '**COVID-19 UK&INTL P1/P2 Refusal View**', '**Exec Complaints View**',
                 '**Social Media View**', '**COVID-19 UK/US and INTL Parked View**', '**COVID-19 DACH Parked View**')
               AND lower(status) IN ('new') AND sf.case_name IS NULL THEN 'new - not yet action'
           WHEN cr.credit_active > 0 THEN 'Credit Issued'
           WHEN cr.credit_deleted > 0 THEN 'Credit Deleted'
           WHEN bc.refund_channel = 'PAYMENT_METHOD' THEN 'Refunded by Payment Method'
           WHEN bs.checkin <= '2020-03-17' THEN 'Pre 17 March CheckIn'
           ELSE 'No Credit Issued'
           END                                   AS status_se_pkg,
       badj.adjusted_check_in_date,
       badj.adjusted_check_out_date,
       badj.voucher_stay_by_date,
       bc.date_created                           AS bk_cnx_date,
       bc.last_updated                           AS bk_cnx_last_updated,
       bc.fault                                  AS bk_cnx_fault,
       bc.reason                                 AS bk_cnx_reason,
       -- bc.BOOKING_FEE                            AS BK_CNX_BOOKING_FEE,
       -- bc.CC_FEE                                 AS BK_CNX_CC_FEE,
       -- bc.HOTEL_GOOD_WILL                        AS BK_CNX_HOTEL_GOOD_WILL,
       bc.refund_channel                         AS bk_cnx_refund_channel,
       bc.refund_type                            AS bk_cnx_refund_type,
       -- bc.SE_GOOD_WILL                           AS BK_CNX_SE_GOOD_WILL,
       bc.who_pays                               AS bk_cnx_who_pays,
       bc.cancel_with_provider                   AS bk_cnx_cancel_with_provider,
       -- COALESCE(bc.BOOKING_FEE, 0) + COALESCE(bc.CC_FEE, 0) + COALESCE(bc.SE_GOOD_WILL, 0) +
       --COALESCE(bc.HOTEL_GOOD_WILL, 0)           AS BK_CNX_PARTIAL_REFUND_AMOUNT,
       cr.credit_active,
       cr.credit_deleted,
       cr.credit_used,
       cr.credit_used_tb,
       cr.credit_refunded_cash,
       cbse.date                                 AS cb_se_date,
       cbse.order_code                           AS cb_se_order_code,
       cbse.payment_method                       AS cb_se_payment_method,
       cbse.currency                             AS cb_se_currency,
       cbse.payment_amount                       AS cb_se_payment_amount,
       cbse.cb_status                            AS cb_se_status,
       cbctl.reference                           AS cb_ctl_reference,
       cbctl.ccy                                 AS cb_ctl_ccy,
       cbctl.lost_amount                         AS cb_ctl_amount_lost,
       cbctl.pending_amount                      AS cb_ctl_amount_pending,
       cbctl.won_amount                          AS cb_ctl_amount_won,
       mancb.refund_timestamp                    AS manual_bacs_refund_timestamp,
       mancb.payment_status                      AS manual_bacs_payment_status,
       mancb.customer_currency                   AS manual_bacs_customer_currency,
       mancb.amount_in_customer_currency         AS manual_bacs_amount_in_customer_currency,
       mancb.bank_details_type                   AS manual_bacs_bank_details_type,
       mancb.product_type                        AS manual_bacs_product_typefull_cms_transaction_id,
       mancb.full_cms_transaction_id             AS manual_bacs_full_cms_transaction_id,
       mancb.type_of_refund                      AS manual_bacs_type_of_refund,
       mancb.reference_transaction_id            AS manual_bacs_reference_transaction_id,
       mancb.refund_speed                        AS manual_bacs_refund_speed,
       mancb.duplicate                           AS manual_bacs_duplicate,
       mancb.cb_raised                           AS manual_bacs_cb_raised,
       mancb.fraud_team_comment                  AS manual_bacs_fraud_team_comment,
       fin.booking_id                            AS finance_booking_id,
       fin.include_flight                        AS finance_include_flight,
       fin.net_amount_paid_fx                    AS finance_net_amount_paid_fx,
       fin.net_amount_paid_gbp                   AS finance_net_amount_paid_gbp,
       fin.non_flight_spls_cash_held             AS finance_non_flight_spls_cash_held,
       fin.non_flight_vcc_held                   AS finance_non_flight_vcc_held,
       fin.flight_refunds_received_gbp           AS finance_flight_refunds_received_gbp,
       fin.total_held_gbp                        AS finance_total_held_gbp,
       fin.perc_held                             AS finance_perc_held,
       fin.flight_and_non_flight_components_held AS finance_flight_and_non_flight_components_held,
       fin.refund_made                           AS finance_refund_made,
       fin.refund_type                           AS finance_refund_type,
       fin.amount                                AS finance_amount,
       fin.chargeback                            AS finance_chargeback,
       fin.currency                              AS finance_currency,
       fin.amount_inc_margin_adj                 AS finance_amount_inc_margin_adj,
       pnr.flight_pnr,
       cr1.airline_name                          AS car_airline_name_1,
       cr1.supplier                              AS car_supplier_1,
       cr1.overall_booking_status                AS car_overall_booking_status_1,
       cr1.flight_booking_status                 AS car_flight_booking_status_1,
       cr1.cost_in_buying_currency               AS car_cost_in_buying_currency_1,
       cr1.cost_in_gbp                           AS car_cost_in_gbp_1,
       cr1.member_refund_type                    AS car_member_refund_type_1,
       cr1.booking_system                        AS car_booking_system_1,
       cr2.airline_name                          AS car_airline_name_2,
       cr2.supplier                              AS car_supplier_2,
       cr2.overall_booking_status                AS car_overall_booking_status_2,
       cr2.flight_booking_status                 AS car_flight_booking_status_2,
       cr2.cost_in_buying_currency               AS car_cost_in_buying_currency_2,
       cr2.cost_in_gbp                           AS car_cost_in_gbp_2,
       cr2.member_refund_type                    AS car_member_refund_type_2,
       cr2.booking_system                        AS car_booking_system_2,
       cr3.airline_name                          AS car_airline_name_3,
       cr3.supplier                              AS car_supplier_3,
       cr3.overall_booking_status                AS car_overall_booking_status_3,
       cr3.flight_booking_status                 AS car_flight_booking_status_3,
       cr3.cost_in_buying_currency               AS car_cost_in_buying_currency_3,
       cr3.cost_in_gbp                           AS car_cost_in_gbp_3,
       cr3.member_refund_type                    AS car_member_refund_type_3,
       cr3.booking_system                        AS car_booking_system_3,
       cr4.airline_name                          AS car_airline_name_4,
       cr4.supplier                              AS car_supplier_4,
       cr4.overall_booking_status                AS car_overall_booking_status_4,
       cr4.flight_booking_status                 AS car_flight_booking_status_4,
       cr4.cost_in_buying_currency               AS car_cost_in_buying_currency_4,
       cr4.cost_in_gbp                           AS car_cost_in_gbp_4,
       cr4.member_refund_type                    AS car_member_refund_type_4,
       cr4.booking_system                        AS car_booking_system_4,
       wp.min_event_date                         AS world_pay_min_event_date,
       wp.max_event_date                         AS world_pay_max_event_date,
       wp.number_events                          AS world_pay_number_events,
       wp.currency_code                          AS world_pay_currency,
       wp.amount                                 AS world_pay_amount,
       rp.currency                               AS rate_pay_currency,
       rp.amount_gross                           AS rate_pay_amount_gross,
       rp.disagio_gross                          AS rate_pay_disagio_gross,
       rp.transactionfee_gross                   AS rate_pay_transactionfee_gross,
       rp.paymentchangefee_gross                 AS rate_pay_paymentchangefee_gross,
       str.type                                  AS stripe_type,
       str.source                                AS stripe_source,
       str.amount                                AS stripe_amount,
       str.fee                                   AS stripe_fee,
       str.destination_platform_fee              AS stripe_destination_platform_fee,
       str.net                                   AS stripe_net,
       str.currency                              AS stripe_currency,
       str.created_utc                           AS stripe_created_utc,
       str.available_on_utc                      AS stripe_available_on_utc,
       str.description                           AS stripe_description,
       str.customer_facing_amount                AS stripe_customer_facing_amount,
       str.customer_facing_currency              AS stripe_customer_facing_currency,
       str.transfer                              AS stripe_transfer,
       str.transfer_date_utc                     AS stripe_transfer_date_utc,
       str.transfer_group                        AS stripe_transfer_group,
       str.order_id_metadata                     AS stripe_order_id_metadata,
       str.payment_id_metadata                   AS stripe_payment_id_metadata,
       str.session_key_metadata                  AS stripe_session_key_metadata,
       str.offer_id_metadata                     AS stripe_offer_id_metadata,
       cmap.updated                              AS carrier_mapping_1_updated,
       cmap.flight_carrier                       AS carrier_mapping_1_flight_carrier,
       cmap.type                                 AS carrier_mapping_1_type,
       cmap.refund_type                          AS carrier_mapping_1_refund_type,
       cmap.reported_refund_type                 AS carrier_mapping_1_reported_refund_type
FROM se.data_pii.se_booking_summary_extended bs
         LEFT JOIN salesforce_data_clean sf ON bs.transactionid = sf.unique_transaction_id
         LEFT JOIN duplicated_solved_count sfdc ON bs.transactionid = sfdc.unique_transaction_id
         LEFT JOIN chargeback_catalogue cbctl ON cbctl.reference = bs.booking_id
         LEFT JOIN raw_vault_mvp.finance_gsheets.chargebacks_se cbse ON cbse.booking_id = bs.booking_id
         LEFT JOIN raw_vault_mvp.finance_gsheets.manual_refunds mancb ON mancb.full_cms_transaction_id = bs.transactionid
         LEFT JOIN (
    SELECT *
    FROM carrier_data_flight_report
    WHERE rank = 1
) cr1 ON cr1.external_reference = bs.transactionid
         LEFT JOIN (
    SELECT *
    FROM carrier_data_flight_report
    WHERE rank = 2
) cr2 ON cr2.external_reference = bs.transactionid
         LEFT JOIN (
    SELECT *
    FROM carrier_data_flight_report
    WHERE rank = 3
) cr3 ON cr3.external_reference = bs.transactionid
         LEFT JOIN (
    SELECT *
    FROM carrier_data_flight_report
    WHERE rank = 4
) cr4 ON cr4.external_reference = bs.transactionid
         LEFT JOIN (
    SELECT *
    FROM carrier_data_flight_report_pnr
) pnr ON pnr.external_reference = bs.transactionid
         LEFT JOIN raw_vault_mvp.finance_gsheets.cash_refunds_to_members fin ON fin.cms_transaction_id = bs.transactionid
         LEFT JOIN se_booking_adj badj ON bs.booking_id = badj.booking_id
         LEFT JOIN world_pay_by_order_code_cur wp ON wp.order_code = bs.uniquetransactionreference
         LEFT JOIN ratepay_by_shopsorder_cur rp ON rp.shopsorder_id = bs.booking_id
         LEFT JOIN raw_vault_mvp.finance_gsheets.cash_refunds_stripe str ON SUBSTR(str.id, 5) = bs.uniquetransactionreference
         LEFT JOIN raw_vault_mvp.finance_gsheets.cash_refunds_airline_refund_status cmap ON cmap.flight_carrier = cr1.airline_name
         LEFT JOIN booking_cancellation_temp_fix bc ON COALESCE(bc.booking_id::VARCHAR, 'A' || bc.reservation_id::VARCHAR) = CASE
                                                                                                                                 WHEN LEFT(bs.transactionid, 1) = 'A'
                                                                                                                                     THEN 'A' || REGEXP_SUBSTR(bs.transactionid, '-.*-(.*)', 1, 1, 'e')
                                                                                                                                 ELSE REGEXP_SUBSTR(bs.transactionid, '-.*-(.*)', 1, 1, 'e') END
         LEFT JOIN credit_data cr ON CASE
                                         WHEN LEFT(bs.transactionid, 1) = 'A'
                                             THEN 'A' || REGEXP_SUBSTR(bs.transactionid, '-.*-(.*)', 1, 1, 'e')
                                         ELSE REGEXP_SUBSTR(bs.transactionid, '-.*-(.*)', 1, 1, 'e') END =
                                     cr.original_se_booking_id;



--covid_master_list_catalogue


------------------------------------------------------------------------------------------------------------------------
--Questions:
--Whats the difference between these dags:
-- incoming__sfsc__rebooking_request_cases__daily_at_04h00
-- incoming__sfsc__rebooking_request_cases_ho__daily_at_04h00
-- incoming__sfsc__rebooking_request_cases_packages__daily_at_04h00
-- incoming__sfsc__rebooking_request_cases_pkg__daily_at_04h00


WITH duplicated_solved_count AS
         (
             --count of solved cases assigned to this booking that aren't
             --classed as the primary rebooking case
             SELECT unique_transaction_id,
                    --don't count case 'assigned' to booking
                    SUM(CASE WHEN rank > 1 THEN 1 ELSE 0 END) AS number_dup_cases_solved
             FROM hygiene_vault_mvp.sfsc.rebooking_requests
             GROUP BY 1
         ),
     amended_dates AS
         (
             SELECT booking_id,
                    check_in_date  AS adjusted_check_in_date,
                    check_out_date AS adjusted_check_out_date,
                    sb.voucher_stay_by_date
             FROM data_vault_mvp_dev_robin.dwh.se_booking sb
         ),
     credit_data AS
         (
             --sum credits to booking level, not include deleted
             SELECT original_se_booking_id,
                    credit_status,
                    SUM(credit_amount) AS credit_amount
             FROM se.data.se_credit_model
             WHERE lower(credit_status) != 'deleted'
             GROUP BY 1, 2
         ),
     credit_data_deleted AS
         (
             --combine this with credit data and filter in the sum
             SELECT original_se_booking_id,
                    credit_status,
                    SUM(credit_amount) AS credit_amount
             FROM se.data.se_credit_model
             WHERE upper(credit_status) = 'DELETED'
             GROUP BY 1, 2
         ),
     --temporary fix to dedup so i can validate the rest of the SQL is not duplicating, I have asked Mike in tech support, and will replace witht the fixed version
     booking_cancellation_temp_fix AS
         (
             --CS can sometimes initiate more than one cancellation per booking, so we dedupe to most recent.
             SELECT booking_id,
                    date_created,
                    last_updated,
                    fault,
                    reason,
                    booking_fee,
                    cc_fee,
                    hotel_good_will,
                    refund_channel,
                    refund_type,
                    se_good_will,
                    who_pays,
                    reservation_id,
                    cancel_with_provider,
                    extract_metadata
             FROM data_vault_mvp.cms_mysql_snapshots.booking_cancellation_snapshot
                 QUALIFY ROW_NUMBER() OVER (PARTITION BY COALESCE(booking_id, reservation_id) ORDER BY last_updated DESC) = 1
         ),
     world_pay_by_order_code_cur AS
         (
             --refund data only
             SELECT order_code,
                    currency_code,
                    MIN(event_date::DATE)                                      AS min_event_date,
                    MAX(event_date::DATE)                                      AS max_event_date,
                    COUNT(event_date)                                          AS number_events,
                    --negatve because mix match of data
                    sum(CASE WHEN amount < 0 THEN amount * -1 ELSE amount END) AS amount
             FROM raw_vault_mvp.worldpay.transaction_summary
             WHERE lower(status) IN ('refunded', 'refunded_by_merchant') --means we've given money back to customer
             GROUP BY 1, 2
         ),
     ratepay_by_shopsorder_cur AS
         (
             SELECT shopsorder_id,
                    currency,
                    sum(amount_gross)           AS amount_gross,
                    sum(disagio_gross)          AS disagio_gross,
                    sum(transactionfee_gross)   AS transactionfee_gross,
                    sum(paymentchangefee_gross) AS paymentchangefee_gross
             FROM raw_vault_mvp.ratepay.clearing
             WHERE lower(entry_type) IN ('5', '6', 'return', 'credit') --refund status
             GROUP BY 1, 2
         ),
     chargeback_catalogue AS
         (
             --multiple chargebacks so sum up to booking
             SELECT SPLIT(reference, '-')[1]                                        AS reference,
                    ccy,
                    SUM(CASE WHEN lower(result) = 'lost' THEN amount ELSE 0 END)    AS lost_amount,
                    SUM(CASE WHEN lower(result) = 'won' THEN amount ELSE 0 END)     AS won_amount,
                    SUM(CASE WHEN lower(result) = 'pending' THEN amount ELSE 0 END) AS pending_amount,
                    SUM(amount)                                                     AS amount
             FROM raw_vault_mvp.finance_gsheets.chargebacks_catalogue
             GROUP BY 1, 2
         ),
     carrier_data_flight_report
         AS
         (
             --booking can have multiple carriers so we create a rank
             SELECT external_reference,
                    airline_name,
                    supplier,
                    overall_booking_status,
                    flight_booking_status,
                    cost_in_buying_currency,
                    cost_in_gbp,
                    member_refund_type,
                    booking_system,
                    ROW_NUMBER() OVER (
                        PARTITION BY external_reference
                        ORDER BY
                            travel_date
                        ) AS rank
             FROM raw_vault_mvp.finance_gsheets.cash_refunds_airline_refund_report
         )
SELECT bs.*,
       sf.case_number,
       sf.case_owner,
       sf.transaction_id,
       sf.unique_transaction_id                  AS sf_unique_transaction_id,
       sf.subject,
       sf.opportunity_sale_id,
       sf.status,
       sfdc.number_dup_cases_solved,
       sf.case_origin,
       sf."VIEW",
       sf.booking_lookup_check_in_date,
       sf.booking_lookup_check_out_date,
       sf.requested_rebooking_date,
       sf.postponed_booking_request,
       sf.booking_lookup_store_id,
       sf.booking_lookup_supplier_territory,
       sf.contact_reason,
       sf.last_modified_by_full_name,
       sf.overbooking_rebooking_stage,
       sf.reason,
       sf.case_id,
       sf.date_time_opened,
       sf.case_overview_number,
       sf.last_modified_date,
--        sf.last_modified_by_case_overview,
--        sf.priority_type,
--        sf.covid19_member_resolution_cs,
--        sf.case_overview_id,
--        sf.case_thread_id,
--        sf.priority,
       CASE
           WHEN LOWER(sf.subject) LIKE '%refund%' THEN 'Member asked for refund'
           WHEN LOWER(sf.subject) LIKE '%storn%' THEN 'Member asked for refund'
           WHEN LOWER(sf.subject) LIKE '%cxl%' THEN 'Member asked for refund'
           WHEN LOWER(sf.subject) LIKE '%cancel%' THEN 'Member asked for refund'
           WHEN lower("VIEW") LIKE ('%rebook%') AND requested_rebooking_date IS NOT NULL
               THEN 'Member asked for rebooking with date'
           WHEN lower("VIEW") LIKE ('%rebook%') AND requested_rebooking_date IS NULL
               THEN 'Member asked for rebooking without date'
           ELSE 'Unknown' END                    AS status_se_ho,
       CASE
           WHEN badj.adjusted_check_in_date IS NOT NULL THEN 'IHP/H+ rebooked'
           WHEN sf.reason = 'epidemic packages- cash 100%' AND sf.covid19_member_resolution_cs = 'Resolution accepted' AND
                lower(fin.flight_and_non_flight_components_held) = 'yes' THEN 'Refund Cash - Actioned from booking'
           WHEN sf.reason = 'epidemic packages- cash 100%' AND sf.covid19_member_resolution_cs = 'Resolution accepted' AND
                lower(fin.flight_and_non_flight_components_held) = 'no' THEN 'Refund Cash - Actioned from fund'
           WHEN sf."VIEW" IN
                ('**COVID-19 DACH P1/P2 Refusal View**', '**COVID-19 UK&INTL P1/P2 Refusal View**', '**Exec Complaints View**',
                 '**Social Media View**', '**COVID-19 UK/US and INTL Parked View**', '**COVID-19 DACH Parked View**')
               THEN 'Refund Cash - Requested (Credit refusal)'
           WHEN sf.covid19_member_resolution_cs IN ('Resolution accepted') AND lower(cr.credit_status) IS NOT NULL
               THEN 'Credit Accepted'
           WHEN sf."VIEW" NOT IN
                ('**COVID-19 DACH P1/P2 Refusal View**', '**COVID-19 UK&INTL P1/P2 Refusal View**', '**Exec Complaints View**',
                 '**Social Media View**', '**COVID-19 UK/US and INTL Parked View**', '**COVID-19 DACH Parked View**')
               AND lower(status) IN ('open', 'pending', 'hold') THEN 'in-progress'
           WHEN sf."VIEW" NOT IN
                ('**COVID-19 DACH P1/P2 Refusal View**', '**COVID-19 UK&INTL P1/P2 Refusal View**', '**Exec Complaints View**',
                 '**Social Media View**', '**COVID-19 UK/US and INTL Parked View**', '**COVID-19 DACH Parked View**')
               AND lower(status) IN ('new') AND sf.case_name IS NULL THEN 'new - not yet action'
           WHEN lower(cr.credit_status) IS NOT NULL THEN 'Credit Issued'
           WHEN lower(crd.credit_status) IS NOT NULL THEN 'Credit Deleted'
           WHEN bc.refund_channel = 'PAYMENT_METHOD' THEN 'Refunded by Payment Method'
           WHEN bs.checkin <= '2020-03-17' THEN 'Pre 17 March CheckIn'
           ELSE 'No Credit Issued'
           END                                   AS status_se_pkg,
       badj.adjusted_check_in_date,
       badj.adjusted_check_out_date,
       badj.voucher_stay_by_date,
       bc.date_created                           AS bk_cnx_date,
       bc.last_updated                           AS bk_cnx_last_updated,
       bc.fault                                  AS bk_cnx_fault,
       bc.reason                                 AS bk_cnx_reason,
       bc.refund_channel                         AS bk_cnx_refund_channel,
       bc.refund_type                            AS bk_cnx_refund_type,
       bc.who_pays                               AS bk_cnx_who_pays,
       bc.cancel_with_provider                   AS bk_cnx_cancel_with_provider,
       cr.credit_status,
       cr.credit_amount,
       crd.credit_status                         AS credit_status_deleted,
       crd.credit_amount                         AS credit_amount_deleted,
       cbse.date                                 AS cb_se_date,
       cbse.order_code                           AS cb_se_order_code,
       cbse.payment_method                       AS cb_se_payment_method,
       cbse.currency                             AS cb_se_currency,
       cbse.payment_amount                       AS cb_se_payment_amount,
       cbse.cb_status                            AS cb_se_status,
       cbctl.reference                           AS cb_ctl_reference,
       cbctl.ccy                                 AS cb_ctl_ccy,
       cbctl.amount                              AS cb_ctl_amount,
       mancb.refund_timestamp                    AS manual_bacs_refund_timestamp,
       mancb.payment_status                      AS manual_bacs_payment_status,
       mancb.customer_currency                   AS manual_bacs_customer_currency,
       mancb.amount_in_customer_currency         AS manual_bacs_amount_in_customer_currency,
       mancb.bank_details_type                   AS manual_bacs_bank_details_type,
       mancb.product_type                        AS manual_bacs_product_typefull_cms_transaction_id,
       mancb.full_cms_transaction_id             AS manual_bacs_full_cms_transaction_id,
       mancb.type_of_refund                      AS manual_bacs_type_of_refund,
       mancb.reference_transaction_id            AS manual_bacs_reference_transaction_id,
       mancb.refund_speed                        AS manual_bacs_refund_speed,
       mancb.duplicate                           AS manual_bacs_duplicate,
       mancb.cb_raised                           AS manual_bacs_cb_raised,
       mancb.fraud_team_comment                  AS manual_bacs_fraud_team_comment,
       fin.booking_id                            AS finance_booking_id,
       fin.include_flight                        AS finance_include_flight,
       fin.net_amount_paid_fx                    AS finance_net_amount_paid_fx,
       fin.net_amount_paid_gbp                   AS finance_net_amount_paid_gbp,
       fin.non_flight_spls_cash_held             AS finance_non_flight_spls_cash_held,
       fin.non_flight_vcc_held                   AS finance_non_flight_vcc_held,
       fin.flight_refunds_received_gbp           AS finance_flight_refunds_received_gbp,
       fin.total_held_gbp                        AS finance_total_held_gbp,
       fin.perc_held                             AS finance_perc_held,
       fin.flight_and_non_flight_components_held AS finance_flight_and_non_flight_components_held,
       fin.refund_made                           AS finance_refund_made,
       fin.refund_type                           AS finance_refund_type,
       fin.amount                                AS finance_amount,
       fin.chargeback                            AS finance_chargeback,
       fin.currency                              AS finance_currency,
       fin.amount_inc_margin_adj                 AS finance_amount_inc_margin_adj,
       cr1.airline_name                          AS car_airline_name_1,
       cr1.supplier                              AS car_supplier_1,
       cr1.overall_booking_status                AS car_overall_booking_status_1,
       cr1.flight_booking_status                 AS car_flight_booking_status_1,
       cr1.cost_in_buying_currency               AS car_cost_in_buying_currency_1,
       cr1.cost_in_gbp                           AS car_cost_in_gbp_1,
       cr1.member_refund_type                    AS car_member_refund_type_1,
       cr1.booking_system                        AS car_booking_system_1,
       cr2.airline_name                          AS car_airline_name_2,
       cr2.supplier                              AS car_supplier_2,
       cr2.overall_booking_status                AS car_overall_booking_status_2,
       cr2.flight_booking_status                 AS car_flight_booking_status_2,
       cr2.cost_in_buying_currency               AS car_cost_in_buying_currency_2,
       cr2.cost_in_gbp                           AS car_cost_in_gbp_2,
       cr2.member_refund_type                    AS car_member_refund_type_2,
       cr2.booking_system                        AS car_booking_system_2,
       cr3.airline_name                          AS car_airline_name_3,
       cr3.supplier                              AS car_supplier_3,
       cr3.overall_booking_status                AS car_overall_booking_status_3,
       cr3.flight_booking_status                 AS car_flight_booking_status_3,
       cr3.cost_in_buying_currency               AS car_cost_in_buying_currency_3,
       cr3.cost_in_gbp                           AS car_cost_in_gbp_3,
       cr3.member_refund_type                    AS car_member_refund_type_3,
       cr3.booking_system                        AS car_booking_system_3,
       cr4.airline_name                          AS car_airline_name_4,
       cr4.supplier                              AS car_supplier_4,
       cr4.overall_booking_status                AS car_overall_booking_status_4,
       cr4.flight_booking_status                 AS car_flight_booking_status_4,
       cr4.cost_in_buying_currency               AS car_cost_in_buying_currency_4,
       cr4.cost_in_gbp                           AS car_cost_in_gbp_4,
       cr4.member_refund_type                    AS car_member_refund_type_4,
       cr4.booking_system                        AS car_booking_system_4,
       wp.min_event_date                         AS world_pay_min_event_date,
       wp.max_event_date                         AS world_pay_max_event_date,
       wp.number_events                          AS world_pay_number_events,
       wp.currency_code                          AS world_pay_currency,
       wp.amount                                 AS world_pay_amount,
       rp.currency                               AS rate_pay_currency,
       rp.amount_gross                           AS rate_pay_amount_gross,
       rp.disagio_gross                          AS rate_pay_disagio_gross,
       rp.transactionfee_gross                   AS rate_pay_transactionfee_gross,
       rp.paymentchangefee_gross                 AS rate_pay_paymentchangefee_gross,
       str.type                                  AS stripe_type,
       str.source                                AS stripe_source,
       str.amount                                AS stripe_amount,
       str.fee                                   AS stripe_fee,
       str.destination_platform_fee              AS stripe_destination_platform_fee,
       str.net                                   AS stripe_net,
       str.currency                              AS stripe_currency,
       str.created_utc                           AS stripe_created_utc,
       str.available_on_utc                      AS stripe_available_on_utc,
       str.description                           AS stripe_description,
       str.customer_facing_amount                AS stripe_customer_facing_amount,
       str.customer_facing_currency              AS stripe_customer_facing_currency,
       str.transfer                              AS stripe_transfer,
       str.transfer_date_utc                     AS stripe_transfer_date_utc,
       str.transfer_group                        AS stripe_transfer_group,
       str.order_id_metadata                     AS stripe_order_id_metadata,
       str.payment_id_metadata                   AS stripe_payment_id_metadata,
       str.session_key_metadata                  AS stripe_session_key_metadata,
       str.offer_id_metadata                     AS stripe_offer_id_metadata,
       cmap.updated                              AS carrier_mapping_1_updated,
       cmap.flight_carrier                       AS carrier_mapping_1_flight_carrier,
       cmap.type                                 AS carrier_mapping_1_type,
       cmap.refund_type                          AS carrier_mapping_1_refund_type,
       cmap.reported_refund_type                 AS carrier_mapping_1_reported_refund_type
FROM se.data_pii.se_booking_summary_extended bs --cms enriched from dwh
--sf data is dirty, will need to hygiene, can have multiple cases per booking, logic to determine which is the active case
         LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.rebooking_requests sf ON bs.transactionid = sf.unique_transaction_id
    --from sf data, when duplicate cases exist, they wanted to know if these cases are resolved
         LEFT JOIN duplicated_solved_count sfdc ON bs.transactionid = sfdc.unique_transaction_id
    --gsheet that logs all the charge backs from catalogue team, customers who've requested money back via credit card provider, if we have lost case then we pay customer back
    --might need to split this out by type
         LEFT JOIN chargeback_catalogue cbctl ON cbctl.reference = bs.booking_id
    --ghseet, charge back for se is everything that's not catalogue
         LEFT JOIN raw_vault_mvp.finance_gsheets.chargebacks_se cbse ON cbse.booking_id = bs.booking_id
    --gsheet, can get your money back from manual BACs refunds,
         LEFT JOIN raw_vault_mvp.finance_gsheets.manual_refunds mancb ON mancb.full_cms_transaction_id = bs.transactionid
    --we use ivector data to get carrier data, because cms carrier information might be outdated, you can sometimes have a flight that has multiple carriers, so at the moment max carrier is 4
         LEFT JOIN (
    SELECT *
    FROM carrier_data_flight_report
    WHERE rank = 1
) cr1 ON cr1.external_reference = bs.transactionid
         LEFT JOIN (
    SELECT *
    FROM carrier_data_flight_report
    WHERE rank = 2
) cr2 ON cr2.external_reference = bs.transactionid
         LEFT JOIN (
    SELECT *
    FROM carrier_data_flight_report
    WHERE rank = 3
) cr3 ON cr3.external_reference = bs.transactionid
         LEFT JOIN (
    SELECT *
    FROM carrier_data_flight_report
    WHERE rank = 4
) cr4 ON cr4.external_reference = bs.transactionid
    --gsheet, tom collates this every monday, (important dataset), from netsuite
         LEFT JOIN raw_vault_mvp.finance_gsheets.cash_refunds_to_members fin ON fin.cms_transaction_id = bs.transactionid
    --cms table for new adjusted booking data, for checkin checkout or received a voucher
         LEFT JOIN amended_dates badj ON bs.booking_id = badj.booking_id
    --psp transaction level
         LEFT JOIN world_pay_by_order_code_cur wp ON wp.order_code = bs.uniquetransactionreference
    --psp transaction level
         LEFT JOIN ratepay_by_shopsorder_cur rp ON rp.shopsorder_id = bs.booking_id
    --stripe psp, catalogue only bookings
         LEFT JOIN raw_vault_mvp.finance_gsheets.cash_refunds_stripe str ON SUBSTR(str.id, 5) = bs.uniquetransactionreference
    --basic list of carriers, and their refund terms
         LEFT JOIN raw_vault_mvp.finance_gsheets.cash_refunds_airline_refund_status cmap ON cmap.flight_carrier = cr1.airline_name
    --cms booking cancellations table, tells what the reason for the cancellation, need to create an A appenditure
         LEFT JOIN booking_cancellation_temp_fix bc ON COALESCE(bc.booking_id::VARCHAR, 'A' || bc.reservation_id::VARCHAR) = CASE
                                                                                                                                 WHEN LEFT(bs.transactionid, 1) = 'A'
                                                                                                                                     THEN 'A' || REGEXP_SUBSTR(bs.transactionid, '-.*-(.*)', 1, 1, 'e')
                                                                                                                                 ELSE REGEXP_SUBSTR(bs.transactionid, '-.*-(.*)', 1, 1, 'e') END
    --for a booking, if they've recieved a credit, how they have received the creditclearing eg. tech bulk
         LEFT JOIN credit_data cr ON CASE
                                         WHEN LEFT(bs.transactionid, 1) = 'A'
                                             THEN 'A' || REGEXP_SUBSTR(bs.transactionid, '-.*-(.*)', 1, 1, 'e')
                                         ELSE REGEXP_SUBSTR(bs.transactionid, '-.*-(.*)', 1, 1, 'e') END =
                                     cr.original_se_booking_id
    --for a booking, if they've recieved a credit, how they have received the credit eg. tech bulk where the credit is deleted.
         LEFT JOIN credit_data_deleted crd ON CASE
                                                  WHEN LEFT(bs.transactionid, 1) = 'A'
                                                      THEN 'A' || REGEXP_SUBSTR(bs.transactionid, '-.*-(.*)', 1, 1, 'e')
                                                  ELSE REGEXP_SUBSTR(bs.transactionid, '-.*-(.*)', 1, 1, 'e') END =
                                              crd.original_se_booking_id;



SET database_name = 'COLLAB';
SET schema_name = 'COVID_PII';
SET object_name = 'COVID_MASTER_LIST_HO_PACKAGES';

SELECT DISTINCT
       u.grantee_name AS user_name
FROM snowflake.account_usage.grants_to_users u
         INNER JOIN snowflake.account_usage.grants_to_roles r
                    ON u.role = r.grantee_name
WHERE u.granted_to = 'USER'
  AND r.table_catalog = $database_name
  AND r.table_schema = $schema_name
  AND r.name = $object_name
  AND u.deleted_on IS NULL
  AND r.deleted_on IS NULL;


CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.user_attributes clone data_vault_mvp.dwh.user_attributes;

SELECT * FROm collab.covid_pii.covid_master_list_ho_packages cmlhp;

SELECT * FROM se.data.se_user_attributes;


CREATE OR REPLACE VIEW collab.covid_pii.covid_master_list_ho_packages
    COPY GRANTS
AS
WITH combine_sales_force
         AS (
        SELECT *
        FROM raw_vault_mvp.sfsc.rebooking_request_cases_pkg
        UNION
        SELECT *
        FROM raw_vault_mvp.sfsc.rebooking_request_cases_ho
    ),
     step01__filter_internal
         AS (
         SELECT transaction_id,
                booking_id,
                booking_lookup_check_in_date, -- some records used to come in without :SS portion
                booking_lookup_check_out_date,
                booking_lookup_store_id,
                booking_lookup_supplier_territory,
                case_number::INT                               AS case_number,
                case_origin,
                case_owner_full_name,
                contact_reason,
                opportunity_sale_id,
                LOWER(status)                                  AS status,
                CASE
                    WHEN LOWER(status) = 'hold' THEN 1
                    WHEN LOWER(status) = 'pending' THEN 1
                    WHEN LOWER(status) = 'open' THEN 1
                    WHEN LOWER(status) = 'new' THEN 2
                    WHEN LOWER(status) = 'solved' THEN 3
                    WHEN LOWER(status) = 'closed' THEN 4
                    ELSE 99
                    END                                        AS status_rank,
                LOWER(subject)                                 AS subject,
                CASE
                    WHEN LOWER(subject) LIKE '%amend%' AND requested_rebooking_date IS NOT NULL
                        THEN 'Member asked for rebooking with date'
                    WHEN LOWER(subject) LIKE '%amend%' AND requested_rebooking_date IS NULL
                        THEN 'Member asked for rebooking without date'
                    WHEN LOWER(subject) LIKE '%refund%' THEN 'Member asked for refund'
                    WHEN LOWER(subject) LIKE '%storn%' THEN 'Member asked for refund'
                    WHEN LOWER(subject) LIKE '%cxl%' THEN 'Member asked for refund'
                    WHEN LOWER(subject) LIKE '%cancel%' THEN 'Member asked for refund'
                    ELSE 'Unknown' END                         AS status_se,
                "VIEW",
                TRY_CAST(postponed_booking_request AS BOOLEAN) AS postponed_booking_request,
                requested_rebooking_date,
                LOWER(last_modified_by_full_name)              AS last_modified_by_full_name,
                LOWER(overbooking_rebooking_stage)             AS overbooking_rebooking_stage,
                LOWER(reason)                                  AS reason,
                case_id,
                date_time_opened,
                case_name::INT                                 AS case_name,
                last_modified_date,
                last_modified_by_case_overview,
                priority_type,
                covid19_member_resolution_cs,
                case_overview_id,
                case_thread_id,
                priority,
                -- CS sometimes put booking id in transaction id field
                COALESCE(transaction_id, booking_id)           AS unique_transaction_id,
                -- hygiene flags
                CASE
                    WHEN unique_transaction_id IS NULL
                        THEN 1
                    END                                        AS fails_validation__unique_transaction_id__expected_nonnull,
                CASE
                    WHEN fails_validation__unique_transaction_id__expected_nonnull = 1
                        THEN 1
                    END                                        AS failed_some_validation,
                ROW_NUMBER() OVER (
                    PARTITION BY unique_transaction_id
                    ORDER BY
                        status_rank ASC,
                        case_number DESC,
                        case_name DESC
                    )                                          AS rank
         FROM combine_sales_force
         WHERE lower(last_modified_by_full_name) NOT IN ('dylan hone', 'kate donaghy', 'jessica ho')
           AND NOT (
                 lower(last_modified_by_full_name) = 'marta lagut'
                 AND case_name IS NULL
                 AND lower(status) = 'solved'
             )
           AND lower(status) != 'closed'
     ),
     duplicated_solved_count AS
         (
             SELECT unique_transaction_id,
                    SUM(CASE WHEN rank > 1 THEN 1 ELSE 0 END) AS number_dup_cases_solved
             FROM step01__filter_internal
             WHERE lower(status) = 'solved'
             GROUP BY 1
         ),
     salesforce_data_clean AS (
         SELECT *
         FROM step01__filter_internal
         WHERE rank = 1
     ),
     se_booking_adj AS (
         SELECT
             -- a booking can have multiple adjustments but we needs the dates associated with
             -- the most recent one
             -- TODO: if we need this elsewhere too we should define it in a `dv` module
             -- or change `cms_mysql_snapshot_bulk_wave2.booking_adjustment` to deduplicate on `booking_id` instead of `id`
             booking_id,
             check_in_date::DATE  AS adjusted_check_in_date,
             check_out_date::DATE AS adjusted_check_out_date,
             stay_by_date         AS voucher_stay_by_date
         FROM data_vault_mvp.cms_mysql_snapshots.amendment_snapshot
             QUALIFY ROW_NUMBER() OVER (PARTITION BY booking_id ORDER BY date_created DESC) = 1
     ),
     credit_data AS (
         SELECT original_se_booking_id,
                SUM(CASE WHEN upper(credit_status) = 'ACTIVE' THEN credit_amount ELSE 0 END)        AS credit_active,
                SUM(CASE WHEN upper(credit_status) = 'DELETED' THEN credit_amount ELSE 0 END)       AS credit_deleted,
                SUM(CASE WHEN upper(credit_status) = 'USED' THEN credit_amount ELSE 0 END)          AS credit_used,
                SUM(CASE WHEN upper(credit_status) = 'USED_TB' THEN credit_amount ELSE 0 END)       AS credit_used_tb,
                SUM(CASE WHEN upper(credit_status) = 'REFUNDED_CASH' THEN credit_amount ELSE 0 END) AS credit_refunded_cash,
                SUM(credit_amount)                                                                  AS credit_amount_all
         FROM se.data.se_credit_model
         GROUP BY 1
     ),
     --temporary fix to dedup so i can validate the rest of the SQL is not duplicating, I have asked Mike in tech support, and will replace witht the fixed version
     booking_cancellation_temp_fix AS
         (
             SELECT *
             FROM data_vault_mvp.cms_mysql_snapshots.booking_cancellation_snapshot
                 QUALIFY ROW_NUMBER() OVER (PARTITION BY COALESCE(booking_id, reservation_id) ORDER BY last_updated DESC) = 1
         ),
     world_pay_by_order_code_cur
         AS
         (
             SELECT order_code,
                    currency_code,
                    MIN(event_date::DATE)                                      AS min_event_date,
                    MAX(event_date::DATE)                                      AS max_event_date,
                    COUNT(event_date)                                          AS number_events,
                    sum(CASE WHEN amount < 0 THEN amount * -1 ELSE amount END) AS amount
             FROM raw_vault_mvp.worldpay.transaction_summary
             WHERE lower(status) IN ('refunded', 'refunded_by_merchant')
             GROUP BY 1, 2
         ),
     ratepay_by_shopsorder_cur
         AS
         (
             SELECT shopsorder_id,
                    currency,
                    sum(amount_gross)           AS amount_gross,
                    sum(disagio_gross)          AS disagio_gross,
                    sum(transactionfee_gross)   AS transactionfee_gross,
                    sum(paymentchangefee_gross) AS paymentchangefee_gross
             FROM raw_vault_mvp.ratepay.clearing
             WHERE lower(entry_type) IN ('5', '6', 'return', 'credit')
             GROUP BY 1, 2
         ),
     chargeback_catalogue AS
         (
             SELECT SPLIT(reference, '-')[1]                                        AS reference,
                    ccy,
                    SUM(CASE WHEN lower(result) = 'lost' THEN amount ELSE 0 END)    AS lost_amount,
                    SUM(CASE WHEN lower(result) = 'won' THEN amount ELSE 0 END)     AS won_amount,
                    SUM(CASE WHEN lower(result) = 'pending' THEN amount ELSE 0 END) AS pending_amount,
                    sum(amount)                                                     AS amount
             FROM raw_vault_mvp.finance_gsheets.chargebacks_catalogue
             GROUP BY 1, 2
         ),
     carrier_data_flight_report
         AS
         (
             SELECT external_reference,
                    airline_name,
                    supplier,
                    overall_booking_status,
                    flight_booking_status,
                    cost_in_buying_currency,
                    cost_in_gbp,
                    member_refund_type,
                    booking_system,
                    ROW_NUMBER() OVER (
                        PARTITION BY external_reference
                        ORDER BY
                            flight_pnr
                        ) AS rank
             FROM raw_vault_mvp.finance_gsheets.cash_refunds_airline_refund_report
         ),
     carrier_data_flight_report_pnr
         AS
         (
             SELECT external_reference,
                    listagg(flight_pnr, '/') AS flight_pnr
             FROM raw_vault_mvp.finance_gsheets.cash_refunds_airline_refund_report
             GROUP BY 1
         )
SELECT bs.*,
       sf.case_number,
       sf.case_owner_full_name,
       sf.transaction_id,
       sf.unique_transaction_id                  AS sf_unique_transaction_id,
       sf.subject,
       sf.opportunity_sale_id,
       sf.status,
       sfdc.number_dup_cases_solved,
       sf.case_origin,
       sf."VIEW",
       sf.booking_lookup_check_in_date,
       sf.booking_lookup_check_out_date,
       sf.requested_rebooking_date,
       sf.postponed_booking_request,
       sf.booking_lookup_store_id,
       sf.booking_lookup_supplier_territory,
       sf.contact_reason,
       sf.last_modified_by_full_name,
       sf.overbooking_rebooking_stage,
       sf.reason,
       sf.case_id,
       sf.date_time_opened,
       sf.case_name,
       sf.last_modified_date,
       sf.last_modified_by_case_overview,
       sf.priority_type,
       sf.covid19_member_resolution_cs,
       sf.case_overview_id,
       sf.case_thread_id,
       sf.priority,
       CASE
           WHEN LOWER(sf.subject) LIKE '%refund%' THEN 'Member asked for refund'
           WHEN LOWER(sf.subject) LIKE '%storn%' THEN 'Member asked for refund'
           WHEN LOWER(sf.subject) LIKE '%cxl%' THEN 'Member asked for refund'
           WHEN LOWER(sf.subject) LIKE '%cancel%' THEN 'Member asked for refund'
           WHEN lower("VIEW") LIKE ('%rebook%') AND requested_rebooking_date IS NOT NULL
               THEN 'Member asked for rebooking with date'
           WHEN lower("VIEW") LIKE ('%rebook%') AND requested_rebooking_date IS NULL
               THEN 'Member asked for rebooking without date'
           ELSE 'Unknown' END                    AS status_se_ho,
       CASE
           WHEN badj.adjusted_check_in_date IS NOT NULL THEN 'IHP/H+ rebooked'
           WHEN sf.reason = 'epidemic packages- cash 100%' AND sf.covid19_member_resolution_cs = 'Resolution accepted' AND
                lower(fin.flight_and_non_flight_components_held) = 'yes' THEN 'Refund Cash - Actioned from booking'
           WHEN sf.reason = 'epidemic packages- cash 100%' AND sf.covid19_member_resolution_cs = 'Resolution accepted' AND
                lower(fin.flight_and_non_flight_components_held) = 'no' THEN 'Refund Cash - Actioned from fund'
           WHEN sf."VIEW" IN
                ('**COVID-19 DACH P1/P2 Refusal View**', '**COVID-19 UK&INTL P1/P2 Refusal View**', '**Exec Complaints View**',
                 '**Social Media View**', '**COVID-19 UK/US and INTL Parked View**', '**COVID-19 DACH Parked View**')
               THEN 'Refund Cash - Requested (Credit refusal)'
           WHEN sf.covid19_member_resolution_cs IN ('Resolution accepted') AND cr.credit_active > 0 THEN 'Credit Accepted'
           WHEN sf."VIEW" NOT IN
                ('**COVID-19 DACH P1/P2 Refusal View**', '**COVID-19 UK&INTL P1/P2 Refusal View**', '**Exec Complaints View**',
                 '**Social Media View**', '**COVID-19 UK/US and INTL Parked View**', '**COVID-19 DACH Parked View**')
               AND lower(status) IN ('open', 'pending', 'hold') THEN 'in-progress'
           WHEN sf."VIEW" NOT IN
                ('**COVID-19 DACH P1/P2 Refusal View**', '**COVID-19 UK&INTL P1/P2 Refusal View**', '**Exec Complaints View**',
                 '**Social Media View**', '**COVID-19 UK/US and INTL Parked View**', '**COVID-19 DACH Parked View**')
               AND lower(status) IN ('new') AND sf.case_name IS NULL THEN 'new - not yet action'
           WHEN cr.credit_active > 0 THEN 'Credit Issued'
           WHEN cr.credit_deleted > 0 THEN 'Credit Deleted'
           WHEN bc.refund_channel = 'PAYMENT_METHOD' THEN 'Refunded by Payment Method'
           WHEN bs.checkin <= '2020-03-17' THEN 'Pre 17 March CheckIn'
           ELSE 'No Credit Issued'
           END                                   AS status_se_pkg,
       badj.adjusted_check_in_date,
       badj.adjusted_check_out_date,
       badj.voucher_stay_by_date,
       bc.date_created                           AS bk_cnx_date,
       bc.last_updated                           AS bk_cnx_last_updated,
       bc.fault                                  AS bk_cnx_fault,
       bc.reason                                 AS bk_cnx_reason,
       -- bc.BOOKING_FEE                            AS BK_CNX_BOOKING_FEE,
       -- bc.CC_FEE                                 AS BK_CNX_CC_FEE,
       -- bc.HOTEL_GOOD_WILL                        AS BK_CNX_HOTEL_GOOD_WILL,
       bc.refund_channel                         AS bk_cnx_refund_channel,
       bc.refund_type                            AS bk_cnx_refund_type,
       -- bc.SE_GOOD_WILL                           AS BK_CNX_SE_GOOD_WILL,
       bc.who_pays                               AS bk_cnx_who_pays,
       bc.cancel_with_provider                   AS bk_cnx_cancel_with_provider,
       -- COALESCE(bc.BOOKING_FEE, 0) + COALESCE(bc.CC_FEE, 0) + COALESCE(bc.SE_GOOD_WILL, 0) +
       --COALESCE(bc.HOTEL_GOOD_WILL, 0)           AS BK_CNX_PARTIAL_REFUND_AMOUNT,
       cr.credit_active,
       cr.credit_deleted,
       cr.credit_used,
       cr.credit_used_tb,
       cr.credit_refunded_cash,

       cbse.date                                 AS cb_se_date,
       cbse.order_code                           AS cb_se_order_code,
       cbse.payment_method                       AS cb_se_payment_method,
       cbse.currency                             AS cb_se_currency,
       cbse.payment_amount                       AS cb_se_payment_amount,
       cbse.cb_status                            AS cb_se_status,

       cbctl.reference                           AS cb_ctl_reference,
       cbctl.ccy                                 AS cb_ctl_ccy,
       cbctl.lost_amount                         AS cb_ctl_amount_lost,
       cbctl.pending_amount                      AS cb_ctl_amount_pending,
       cbctl.won_amount                          AS cb_ctl_amount_won,

       mancb.refund_timestamp                    AS manual_bacs_refund_timestamp,
       mancb.payment_status                      AS manual_bacs_payment_status,
       mancb.customer_currency                   AS manual_bacs_customer_currency,
       mancb.amount_in_customer_currency         AS manual_bacs_amount_in_customer_currency,
       mancb.bank_details_type                   AS manual_bacs_bank_details_type,
       mancb.product_type                        AS manual_bacs_product_typefull_cms_transaction_id,
       mancb.full_cms_transaction_id             AS manual_bacs_full_cms_transaction_id,
       mancb.type_of_refund                      AS manual_bacs_type_of_refund,
       mancb.reference_transaction_id            AS manual_bacs_reference_transaction_id,
       mancb.refund_speed                        AS manual_bacs_refund_speed,
       mancb.duplicate                           AS manual_bacs_duplicate,
       mancb.cb_raised                           AS manual_bacs_cb_raised,
       mancb.fraud_team_comment                  AS manual_bacs_fraud_team_comment,

       fin.booking_id                            AS finance_booking_id,
       fin.include_flight                        AS finance_include_flight,
       fin.net_amount_paid_fx                    AS finance_net_amount_paid_fx,
       fin.net_amount_paid_gbp                   AS finance_net_amount_paid_gbp,
       fin.non_flight_spls_cash_held             AS finance_non_flight_spls_cash_held,
       fin.non_flight_vcc_held                   AS finance_non_flight_vcc_held,
       fin.flight_refunds_received_gbp           AS finance_flight_refunds_received_gbp,
       fin.total_held_gbp                        AS finance_total_held_gbp,
       fin.perc_held                             AS finance_perc_held,
       fin.flight_and_non_flight_components_held AS finance_flight_and_non_flight_components_held,
       fin.refund_made                           AS finance_refund_made,
       fin.refund_type                           AS finance_refund_type,
       fin.amount                                AS finance_amount,
       fin.chargeback                            AS finance_chargeback,
       fin.currency                              AS finance_currency,
       fin.amount_inc_margin_adj                 AS finance_amount_inc_margin_adj,

       pnr.flight_pnr,

       cr1.airline_name                          AS car_airline_name_1,
       cr1.supplier                              AS car_supplier_1,
       cr1.overall_booking_status                AS car_overall_booking_status_1,
       cr1.flight_booking_status                 AS car_flight_booking_status_1,
       cr1.cost_in_buying_currency               AS car_cost_in_buying_currency_1,
       cr1.cost_in_gbp                           AS car_cost_in_gbp_1,
       cr1.member_refund_type                    AS car_member_refund_type_1,
       cr1.booking_system                        AS car_booking_system_1,

       cr2.airline_name                          AS car_airline_name_2,
       cr2.supplier                              AS car_supplier_2,
       cr2.overall_booking_status                AS car_overall_booking_status_2,
       cr2.flight_booking_status                 AS car_flight_booking_status_2,
       cr2.cost_in_buying_currency               AS car_cost_in_buying_currency_2,
       cr2.cost_in_gbp                           AS car_cost_in_gbp_2,
       cr2.member_refund_type                    AS car_member_refund_type_2,
       cr2.booking_system                        AS car_booking_system_2,

       cr3.airline_name                          AS car_airline_name_3,
       cr3.supplier                              AS car_supplier_3,
       cr3.overall_booking_status                AS car_overall_booking_status_3,
       cr3.flight_booking_status                 AS car_flight_booking_status_3,
       cr3.cost_in_buying_currency               AS car_cost_in_buying_currency_3,
       cr3.cost_in_gbp                           AS car_cost_in_gbp_3,
       cr3.member_refund_type                    AS car_member_refund_type_3,
       cr3.booking_system                        AS car_booking_system_3,
       cr4.airline_name                          AS car_airline_name_4,

       cr4.supplier                              AS car_supplier_4,
       cr4.overall_booking_status                AS car_overall_booking_status_4,
       cr4.flight_booking_status                 AS car_flight_booking_status_4,
       cr4.cost_in_buying_currency               AS car_cost_in_buying_currency_4,
       cr4.cost_in_gbp                           AS car_cost_in_gbp_4,
       cr4.member_refund_type                    AS car_member_refund_type_4,
       cr4.booking_system                        AS car_booking_system_4,

       wp.min_event_date                         AS world_pay_min_event_date,
       wp.max_event_date                         AS world_pay_max_event_date,
       wp.number_events                          AS world_pay_number_events,
       wp.currency_code                          AS world_pay_currency,
       wp.amount                                 AS world_pay_amount,

       rp.currency                               AS rate_pay_currency,
       rp.amount_gross                           AS rate_pay_amount_gross,
       rp.disagio_gross                          AS rate_pay_disagio_gross,
       rp.transactionfee_gross                   AS rate_pay_transactionfee_gross,
       rp.paymentchangefee_gross                 AS rate_pay_paymentchangefee_gross,

       str.type                                  AS stripe_type,
       str.source                                AS stripe_source,
       str.amount                                AS stripe_amount,
       str.fee                                   AS stripe_fee,
       str.destination_platform_fee              AS stripe_destination_platform_fee,
       str.net                                   AS stripe_net,
       str.currency                              AS stripe_currency,
       str.created_utc                           AS stripe_created_utc,
       str.available_on_utc                      AS stripe_available_on_utc,
       str.description                           AS stripe_description,
       str.customer_facing_amount                AS stripe_customer_facing_amount,
       str.customer_facing_currency              AS stripe_customer_facing_currency,
       str.transfer                              AS stripe_transfer,
       str.transfer_date_utc                     AS stripe_transfer_date_utc,
       str.transfer_group                        AS stripe_transfer_group,
       str.order_id_metadata                     AS stripe_order_id_metadata,
       str.payment_id_metadata                   AS stripe_payment_id_metadata,
       str.session_key_metadata                  AS stripe_session_key_metadata,
       str.offer_id_metadata                     AS stripe_offer_id_metadata,

       cmap.updated                              AS carrier_mapping_1_updated,
       cmap.flight_carrier                       AS carrier_mapping_1_flight_carrier,
       cmap.type                                 AS carrier_mapping_1_type,
       cmap.refund_type                          AS carrier_mapping_1_refund_type,
       cmap.reported_refund_type                 AS carrier_mapping_1_reported_refund_type
FROM se.data_pii.se_booking_summary_extended bs
         LEFT JOIN salesforce_data_clean sf ON bs.transactionid = sf.unique_transaction_id
         LEFT JOIN duplicated_solved_count sfdc ON bs.transactionid = sfdc.unique_transaction_id
         LEFT JOIN chargeback_catalogue cbctl ON cbctl.reference = bs.booking_id
         LEFT JOIN raw_vault_mvp.finance_gsheets.chargebacks_se cbse ON cbse.booking_id = bs.booking_id
         LEFT JOIN raw_vault_mvp.finance_gsheets.manual_refunds mancb ON mancb.full_cms_transaction_id = bs.transactionid
         LEFT JOIN (
    SELECT *
    FROM carrier_data_flight_report
    WHERE rank = 1
) cr1 ON cr1.external_reference = bs.transactionid
         LEFT JOIN (
    SELECT *
    FROM carrier_data_flight_report
    WHERE rank = 2
) cr2 ON cr2.external_reference = bs.transactionid
         LEFT JOIN (
    SELECT *
    FROM carrier_data_flight_report
    WHERE rank = 3
) cr3 ON cr3.external_reference = bs.transactionid
         LEFT JOIN (
    SELECT *
    FROM carrier_data_flight_report
    WHERE rank = 4
) cr4 ON cr4.external_reference = bs.transactionid
         LEFT JOIN (
    SELECT *
    FROM carrier_data_flight_report_pnr
) pnr ON pnr.external_reference = bs.transactionid
         LEFT JOIN raw_vault_mvp.finance_gsheets.cash_refunds_to_members fin ON fin.cms_transaction_id = bs.transactionid
         LEFT JOIN se_booking_adj badj ON bs.booking_id = badj.booking_id
         LEFT JOIN world_pay_by_order_code_cur wp ON wp.order_code = bs.uniquetransactionreference
         LEFT JOIN ratepay_by_shopsorder_cur rp ON rp.shopsorder_id = bs.booking_id
         LEFT JOIN raw_vault_mvp.finance_gsheets.cash_refunds_stripe str ON SUBSTR(str.id, 5) = bs.uniquetransactionreference
         LEFT JOIN raw_vault_mvp.finance_gsheets.cash_refunds_airline_refund_status cmap ON cmap.flight_carrier = cr1.airline_name
         LEFT JOIN booking_cancellation_temp_fix bc ON COALESCE(bc.booking_id::VARCHAR, 'A' || bc.reservation_id::VARCHAR) = CASE
                                                                                                                                 WHEN LEFT(bs.transactionid, 1) = 'A'
                                                                                                                                     THEN 'A' || REGEXP_SUBSTR(bs.transactionid, '-.*-(.*)', 1, 1, 'e')
                                                                                                                                 ELSE REGEXP_SUBSTR(bs.transactionid, '-.*-(.*)', 1, 1, 'e') END
         LEFT JOIN credit_data cr ON CASE
                                         WHEN LEFT(bs.transactionid, 1) = 'A'
                                             THEN 'A' || REGEXP_SUBSTR(bs.transactionid, '-.*-(.*)', 1, 1, 'e')
                                         ELSE REGEXP_SUBSTR(bs.transactionid, '-.*-(.*)', 1, 1, 'e') END =
                                     cr.original_se_booking_id;

------------------------------------------------------------------------------------------------------------------------


CREATE OR REPLACE VIEW collab.covid_pii.covid_master_list_ho_packages
    COPY GRANTS
AS
WITH combine_sales_force
         AS (
        SELECT *
        FROM raw_vault_mvp.sfsc.rebooking_request_cases_pkg
        UNION
        SELECT *
        FROM raw_vault_mvp.sfsc.rebooking_request_cases_ho
    ),
     step01__filter_internal
         AS (
         SELECT transaction_id,
                booking_id,
                booking_lookup_check_in_date, -- some records used to come in without :SS portion
                booking_lookup_check_out_date,
                booking_lookup_store_id,
                booking_lookup_supplier_territory,
                case_number::INT                               AS case_number,
                case_origin,
                case_owner_full_name,
                contact_reason,
                opportunity_sale_id,
                LOWER(status)                                  AS status,
                CASE
                    WHEN LOWER(status) = 'hold' THEN 1
                    WHEN LOWER(status) = 'pending' THEN 1
                    WHEN LOWER(status) = 'open' THEN 1
                    WHEN LOWER(status) = 'new' THEN 2
                    WHEN LOWER(status) = 'solved' THEN 3
                    WHEN LOWER(status) = 'closed' THEN 4
                    ELSE 99
                    END                                        AS status_rank,
                LOWER(subject)                                 AS subject,
                CASE
                    WHEN LOWER(subject) LIKE '%amend%' AND requested_rebooking_date IS NOT NULL
                        THEN 'Member asked for rebooking with date'
                    WHEN LOWER(subject) LIKE '%amend%' AND requested_rebooking_date IS NULL
                        THEN 'Member asked for rebooking without date'
                    WHEN LOWER(subject) LIKE '%refund%' THEN 'Member asked for refund'
                    WHEN LOWER(subject) LIKE '%storn%' THEN 'Member asked for refund'
                    WHEN LOWER(subject) LIKE '%cxl%' THEN 'Member asked for refund'
                    WHEN LOWER(subject) LIKE '%cancel%' THEN 'Member asked for refund'
                    ELSE 'Unknown' END                         AS status_se,
                "VIEW",
                TRY_CAST(postponed_booking_request AS BOOLEAN) AS postponed_booking_request,
                requested_rebooking_date,
                LOWER(last_modified_by_full_name)              AS last_modified_by_full_name,
                LOWER(overbooking_rebooking_stage)             AS overbooking_rebooking_stage,
                LOWER(reason)                                  AS reason,
                case_id,
                date_time_opened,
                case_name::INT                                 AS case_name,
                last_modified_date,
                last_modified_by_case_overview,
                priority_type,
                covid19_member_resolution_cs,
                case_overview_id,
                case_thread_id,
                priority,
                -- CS sometimes put booking id in transaction id field
                COALESCE(transaction_id, booking_id)           AS unique_transaction_id,
                -- hygiene flags
                CASE
                    WHEN unique_transaction_id IS NULL
                        THEN 1
                    END                                        AS fails_validation__unique_transaction_id__expected_nonnull,
                CASE
                    WHEN fails_validation__unique_transaction_id__expected_nonnull = 1
                        THEN 1
                    END                                        AS failed_some_validation,
                ROW_NUMBER() OVER (
                    PARTITION BY unique_transaction_id
                    ORDER BY
                        status_rank ASC,
                        case_number DESC,
                        case_name DESC
                    )                                          AS rank
         FROM combine_sales_force
         WHERE lower(last_modified_by_full_name) NOT IN ('dylan hone', 'kate donaghy', 'jessica ho')
           AND NOT (
                 lower(last_modified_by_full_name) = 'marta lagut'
                 AND case_name IS NULL
                 AND lower(status) = 'solved'
             )
           AND lower(status) != 'closed'
     ),
     duplicated_solved_count AS
         (
             SELECT unique_transaction_id,
                    SUM(CASE WHEN rank > 1 THEN 1 ELSE 0 END) AS number_dup_cases_solved
             FROM step01__filter_internal
             WHERE lower(status) = 'solved'
             GROUP BY 1
         ),
     salesforce_data_clean AS (
         SELECT *
         FROM step01__filter_internal
         WHERE rank = 1
     ),
     se_booking_adj AS (
         SELECT
             -- a booking can have multiple adjustments but we needs the dates associated with
             -- the most recent one
             -- TODO: if we need this elsewhere too we should define it in a `dv` module
             -- or change `cms_mysql_snapshot_bulk_wave2.booking_adjustment` to deduplicate on `booking_id` instead of `id`
             booking_id,
             check_in_date::DATE  AS adjusted_check_in_date,
             check_out_date::DATE AS adjusted_check_out_date,
             stay_by_date         AS voucher_stay_by_date
         FROM data_vault_mvp.cms_mysql_snapshots.amendment_snapshot
             QUALIFY ROW_NUMBER() OVER (PARTITION BY booking_id ORDER BY date_created DESC) = 1
     ),
     credit_data AS (
         SELECT original_se_booking_id,
                SUM(CASE WHEN upper(credit_status) = 'ACTIVE' THEN credit_amount ELSE 0 END)        AS credit_active,
                SUM(CASE WHEN upper(credit_status) = 'DELETED' THEN credit_amount ELSE 0 END)       AS credit_deleted,
                SUM(CASE WHEN upper(credit_status) = 'USED' THEN credit_amount ELSE 0 END)          AS credit_used,
                SUM(CASE WHEN upper(credit_status) = 'USED_TB' THEN credit_amount ELSE 0 END)       AS credit_used_tb,
                SUM(CASE WHEN upper(credit_status) = 'REFUNDED_CASH' THEN credit_amount ELSE 0 END) AS credit_refunded_cash,
                SUM(credit_amount)                                                                  AS credit_amount_all
         FROM se.data.se_credit_model
         GROUP BY 1
     ),
     --temporary fix to dedup so i can validate the rest of the SQL is not duplicating, I have asked Mike in tech support, and will replace witht the fixed version
     booking_cancellation_temp_fix AS
         (
             SELECT *
             FROM data_vault_mvp.cms_mysql_snapshots.booking_cancellation_snapshot
                 QUALIFY ROW_NUMBER() OVER (PARTITION BY COALESCE(booking_id, reservation_id) ORDER BY last_updated DESC) = 1
         ),
     world_pay_by_order_code_cur
         AS
         (
             SELECT order_code,
                    currency_code,
                    MIN(event_date::DATE)                                      AS min_event_date,
                    MAX(event_date::DATE)                                      AS max_event_date,
                    COUNT(event_date)                                          AS number_events,
                    SUM(CASE WHEN amount < 0 THEN amount * -1 ELSE amount END) AS amount
             FROM hygiene_snapshot_vault_mvp.worldpay.transaction_summary
             WHERE lower(status) IN ('refunded', 'refunded_by_merchant')
             GROUP BY 1, 2
         ),
     ratepay_by_shopsorder_cur
         AS
         (
             SELECT shopsorder_id,
                    currency,
                    sum(amount_gross)           AS amount_gross,
                    sum(disagio_gross)          AS disagio_gross,
                    sum(transactionfee_gross)   AS transactionfee_gross,
                    sum(paymentchangefee_gross) AS paymentchangefee_gross
             FROM raw_vault_mvp.ratepay.clearing
             WHERE lower(entry_type) IN ('5', '6', 'return', 'credit')
             GROUP BY 1, 2
         ),
     chargeback_catalogue AS
         (
             SELECT SPLIT(reference, '-')[1]                                        AS reference,
                    ccy,
                    SUM(CASE WHEN lower(result) = 'lost' THEN amount ELSE 0 END)    AS lost_amount,
                    SUM(CASE WHEN lower(result) = 'won' THEN amount ELSE 0 END)     AS won_amount,
                    SUM(CASE WHEN lower(result) = 'pending' THEN amount ELSE 0 END) AS pending_amount,
                    sum(amount)                                                     AS amount
             FROM raw_vault_mvp.finance_gsheets.chargebacks_catalogue
             GROUP BY 1, 2
         ),
     carrier_data_flight_report
         AS
         (
             SELECT external_reference,
                    airline_name,
                    supplier,
                    overall_booking_status,
                    flight_booking_status,
                    cost_in_buying_currency,
                    cost_in_gbp,
                    member_refund_type,
                    booking_system,
                    ROW_NUMBER() OVER (
                        PARTITION BY external_reference
                        ORDER BY
                            flight_pnr
                        ) AS rank
             FROM raw_vault_mvp.finance_gsheets.cash_refunds_airline_refund_report
         ),
     carrier_data_flight_report_pnr
         AS
         (
             SELECT external_reference,
                    listagg(flight_pnr, '/') AS flight_pnr
             FROM raw_vault_mvp.finance_gsheets.cash_refunds_airline_refund_report
             GROUP BY 1
         )
SELECT bs.*,
       sf.case_number,
       sf.case_owner_full_name,
       sf.transaction_id,
       sf.unique_transaction_id                  AS sf_unique_transaction_id,
       sf.subject,
       sf.opportunity_sale_id,
       sf.status,
       sfdc.number_dup_cases_solved,
       sf.case_origin,
       sf."VIEW",
       sf.booking_lookup_check_in_date,
       sf.booking_lookup_check_out_date,
       sf.requested_rebooking_date,
       sf.postponed_booking_request,
       sf.booking_lookup_store_id,
       sf.booking_lookup_supplier_territory,
       sf.contact_reason,
       sf.last_modified_by_full_name,
       sf.overbooking_rebooking_stage,
       sf.reason,
       sf.case_id,
       sf.date_time_opened,
       sf.case_name,
       sf.last_modified_date,
       sf.last_modified_by_case_overview,
       sf.priority_type,
       sf.covid19_member_resolution_cs,
       sf.case_overview_id,
       sf.case_thread_id,
       sf.priority,
       CASE
           WHEN LOWER(sf.subject) LIKE '%refund%' THEN 'Member asked for refund'
           WHEN LOWER(sf.subject) LIKE '%storn%' THEN 'Member asked for refund'
           WHEN LOWER(sf.subject) LIKE '%cxl%' THEN 'Member asked for refund'
           WHEN LOWER(sf.subject) LIKE '%cancel%' THEN 'Member asked for refund'
           WHEN lower("VIEW") LIKE ('%rebook%') AND requested_rebooking_date IS NOT NULL
               THEN 'Member asked for rebooking with date'
           WHEN lower("VIEW") LIKE ('%rebook%') AND requested_rebooking_date IS NULL
               THEN 'Member asked for rebooking without date'
           ELSE 'Unknown' END                    AS status_se_ho,
       CASE
           WHEN badj.adjusted_check_in_date IS NOT NULL THEN 'IHP/H+ rebooked'
           WHEN sf.reason = 'epidemic packages- cash 100%' AND sf.covid19_member_resolution_cs = 'Resolution accepted' AND
                lower(fin.flight_and_non_flight_components_held) = 'yes' THEN 'Refund Cash - Actioned from booking'
           WHEN sf.reason = 'epidemic packages- cash 100%' AND sf.covid19_member_resolution_cs = 'Resolution accepted' AND
                lower(fin.flight_and_non_flight_components_held) = 'no' THEN 'Refund Cash - Actioned from fund'
           WHEN sf."VIEW" IN
                ('**COVID-19 DACH P1/P2 Refusal View**', '**COVID-19 UK&INTL P1/P2 Refusal View**', '**Exec Complaints View**',
                 '**Social Media View**', '**COVID-19 UK/US and INTL Parked View**', '**COVID-19 DACH Parked View**')
               THEN 'Refund Cash - Requested (Credit refusal)'
           WHEN sf.covid19_member_resolution_cs IN ('Resolution accepted') AND cr.credit_active > 0 THEN 'Credit Accepted'
           WHEN sf."VIEW" NOT IN
                ('**COVID-19 DACH P1/P2 Refusal View**', '**COVID-19 UK&INTL P1/P2 Refusal View**', '**Exec Complaints View**',
                 '**Social Media View**', '**COVID-19 UK/US and INTL Parked View**', '**COVID-19 DACH Parked View**')
               AND lower(status) IN ('open', 'pending', 'hold') THEN 'in-progress'
           WHEN sf."VIEW" NOT IN
                ('**COVID-19 DACH P1/P2 Refusal View**', '**COVID-19 UK&INTL P1/P2 Refusal View**', '**Exec Complaints View**',
                 '**Social Media View**', '**COVID-19 UK/US and INTL Parked View**', '**COVID-19 DACH Parked View**')
               AND lower(status) IN ('new') AND sf.case_name IS NULL THEN 'new - not yet action'
           WHEN cr.credit_active > 0 THEN 'Credit Issued'
           WHEN cr.credit_deleted > 0 THEN 'Credit Deleted'
           WHEN bc.refund_channel = 'PAYMENT_METHOD' THEN 'Refunded by Payment Method'
           WHEN bs.checkin <= '2020-03-17' THEN 'Pre 17 March CheckIn'
           ELSE 'No Credit Issued'
           END                                   AS status_se_pkg,
       badj.adjusted_check_in_date,
       badj.adjusted_check_out_date,
       badj.voucher_stay_by_date,
       bc.date_created                           AS bk_cnx_date,
       bc.last_updated                           AS bk_cnx_last_updated,
       bc.fault                                  AS bk_cnx_fault,
       bc.reason                                 AS bk_cnx_reason,
       -- bc.BOOKING_FEE                            AS BK_CNX_BOOKING_FEE,
       -- bc.CC_FEE                                 AS BK_CNX_CC_FEE,
       -- bc.HOTEL_GOOD_WILL                        AS BK_CNX_HOTEL_GOOD_WILL,
       bc.refund_channel                         AS bk_cnx_refund_channel,
       bc.refund_type                            AS bk_cnx_refund_type,
       -- bc.SE_GOOD_WILL                           AS BK_CNX_SE_GOOD_WILL,
       bc.who_pays                               AS bk_cnx_who_pays,
       bc.cancel_with_provider                   AS bk_cnx_cancel_with_provider,
       -- COALESCE(bc.BOOKING_FEE, 0) + COALESCE(bc.CC_FEE, 0) + COALESCE(bc.SE_GOOD_WILL, 0) +
       --COALESCE(bc.HOTEL_GOOD_WILL, 0)           AS BK_CNX_PARTIAL_REFUND_AMOUNT,
       cr.credit_active,
       cr.credit_deleted,
       cr.credit_used,
       cr.credit_used_tb,
       cr.credit_refunded_cash,
       cbse.date                                 AS cb_se_date,
       cbse.order_code                           AS cb_se_order_code,
       cbse.payment_method                       AS cb_se_payment_method,
       cbse.currency                             AS cb_se_currency,
       cbse.payment_amount                       AS cb_se_payment_amount,
       cbse.cb_status                            AS cb_se_status,
       cbctl.reference                           AS cb_ctl_reference,
       cbctl.ccy                                 AS cb_ctl_ccy,
       cbctl.lost_amount                         AS cb_ctl_amount_lost,
       cbctl.pending_amount                      AS cb_ctl_amount_pending,
       cbctl.won_amount                          AS cb_ctl_amount_won,
       mancb.refund_timestamp                    AS manual_bacs_refund_timestamp,
       mancb.payment_status                      AS manual_bacs_payment_status,
       mancb.customer_currency                   AS manual_bacs_customer_currency,
       mancb.amount_in_customer_currency         AS manual_bacs_amount_in_customer_currency,
       mancb.bank_details_type                   AS manual_bacs_bank_details_type,
       mancb.product_type                        AS manual_bacs_product_typefull_cms_transaction_id,
       mancb.full_cms_transaction_id             AS manual_bacs_full_cms_transaction_id,
       mancb.type_of_refund                      AS manual_bacs_type_of_refund,
       mancb.reference_transaction_id            AS manual_bacs_reference_transaction_id,
       mancb.refund_speed                        AS manual_bacs_refund_speed,
       mancb.duplicate                           AS manual_bacs_duplicate,
       mancb.cb_raised                           AS manual_bacs_cb_raised,
       mancb.fraud_team_comment                  AS manual_bacs_fraud_team_comment,
       fin.booking_id                            AS finance_booking_id,
       fin.include_flight                        AS finance_include_flight,
       fin.net_amount_paid_fx                    AS finance_net_amount_paid_fx,
       fin.net_amount_paid_gbp                   AS finance_net_amount_paid_gbp,
       fin.non_flight_spls_cash_held             AS finance_non_flight_spls_cash_held,
       fin.non_flight_vcc_held                   AS finance_non_flight_vcc_held,
       fin.flight_refunds_received_gbp           AS finance_flight_refunds_received_gbp,
       fin.total_held_gbp                        AS finance_total_held_gbp,
       fin.perc_held                             AS finance_perc_held,
       fin.flight_and_non_flight_components_held AS finance_flight_and_non_flight_components_held,
       fin.refund_made                           AS finance_refund_made,
       fin.refund_type                           AS finance_refund_type,
       fin.amount                                AS finance_amount,
       fin.chargeback                            AS finance_chargeback,
       fin.currency                              AS finance_currency,
       fin.amount_inc_margin_adj                 AS finance_amount_inc_margin_adj,
       pnr.flight_pnr,
       cr1.airline_name                          AS car_airline_name_1,
       cr1.supplier                              AS car_supplier_1,
       cr1.overall_booking_status                AS car_overall_booking_status_1,
       cr1.flight_booking_status                 AS car_flight_booking_status_1,
       cr1.cost_in_buying_currency               AS car_cost_in_buying_currency_1,
       cr1.cost_in_gbp                           AS car_cost_in_gbp_1,
       cr1.member_refund_type                    AS car_member_refund_type_1,
       cr1.booking_system                        AS car_booking_system_1,
       cr2.airline_name                          AS car_airline_name_2,
       cr2.supplier                              AS car_supplier_2,
       cr2.overall_booking_status                AS car_overall_booking_status_2,
       cr2.flight_booking_status                 AS car_flight_booking_status_2,
       cr2.cost_in_buying_currency               AS car_cost_in_buying_currency_2,
       cr2.cost_in_gbp                           AS car_cost_in_gbp_2,
       cr2.member_refund_type                    AS car_member_refund_type_2,
       cr2.booking_system                        AS car_booking_system_2,
       cr3.airline_name                          AS car_airline_name_3,
       cr3.supplier                              AS car_supplier_3,
       cr3.overall_booking_status                AS car_overall_booking_status_3,
       cr3.flight_booking_status                 AS car_flight_booking_status_3,
       cr3.cost_in_buying_currency               AS car_cost_in_buying_currency_3,
       cr3.cost_in_gbp                           AS car_cost_in_gbp_3,
       cr3.member_refund_type                    AS car_member_refund_type_3,
       cr3.booking_system                        AS car_booking_system_3,
       cr4.airline_name                          AS car_airline_name_4,
       cr4.supplier                              AS car_supplier_4,
       cr4.overall_booking_status                AS car_overall_booking_status_4,
       cr4.flight_booking_status                 AS car_flight_booking_status_4,
       cr4.cost_in_buying_currency               AS car_cost_in_buying_currency_4,
       cr4.cost_in_gbp                           AS car_cost_in_gbp_4,
       cr4.member_refund_type                    AS car_member_refund_type_4,
       cr4.booking_system                        AS car_booking_system_4,
       wp.min_event_date                         AS world_pay_min_event_date,
       wp.max_event_date                         AS world_pay_max_event_date,
       wp.number_events                          AS world_pay_number_events,
       wp.currency_code                          AS world_pay_currency,
       wp.amount                                 AS world_pay_amount,
       rp.currency                               AS rate_pay_currency,
       rp.amount_gross                           AS rate_pay_amount_gross,
       rp.disagio_gross                          AS rate_pay_disagio_gross,
       rp.transactionfee_gross                   AS rate_pay_transactionfee_gross,
       rp.paymentchangefee_gross                 AS rate_pay_paymentchangefee_gross,
       str.type                                  AS stripe_type,
       str.source                                AS stripe_source,
       str.amount                                AS stripe_amount,
       str.fee                                   AS stripe_fee,
       str.destination_platform_fee              AS stripe_destination_platform_fee,
       str.net                                   AS stripe_net,
       str.currency                              AS stripe_currency,
       str.created_utc                           AS stripe_created_utc,
       str.available_on_utc                      AS stripe_available_on_utc,
       str.description                           AS stripe_description,
       str.customer_facing_amount                AS stripe_customer_facing_amount,
       str.customer_facing_currency              AS stripe_customer_facing_currency,
       str.transfer                              AS stripe_transfer,
       str.transfer_date_utc                     AS stripe_transfer_date_utc,
       str.transfer_group                        AS stripe_transfer_group,
       str.order_id_metadata                     AS stripe_order_id_metadata,
       str.payment_id_metadata                   AS stripe_payment_id_metadata,
       str.session_key_metadata                  AS stripe_session_key_metadata,
       str.offer_id_metadata                     AS stripe_offer_id_metadata,
       cmap.updated                              AS carrier_mapping_1_updated,
       cmap.flight_carrier                       AS carrier_mapping_1_flight_carrier,
       cmap.type                                 AS carrier_mapping_1_type,
       cmap.refund_type                          AS carrier_mapping_1_refund_type,
       cmap.reported_refund_type                 AS carrier_mapping_1_reported_refund_type
FROM se.data_pii.se_booking_summary_extended bs
         LEFT JOIN salesforce_data_clean sf ON bs.transactionid = sf.unique_transaction_id
         LEFT JOIN duplicated_solved_count sfdc ON bs.transactionid = sfdc.unique_transaction_id
         LEFT JOIN chargeback_catalogue cbctl ON cbctl.reference = bs.booking_id
         LEFT JOIN raw_vault_mvp.finance_gsheets.chargebacks_se cbse ON cbse.booking_id = bs.booking_id
         LEFT JOIN raw_vault_mvp.finance_gsheets.manual_refunds mancb ON mancb.full_cms_transaction_id = bs.transactionid
         LEFT JOIN (
    SELECT *
    FROM carrier_data_flight_report
    WHERE rank = 1
) cr1 ON cr1.external_reference = bs.transactionid
         LEFT JOIN (
    SELECT *
    FROM carrier_data_flight_report
    WHERE rank = 2
) cr2 ON cr2.external_reference = bs.transactionid
         LEFT JOIN (
    SELECT *
    FROM carrier_data_flight_report
    WHERE rank = 3
) cr3 ON cr3.external_reference = bs.transactionid
         LEFT JOIN (
    SELECT *
    FROM carrier_data_flight_report
    WHERE rank = 4
) cr4 ON cr4.external_reference = bs.transactionid
         LEFT JOIN (
    SELECT *
    FROM carrier_data_flight_report_pnr
) pnr ON pnr.external_reference = bs.transactionid
         LEFT JOIN raw_vault_mvp.finance_gsheets.cash_refunds_to_members fin ON fin.cms_transaction_id = bs.transactionid
         LEFT JOIN se_booking_adj badj ON bs.booking_id = badj.booking_id
         LEFT JOIN world_pay_by_order_code_cur wp ON wp.order_code = bs.uniquetransactionreference
         LEFT JOIN ratepay_by_shopsorder_cur rp ON rp.shopsorder_id = bs.booking_id
         LEFT JOIN raw_vault_mvp.finance_gsheets.cash_refunds_stripe str ON SUBSTR(str.id, 5) = bs.uniquetransactionreference
         LEFT JOIN raw_vault_mvp.finance_gsheets.cash_refunds_airline_refund_status cmap ON cmap.flight_carrier = cr1.airline_name
         LEFT JOIN booking_cancellation_temp_fix bc ON COALESCE(bc.booking_id::VARCHAR, 'A' || bc.reservation_id::VARCHAR) = CASE
                                                                                                                                 WHEN LEFT(bs.transactionid, 1) = 'A'
                                                                                                                                     THEN 'A' || REGEXP_SUBSTR(bs.transactionid, '-.*-(.*)', 1, 1, 'e')
                                                                                                                                 ELSE REGEXP_SUBSTR(bs.transactionid, '-.*-(.*)', 1, 1, 'e') END
         LEFT JOIN credit_data cr ON CASE
                                         WHEN LEFT(bs.transactionid, 1) = 'A'
                                             THEN 'A' || REGEXP_SUBSTR(bs.transactionid, '-.*-(.*)', 1, 1, 'e')
                                         ELSE REGEXP_SUBSTR(bs.transactionid, '-.*-(.*)', 1, 1, 'e') END =
                                     cr.original_se_booking_id;

GRANT SELECT ON TABLE COLLAB.covid_pii.covid_master_list_ho_packages TO ROLE personal_role__BARBARAFRASCOLI;
GRANT SELECT ON TABLE COLLAB.covid_pii.covid_master_list_ho_packages TO ROLE personal_role__TOMMARIANI;
GRANT SELECT ON TABLE COLLAB.covid_pii.covid_master_list_ho_packages TO ROLE personal_role__KIRSTENGRIEVE;
GRANT SELECT ON TABLE COLLAB.covid_pii.covid_master_list_ho_packages TO ROLE personal_role__WARSANABDULLAHI;
GRANT SELECT ON TABLE COLLAB.covid_pii.covid_master_list_ho_packages TO ROLE personal_role__JADEVALLANCE;
GRANT SELECT ON TABLE COLLAB.covid_pii.covid_master_list_ho_packages TO ROLE personal_role__OANAARBORE;
GRANT SELECT ON TABLE COLLAB.covid_pii.covid_master_list_ho_packages TO ROLE personal_role__EMMAHOLMES;
GRANT SELECT ON TABLE COLLAB.covid_pii.covid_master_list_ho_packages TO ROLE personal_role__MARIYATODOROVA;
GRANT SELECT ON TABLE COLLAB.covid_pii.covid_master_list_ho_packages TO ROLE personal_role__ASHMITABHIMJI;
GRANT SELECT ON TABLE COLLAB.covid_pii.covid_master_list_ho_packages TO ROLE personal_role__ROBINPATEL;
GRANT SELECT ON TABLE COLLAB.covid_pii.covid_master_list_ho_packages TO ROLE personal_role__SAMANTHAMANDELDALLAL;
GRANT SELECT ON TABLE COLLAB.covid_pii.covid_master_list_ho_packages TO ROLE personal_role__JOHANNALANTIGUA;
GRANT SELECT ON TABLE COLLAB.covid_pii.covid_master_list_ho_packages TO ROLE personal_role__JANHITZKE;
GRANT SELECT ON TABLE COLLAB.covid_pii.covid_master_list_ho_packages TO ROLE personal_role__MARKUSSCHAEFERHENKE;
GRANT SELECT ON TABLE COLLAB.covid_pii.covid_master_list_ho_packages TO ROLE personal_role__GIANNIRAFTIS;
GRANT SELECT ON TABLE COLLAB.covid_pii.covid_master_list_ho_packages TO ROLE personal_role__SHIANNESTANNARD;
GRANT SELECT ON TABLE COLLAB.covid_pii.covid_master_list_ho_packages TO ROLE personal_role__KONSTANTINEBERLE;
GRANT SELECT ON TABLE COLLAB.covid_pii.covid_master_list_ho_packages TO ROLE personal_role__CARMENMARDIROS;
GRANT SELECT ON TABLE COLLAB.covid_pii.covid_master_list_ho_packages TO ROLE personal_role__KATIEKEMP;
GRANT SELECT ON TABLE COLLAB.covid_pii.covid_master_list_ho_packages TO ROLE personal_role__MORGANAFOTI;
GRANT SELECT ON TABLE COLLAB.covid_pii.covid_master_list_ho_packages TO ROLE personal_role__NIROSHANBALAKUMAR;
GRANT SELECT ON TABLE COLLAB.covid_pii.covid_master_list_ho_packages TO ROLE personal_role__SAURDASH;
GRANT SELECT ON TABLE COLLAB.covid_pii.covid_master_list_ho_packages TO ROLE personal_role__TANITHSPINELLI;
GRANT SELECT ON TABLE COLLAB.covid_pii.covid_master_list_ho_packages TO ROLE personal_role__ALIZAIDI;
GRANT SELECT ON TABLE COLLAB.covid_pii.covid_master_list_ho_packages TO ROLE personal_role__MARTALAGUT;
GRANT SELECT ON TABLE COLLAB.covid_pii.covid_master_list_ho_packages TO ROLE personal_role__RADUJOSAN;
