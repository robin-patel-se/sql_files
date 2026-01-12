CREATE OR REPLACE VIEW collab.covid_pii.covid_master_list_catalogue
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
                case_number::INT                                     AS case_number,
                case_origin,
                case_owner_full_name,
                contact_reason,
                opportunity_sale_id,
                LOWER(status)                                        AS status,
                CASE
                    WHEN LOWER(status) = 'hold' THEN 1
                    WHEN LOWER(status) = 'pending' THEN 1
                    WHEN LOWER(status) = 'open' THEN 1
                    WHEN LOWER(status) = 'new' THEN 2
                    WHEN LOWER(status) = 'solved' THEN 3
                    WHEN LOWER(status) = 'closed' THEN 4
                    ELSE 99
                    END                                              AS status_rank,
                LOWER(subject)                                       AS subject,
                CASE
                    WHEN LOWER(subject) LIKE '%amend%' AND requested_rebooking_date IS NOT NULL
                        THEN 'Member asked for rebooking with date'
                    WHEN LOWER(subject) LIKE '%amend%' AND requested_rebooking_date IS NULL
                        THEN 'Member asked for rebooking without date'
                    WHEN LOWER(subject) LIKE '%refund%' THEN 'Member asked for refund'
                    WHEN LOWER(subject) LIKE '%storn%' THEN 'Member asked for refund'
                    WHEN LOWER(subject) LIKE '%cxl%' THEN 'Member asked for refund'
                    WHEN LOWER(subject) LIKE '%cancel%' THEN 'Member asked for refund'
                    ELSE 'Unknown' END                               AS status_se,
                "VIEW",
                TRY_CAST(postponed_booking_request AS BOOLEAN)       AS postponed_booking_request,
                requested_rebooking_date,
                LOWER(last_modified_by_full_name)                    AS last_modified_by_full_name,
                LOWER(overbooking_rebooking_stage)                   AS overbooking_rebooking_stage,
                LOWER(reason)                                        AS reason,
                case_id,
                date_time_opened,
                case_name::INT                                       AS case_name,
                last_modified_date,
                last_modified_by_case_overview,
                priority_type,
                covid19_member_resolution_cs,
                case_overview_id,
                case_thread_id,
                priority,
                -- CS sometimes put booking id in transaction id field
                COALESCE(transaction_id, booking_id)                 AS unique_transaction_id,
                REGEXP_SUBSTR(transaction_id, '-.*-(.*)', 1, 1, 'e') AS travel_bird_booking_id,
                -- hygiene flags
                CASE
                    WHEN travel_bird_booking_id IS NULL
                        THEN 1
                    END                                              AS fails_validation__unique_transaction_id__expected_nonnull,
                CASE
                    WHEN fails_validation__unique_transaction_id__expected_nonnull = 1
                        THEN 1
                    END                                              AS failed_some_validation,
                row_number                                           AS over (
                    partition BY travel_bird_booking_id
                    ORDER BY
                        status_rank ASC,
                        case_number DESC,
                        case_name DESC
                    )                                                AS rank
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
             SELECT travel_bird_booking_id,
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
     credit_data AS (
         SELECT cm.original_external_id,
                eb.external_id,
                eb.reference_id,
                SUM(CASE WHEN upper(credit_status) = 'ACTIVE' THEN credit_amount ELSE 0 END)        AS credit_active,
                SUM(CASE WHEN upper(credit_status) = 'DELETED' THEN credit_amount ELSE 0 END)       AS credit_deleted,
                SUM(CASE WHEN upper(credit_status) = 'USED' THEN credit_amount ELSE 0 END)          AS credit_used,
                SUM(CASE WHEN upper(credit_status) = 'USED_TB' THEN credit_amount ELSE 0 END)       AS credit_used_tb,
                SUM(CASE WHEN upper(credit_status) = 'REFUNDED_CASH' THEN credit_amount ELSE 0 END) AS credit_refunded_cash,
                SUM(credit_amount)                                                                  AS credit_amount_all
         FROM se.data.se_credit_model cm
                  LEFT JOIN data_vault_mvp.cms_mysql_snapshots.external_booking_snapshot eb ON eb.id = cm.original_external_id
         GROUP BY 1, 2, 3
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
     stripe_data AS
         (
             SELECT
                 --REGEXP_SUBSTR(description, '\\((.*)\\)', 1, 1, '', 1) AS REFERENCE_1,
                 --REGEXP_SUBSTR(description, '\\(\\S+ (.*)\\)', 1, 1, '', 1) AS REFERENCE_2,
                 COALESCE(order_id_metadata::VARCHAR, REGEXP_SUBSTR(description, '([0-9]+)\\)', 1, 1, '', 1)) AS order_id,
                 currency,
                 SUM(CASE WHEN lower(type) IN ('refund', 'payment_refund') THEN amount ELSE 0 END)            AS refunded_amount,
                 SUM(CASE WHEN lower(type) IN ('charge', 'payment') THEN amount ELSE 0 END)                   AS payment_amount
             FROM raw_vault_mvp.finance_gsheets.cash_refunds_stripe
             GROUP BY 1, 2
         ),
     carrier_data_flight_report
         AS
         (
             SELECT booking_reference,
                    airline_name,
                    supplier,
                    overall_booking_status,
                    flight_booking_status,
                    cost_in_buying_currency,
                    cost_in_gbp,
                    member_refund_type,
                    booking_system,
                    row_number AS over (
                        partition BY external_reference
                        ORDER BY
                            flight_pnr
                        ) AS rank
             FROM raw_vault_mvp.finance_gsheets.cash_refunds_airline_refund_report
         ),
     carrier_data_flight_report_pnr
         AS
         (
             SELECT booking_reference,
                    listagg(flight_pnr, '/') AS flight_pnr
             FROM raw_vault_mvp.finance_gsheets.cash_refunds_airline_refund_report
             GROUP BY 1
         ),
     manual_refunds AS
         (
             SELECT mr.full_cms_transaction_id,
                    MAX(mr.refund_timestamp)                            AS refund_timestamp,
                    LISTAGG(DISTINCT mr.email_address, ', ')            AS email_address,
                    LISTAGG(DISTINCT mr.payment_status, ', ')           AS payment_status,
                    LISTAGG(DISTINCT mr.customer_currency, ', ')        AS customer_currency,
                    SUM(mr.amount_in_customer_currency)                 AS amount_in_customer_currency,
                    LISTAGG(DISTINCT mr.bank_details_type, ', ')        AS bank_details_type,
                    LISTAGG(DISTINCT mr.product_type, ', ')             AS product_type,

                    LISTAGG(DISTINCT mr.type_of_refund, ', ')           AS type_of_refund,
                    LISTAGG(DISTINCT mr.reference_transaction_id, ', ') AS reference_transaction_id,
                    LISTAGG(DISTINCT mr.refund_speed, ', ')             AS refund_speed,
                    LISTAGG(DISTINCT mr.duplicate, ', ')                AS duplicate,
                    LISTAGG(DISTINCT mr.cb_raised, ', ')                AS cb_raised,
                    LISTAGG(DISTINCT mr.fraud_team_comment, ', ')       AS fraud_team_comment
             FROM raw_vault_mvp.finance_gsheets.manual_refunds mr
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
           --WHEN badj.adjusted_check_in_date IS NOT NULL THEN 'IHP/H+ rebooked'
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
           --WHEN bc.REFUND_CHANNEL  = 'PAYMENT_METHOD' THEN 'Refunded by Payment Method'
           WHEN bs.travel_date <= '2020-03-17' THEN 'Pre 17 March CheckIn'
           ELSE 'No Credit Issued'
           END                                   AS status_se_pkg,
       cr.credit_active,
       cr.credit_deleted,
       cr.credit_used,
       cr.credit_used_tb,
       cr.credit_refunded_cash,

       cbctl.reference                           AS cb_ctl_reference,
       cbctl.ccy                                 AS cb_ctl_ccy,
       cbctl.amount                              AS cb_ctl_amount,
       cbctl.lost_amount                         AS cb_ctl_amount_lost,
       cbctl.pending_amount                      AS cb_ctl_amount_pending,
       cbctl.won_amount                          AS cb_ctl_amount_won,

       mancb.refund_timestamp                    AS manual_bacs_refund_timestamp,
       mancb.payment_status                      AS manual_bacs_payment_status,
       mancb.customer_currency                   AS manual_bacs_customer_currency,
       mancb.amount_in_customer_currency         AS manual_bacs_amount_in_customer_currency,
       mancb.bank_details_type                   AS manual_bacs_bank_details_type,
       mancb.product_type                        AS manual_bacs_product_typefull_cms_transaction_id,
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

       str.currency                              AS stripe_currency,
       str.refunded_amount                       AS stripe_refunded_amount,
       str.payment_amount                        AS stripe_payment_amount,

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
       cr4.booking_system                        AS car_booking_system_4
       --cmap.updated AS CARRIER_MAPPING_1_UPDATED,
       --cmap.flight_carrier AS CARRIER_MAPPING_1_flight_carrier,
       --cmap.type AS CARRIER_MAPPING_1_type,
       --cmap.refund_type AS CARRIER_MAPPING_1_refund_type,
       --cmap.reported_refund_type AS CARRIER_MAPPING_1_reported_refund_type
FROM hygiene_snapshot_vault_mvp.tableau.travelbird_rebookings bs
         LEFT JOIN salesforce_data_clean sf ON bs.booking_id::VARCHAR = sf.travel_bird_booking_id
         LEFT JOIN duplicated_solved_count sfdc ON bs.booking_id::VARCHAR = sfdc.travel_bird_booking_id
         LEFT JOIN chargeback_catalogue cbctl ON cbctl.reference = bs.booking_id::VARCHAR
         LEFT JOIN (
    SELECT *
    FROM carrier_data_flight_report
    WHERE rank = 1
) cr1 ON cr1.booking_reference = bs.booking_id
         LEFT JOIN (
    SELECT *
    FROM carrier_data_flight_report
    WHERE rank = 2
) cr2 ON cr2.booking_reference = bs.booking_id
         LEFT JOIN (
    SELECT *
    FROM carrier_data_flight_report
    WHERE rank = 3
) cr3 ON cr3.booking_reference = bs.booking_id
         LEFT JOIN (
    SELECT *
    FROM carrier_data_flight_report
    WHERE rank = 4
) cr4 ON cr4.booking_reference = bs.booking_id
         LEFT JOIN (
    SELECT *
    FROM carrier_data_flight_report_pnr
) pnr ON pnr.booking_reference = bs.booking_id
         LEFT JOIN manual_refunds mancb
                   ON COALESCE(REGEXP_SUBSTR(mancb.full_cms_transaction_id, '-.*-(.*)', 1, 1, 'e'),
                               SPLIT(mancb.full_cms_transaction_id, '-')[1]) = bs.booking_id::VARCHAR
         LEFT JOIN raw_vault_mvp.finance_gsheets.cash_refunds_to_members fin ON fin.booking_id = bs.booking_id::VARCHAR
         LEFT JOIN stripe_data str ON str.order_id = bs.booking_id::VARCHAR
--LEFT JOIN raw_vault_mvp.finance_gsheets.CASH_REFUNDS_AIRLINE_REFUND_STATUS cmap ON cmap.flight_carrier = cr1.airline_name
         LEFT JOIN credit_data cr ON bs.booking_id = cr.external_id;



SELECT *
FROM collab.covid_pii.covid_master_list_catalogue cmlc;

SELECT *
FROM se.data_pii.se_booking_summary_extended sbse

SELECT *
FROM data_vault_mvp.cms_mysql_snapshots.affiliate_snapshot "AS";



CREATE OR REPLACE TABLE scratch.robinpatel.covid_master_list_catalogue AS
SELECT *
FROM collab.covid_pii.covid_master_list_catalogue;

self_describing_task --include 'dv/dwh/master_booking_list/master_tb_booking_list'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
/
USERS/
robin/
myrepos/
one-DATA-pipeline/
biapp/
task_catalogue/
dv/
dwh/
master_booking_list/
master_tb_booking_list.py

CREATE SCHEMA hygiene_snapshot_vault_mvp_dev_robin.tableau;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.tableau.travelbird_rebookings CLONE hygiene_snapshot_vault_mvp.tableau.travelbird_rebookings;

SELECT *
FROM hygiene_snapshot_vault_mvp.tableau.travelbird_rebookings bs;

SELECT *
FROM hygiene_snapshot_vault_mvp.sfsc.rebooking_requests
WHERE LEFT(booking_id, 3) = '218';

CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.sfsc.rebooking_request_cases_ho CLONE raw_vault_mvp.sfsc.rebooking_request_cases_ho;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.sfsc.rebooking_request_cases_pkg CLONE raw_vault_mvp.sfsc.rebooking_request_cases_pkg;

self_describing_task --include 'staging/hygiene/sfsc/rebooking_requests'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'staging/hygiene_snapshots/sfsc/rebooking_requests'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.sfsc.rebooking_requests
WHERE LEFT(booking_id, 3) = 'TB-';

SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.sfsc.rebooking_requests
WHERE LEFT(booking_id, 3) = '218'

SELECT *
FROM se.data.se_credit_model scm
WHERE LEFT(scm.original_booking_id, 1) = 'A';

SELECT *
FROM se.data.se_credit_model scm
WHERE external_id IS NOT NULL;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.master_tb_booking_list__model_data
WHERE master_tb_booking_list__model_data.transaction_id IS NULL;



SELECT *
FROM hygiene_snapshot_vault_mvp.tableau.travelbird_rebookings tr
WHERE LEFT(tr.booking_id, 2) = 'TB';



CREATE TRANSIENT TABLE dwh.master_tb_booking_list__model_data
(
    transaction_id                                VARCHAR,
    booking_id                                    VARCHAR,
    margin_gross_of_toms_gbp                      DOUBLE,
    airline                                       VARCHAR,
    booking_status                                VARCHAR,
    concept                                       VARCHAR,
    country                                       VARCHAR,
    booking_date                                  DATE,
    return_date                                   DATE,
    travel_date                                   DATE,
    flight_transaction_state                      VARCHAR,
    has_credit_refund_note                        BOOLEAN,
    order_cms_url                                 VARCHAR,
    paid_with_deposit                             VARCHAR,
    payment_currency                              VARCHAR,
    payment_status                                VARCHAR,
    product_line                                  VARCHAR,
    refunded_method                               VARCHAR,
    sale_id                                       VARCHAR,
    sale_type                                     VARCHAR,
    supplier                                      VARCHAR,
    territory_name                                VARCHAR,
    index                                         NUMBER,
    sell_rate_eur                                 DOUBLE,
    booking_fees_eur                              DOUBLE,
    sold_price_eur                                DOUBLE,
    cost_price_eur                                DOUBLE,
    margin_eur                                    DOUBLE,
    sold_price_in_currency                        DOUBLE,
    flight_cost_price                             DOUBLE,
    flight_sold_price                             DOUBLE,
    customer_refund_perc                          DOUBLE,
    payment_amount                                DOUBLE,
    refunded_in_currency                          DOUBLE,
    refunded_credit                               DOUBLE,
    sold_price_gbp                                DOUBLE,
    cost_price_gbp                                DOUBLE,
    margin_gbp                                    DOUBLE,
    margin_excl_toms_gbp                          DOUBLE,
    booking_fees_gbp                              DOUBLE,
    booking_id__o                                 VARCHAR,
    booking_date__o                               VARCHAR,
    return_date__o                                VARCHAR,
    travel_date__o                                VARCHAR,
    has_credit_refund_note__o                     VARCHAR,
    sell_rate_eur__o                              VARCHAR,
    booking_fees_eur__o                           VARCHAR,
    sold_price_eur__o                             VARCHAR,
    cost_price_eur__o                             VARCHAR,
    margin_eur__o                                 VARCHAR,
    sold_price_in_currency__o                     VARCHAR,
    flight_cost_price__o                          VARCHAR,
    flight_sold_price__o                          VARCHAR,
    customer_refund_perc__o                       VARCHAR,
    payment_amount__o                             VARCHAR,
    refunded_in_currency__o                       VARCHAR,
    refunded_credit__o                            VARCHAR,
    sold_price_gbp__o                             VARCHAR,
    cost_price_gbp__o                             VARCHAR,
    margin_gbp__o                                 VARCHAR,
    margin_excl_toms_gbp__o                       VARCHAR,
    booking_fees_gbp__o                           VARCHAR,
    sf_case_number                                NUMBER,
    sf_case_owner_full_name                       VARCHAR,
    sf_transaction_id                             VARCHAR,
    sf_subject                                    VARCHAR,
    sf_opportunity_sale_id                        VARCHAR,
    sf_status                                     VARCHAR,
    sf_case_origin                                VARCHAR,
    sf_view                                       VARCHAR,
    sf_booking_lookup_check_in_date               TIMESTAMP,
    sf_booking_lookup_check_out_date              TIMESTAMP,
    sf_requested_rebooking_date                   DATE,
    sf_postponed_booking_request                  BOOLEAN,
    sf_booking_lookup_store_id                    VARCHAR,
    sf_booking_lookup_supplier_territory          VARCHAR,
    sf_contact_reason                             VARCHAR,
    sf_last_modified_by_full_name                 VARCHAR,
    sf_overbooking_rebooking_stage                VARCHAR,
    sf_reason                                     VARCHAR,
    sf_case_id                                    VARCHAR,
    sf_date_time_opened                           TIMESTAMP,
    sf_case_name                                  NUMBER,
    sf_last_modified_date                         DATE,
    sf_last_modified_by_case_overview             VARCHAR,
    sf_priority_type                              VARCHAR,
    sf_covid19_member_resolution_cs               VARCHAR,
    sf_case_overview_id                           VARCHAR,
    sf_case_thread_id                             VARCHAR,
    sf_priority                                   VARCHAR,
    number_dup_cases_solved                       NUMBER,
    sf_tb_status                                  VARCHAR,
    cr_credit_active                              DOUBLE,
    cr_credit_deleted                             DOUBLE,
    cr_credit_used                                DOUBLE,
    cr_credit_used_tb                             DOUBLE,
    cr_credit_refunded_cash                       DOUBLE,
    m_bacs_refund_timestamp                       VARCHAR,
    m_bacs_payment_status                         VARCHAR,
    m_bacs_customer_currency                      VARCHAR,
    m_bacs_amount_in_customer_currency            DOUBLE,
    m_bacs_bank_details_type                      VARCHAR,
    m_bacs_product_type                           VARCHAR,
    m_bacs_type_of_refund                         VARCHAR,
    m_bacs_reference_transaction_id               VARCHAR,
    m_bacs_refund_speed                           VARCHAR,
    m_bacs_duplicate                              VARCHAR,
    m_bacs_cb_raised                              VARCHAR,
    m_bacs_fraud_team_comment                     VARCHAR,
    cb_tb_ccy                                     VARCHAR,
    cb_tb_lost_amount                             DOUBLE,
    cb_tb_won_amount                              DOUBLE,
    cb_tb_pending_amount                          DOUBLE,
    cb_tb_amount                                  DOUBLE,
    finance_include_flight                        VARCHAR,
    finance_net_amount_paid_fx                    VARCHAR,
    finance_net_amount_paid_gbp                   VARCHAR,
    finance_non_flight_spls_cash_held             VARCHAR,
    finance_non_flight_vcc_held                   VARCHAR,
    finance_flight_refunds_received_gbp           VARCHAR,
    finance_total_held_gbp                        VARCHAR,
    finance_perc_held                             VARCHAR,
    finance_flight_and_non_flight_components_held VARCHAR,
    finance_refund_made                           VARCHAR,
    finance_refund_type                           VARCHAR,
    finance_amount                                VARCHAR,
    finance_chargeback                            VARCHAR,
    finance_currency                              VARCHAR,
    finance_amount_inc_margin_adj                 VARCHAR,
    cr_flight_pnr                                 VARCHAR,
    cr_total_flights                              NUMBER,
    car1_airline_name                             VARCHAR,
    car1_supplier                                 VARCHAR,
    car1_overall_booking_status                   VARCHAR,
    car1_flight_booking_status                    VARCHAR,
    car1_cost_in_buying_currency                  DOUBLE,
    car1_cost_in_gbp                              DOUBLE,
    car1_member_refund_type                       VARCHAR,
    car1_booking_system                           VARCHAR,
    car1_mapping_updated                          DATE,
    car1_mapping_flight_carrier                   VARCHAR,
    car1_mapping_type                             VARCHAR,
    car1_mapping_refund_type                      VARCHAR,
    car1_mapping_reported_refund_type             VARCHAR,
    car2_airline_name                             VARCHAR,
    car2_supplier                                 VARCHAR,
    car2_overall_booking_status                   VARCHAR,
    car2_flight_booking_status                    VARCHAR,
    car2_cost_in_buying_currency                  DOUBLE,
    car2_cost_in_gbp                              DOUBLE,
    car2_member_refund_type                       VARCHAR,
    car2_booking_system                           VARCHAR,
    car2_mapping_updated                          DATE,
    car2_mapping_flight_carrier                   VARCHAR,
    car2_mapping_type                             VARCHAR,
    car2_mapping_refund_type                      VARCHAR,
    car2_mapping_reported_refund_type             VARCHAR,
    car3_airline_name                             VARCHAR,
    car3_supplier                                 VARCHAR,
    car3_overall_booking_status                   VARCHAR,
    car3_flight_booking_status                    VARCHAR,
    car3_cost_in_buying_currency                  DOUBLE,
    car3_cost_in_gbp                              DOUBLE,
    car3_member_refund_type                       VARCHAR,
    car3_booking_system                           VARCHAR,
    car3_mapping_updated                          DATE,
    car3_mapping_flight_carrier                   VARCHAR,
    car3_mapping_type                             VARCHAR,
    car3_mapping_refund_type                      VARCHAR,
    car3_mapping_reported_refund_type             VARCHAR,
    car4_airline_name                             VARCHAR,
    car4_supplier                                 VARCHAR,
    car4_overall_booking_status                   VARCHAR,
    car4_flight_booking_status                    VARCHAR,
    car4_cost_in_buying_currency                  DOUBLE,
    car4_cost_in_gbp                              DOUBLE,
    car4_member_refund_type                       VARCHAR,
    car4_booking_system                           VARCHAR,
    car4_mapping_updated                          DATE,
    car4_mapping_flight_carrier                   VARCHAR,
    car4_mapping_type                             VARCHAR,
    car4_mapping_refund_type                      VARCHAR,
    car4_mapping_reported_refund_type             VARCHAR,
    stripe_currency                               VARCHAR,
    stripe_refunded_amount                        DOUBLE,
    stripe_payment_amount                         DOUBLE
);

CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.tableau.travelbird_rebookings CLONE hygiene_snapshot_vault_mvp.tableau.travelbird_rebookings;

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.tb_booking CLONE data_vault_mvp.dwh.tb_booking;

--dupes in masterlist
SELECT *
FROM data_vault_mvp_dev_robin.dwh.master_tb_booking_list
WHERE master_tb_booking_list.booking_id IN (
    SELECT mtbl.booqking_id
    FROM data_vault_mvp_dev_robin.dwh.master_tb_booking_list mtbl
    GROUP BY 1
    HAVING count(*) > 1
);


SELECT *
FROM hygiene_snapshot_vault_mvp.finance_gsheets.cash_refunds_to_members crtm
WHERE booking_id IN (
    SELECT crtm.booking_id
    FROM hygiene_snapshot_vault_mvp.finance_gsheets.cash_refunds_to_members crtm
    GROUP BY 1
    HAVING count(*) > 1
)

SELECT *
FROM data_vault_mvp_dev_robin.dwh.master_tb_booking_list__cash_refunds_to_members_data;

CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.finance_gsheets.cash_refunds_airline_refund_report CLONE hygiene_snapshot_vault_mvp.finance_gsheets.cash_refunds_airline_refund_report

SELECT *
FROM hygiene_vault_mvp.finance_gsheets.cash_refunds_airline_refund_report crarr
WHERE crarr.booking_id IN (
    SELECT mtbl.booking_id
    FROM data_vault_mvp_dev_robin.dwh.master_tb_booking_list mtbl
    GROUP BY 1
    HAVING count(*) > 1
);

SELECT *
FROM hygiene_vault_mvp.finance_gsheets.cash_refunds_airline_refund_report crarr;

CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.finance_gsheets.cash_refunds_airline_refund_report CLONE raw_vault_mvp.finance_gsheets.cash_refunds_airline_refund_report;

self_describing_task --include 'staging/hygiene/finance_gsheets/cash_refunds_airline_refund_report'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

self_describing_task --include 'staging/hygiene_snapshots/finance_gsheets/cash_refunds_airline_refund_report'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT count(*)
FROM collab.covid_pii.covid_master_list_catalogue cmlc;
SELECT count(*)
FROM data_vault_mvp_dev_robin.dwh.master_tb_booking_list;


SELECT *
FROM collab.covid_pii.covid_master_list_catalogue cmlc
WHERE cmlc.booking_id IN (
    SELECT cmlc.booking_id
--        count(*)
    FROM collab.covid_pii.covid_master_list_catalogue cmlc
    GROUP BY 1
    HAVING count(*) > 1
)
;
