SELECT transactionid,
       customertotalprice,
       active_credit,
       bk_cnx_refund_channel,
       rate_pay_amount_gross,
       world_pay_amount,
       manual_bacs_amount_in_customer_currency,
       cb_se_status,
       cb_se_payment_amount
FROM collab.covid_pii.covid_master_list_all
WHERE checkin >= '2020-03-18'
  AND checkin <= '2020-12-31'
  AND (
        (LOWER(saledimension) LIKE ('ihp%')
            AND
         LOWER(supplier) LIKE ('secret escapes%')
            )
        OR
        (LOWER(saledimension) = 'hotel'
            AND LOWER(dynamicflightbooked) = 'y')
    )
  AND (rate_pay_amount_gross <> 0 OR world_pay_amount <> 0 OR manual_bacs_amount_in_customer_currency <> 0 OR
       (cb_se_payment_amount <> 0 AND lower(cb_se_status) = 'lost'));

DROP VIEW collab.covid_pii.covid_master_list_all_test;

GRANT SELECT ON VIEW collab.covid_pii.covid_master_list_all TO ROLE personal_role__gianniraftis;
GRANT SELECT ON VIEW collab.covid_pii.covid_master_list_all TO ROLE personal_role__saurdash;
GRANT SELECT ON VIEW collab.covid_pii.covid_master_list_all TO ROLE personal_role__tommariani;
GRANT SELECT ON VIEW collab.covid_pii.covid_master_list_all TO ROLE personal_role__mariyatodorova;
GRANT SELECT ON VIEW collab.covid_pii.covid_master_list_all TO ROLE personal_role__ashmitabhimji;
GRANT SELECT ON VIEW collab.covid_pii.covid_master_list_all TO ROLE personal_role__kirstengrieve;
GRANT SELECT ON VIEW collab.covid_pii.covid_master_list_all TO ROLE personal_role__shiannestannard;
GRANT SELECT ON VIEW collab.covid_pii.covid_master_list_all TO ROLE personal_role__morganafoti;
GRANT SELECT ON VIEW collab.covid_pii.covid_master_list_all TO ROLE personal_role__janhitzke;
GRANT SELECT ON VIEW collab.covid_pii.covid_master_list_all TO ROLE personal_role__johannalantigua;
GRANT SELECT ON VIEW collab.covid_pii.covid_master_list_all TO ROLE personal_role__warsanabdullahi;
GRANT SELECT ON VIEW collab.covid_pii.covid_master_list_all TO ROLE personal_role__gianniraftis;
GRANT SELECT ON VIEW collab.covid_pii.covid_master_list_all TO ROLE personal_role__jadevallance;
GRANT SELECT ON VIEW collab.covid_pii.covid_master_list_all TO ROLE personal_role__barbarafrascoli;
GRANT SELECT ON VIEW collab.covid_pii.covid_master_list_all TO ROLE personal_role__radujosan;
GRANT SELECT ON VIEW collab.covid_pii.covid_master_list_all TO ROLE personal_role__carmenmardiros;
GRANT SELECT ON VIEW collab.covid_pii.covid_master_list_all TO ROLE personal_role__samanthamandeldallal;
GRANT SELECT ON VIEW collab.covid_pii.covid_master_list_all TO ROLE personal_role__martalagut;
GRANT SELECT ON VIEW collab.covid_pii.covid_master_list_all TO ROLE personal_role__niroshanbalakumar;
GRANT SELECT ON VIEW collab.covid_pii.covid_master_list_all TO ROLE personal_role__tanithspinelli;
GRANT SELECT ON VIEW collab.covid_pii.covid_master_list_all TO ROLE personal_role__alizaidi;
GRANT SELECT ON VIEW collab.covid_pii.covid_master_list_all TO ROLE personal_role__konstantineberle;
GRANT SELECT ON VIEW collab.covid_pii.covid_master_list_all TO ROLE personal_role__robinpatel;



CREATE OR REPLACE VIEW collab.covid_pii.covid_master_list_all
    COPY GRANTS
AS
WITH se_masterlist
         AS
         (
             SELECT 'SE'                                                                           AS platform,
                    transactionid,
                    booking_id,
                    saleid,
                    salename,
                    datebooked,
                    checkin,
                    checkout,
                    currency,
                    territory,
                    customertotalprice,
                    customerpayment,
                    grossbookingvalue,
                    finance_net_amount_paid_fx,
                    finance_net_amount_paid_gbp,
                    finance_non_flight_spls_cash_held,
                    finance_non_flight_vcc_held,
                    finance_flight_refunds_received_gbp,
                    finance_total_held_gbp,
                    finance_perc_held,
                    finance_flight_and_non_flight_components_held,
                    finance_amount_inc_margin_adj,
                    type,
                    supplier,
                    country,
                    customerid,
                    saledimension,
                    dynamicflightbooked,
                    carrier,
                    grossprofit,
                    CASE WHEN refunded = TRUE OR cancelled THEN 'CANX' ELSE 'LIVE' END             AS booking_status,
                    world_pay_currency,
                    COALESCE(world_pay_amount, 0)                                                  AS world_pay_refunded,
                    rate_pay_currency,
                    COALESCE(rate_pay_amount_gross, 0)                                             AS rate_pay_refunded,
                    stripe_currency,
                    COALESCE(stripe_amount, 0)                                                     AS stripe_refunded,
                    CASE WHEN lower(cb_se_status) = 'lost' THEN cb_se_payment_amount ELSE 0 END    AS chargeback_amount_lost,
                    CASE WHEN lower(cb_se_status) = 'won' THEN cb_se_payment_amount ELSE 0 END     AS chargeback_amount_won,
                    CASE WHEN lower(cb_se_status) = 'pending' THEN cb_se_payment_amount ELSE 0 END AS chargeback_amount_pending,
                    manual_bacs_customer_currency,
                    COALESCE(manual_bacs_amount_in_customer_currency, 0)                           AS manual_bacs_refund,
                    (world_pay_refunded + rate_pay_refunded + stripe_refunded + manual_bacs_refund
                        + chargeback_amount_lost
                        )                                                                          AS total_refunded_cash,
                    coalesce(credit_active, 0)                                                     AS active_credit,
                    coalesce(credit_deleted, 0)                                                    AS deleted_credit,
                    coalesce(credit_used, 0)                                                       AS used_credit,
                    coalesce(credit_used_tb, 0)                                                    AS used_tb_credit,
                    coalesce(credit_refunded_cash, 0)                                              AS refunded_cash_credit,
                    total_refunded_cash + active_credit                                            AS total_cash_and_active_credit,
                    case_number,
                    case_owner_full_name,
                    transaction_id,
                    sf_unique_transaction_id,
                    subject,
                    opportunity_sale_id,
                    status,
                    number_dup_cases_solved,
                    case_origin,
                    "VIEW",
                    booking_lookup_check_in_date,
                    booking_lookup_check_out_date,
                    requested_rebooking_date,
                    postponed_booking_request,
                    booking_lookup_store_id,
                    booking_lookup_supplier_territory,
                    contact_reason,
                    last_modified_by_full_name,
                    overbooking_rebooking_stage,
                    reason,
                    case_id,
                    date_time_opened,
                    case_name,
                    last_modified_date,
                    last_modified_by_case_overview,
                    priority_type,
                    covid19_member_resolution_cs,
                    case_overview_id,
                    case_thread_id,
                    priority,
                    status_se_ho,
                    status_se_pkg,
                    bk_cnx_refund_channel,
                    rate_pay_amount_gross,
                    world_pay_amount,
                    manual_bacs_amount_in_customer_currency,
                    cb_se_status,
                    cb_se_payment_amount
             FROM collab.covid_pii.covid_master_list_ho_packages
         ),
     ctl_master_list
         AS (
         SELECT 'TB'                                                 AS platform,
                booking_id::VARCHAR                                  AS transactionid,
                booking_id,
                sale_id,
                'n/a'                                                AS salename,
                booking_date,
                travel_date                                          AS checkin,
                return_date                                          AS checkout,
                payment_currency,
                territory_name,
                sold_price_in_currency                               AS customertotalprice,
                payment_amount,
                sold_price_in_currency,
                finance_net_amount_paid_fx,
                finance_net_amount_paid_gbp,
                finance_non_flight_spls_cash_held,
                finance_non_flight_vcc_held,
                finance_flight_refunds_received_gbp,
                finance_total_held_gbp,
                finance_perc_held,
                finance_flight_and_non_flight_components_held,
                finance_amount_inc_margin_adj,
                product_line,
                supplier,
                country,
                'n/a'                                                AS customerid,
                sale_type                                            AS saledimension,
                CASE
                    WHEN lower(flight_transaction_state) IN ('complete', 'manual') THEN 'y'
                    ELSE 'n' END                                     AS dynamicflightbooked,
                airline,
                '0'                                                  AS grossprofit,
                booking_status,
                'n/a'                                                AS world_pay_currency,
                0                                                    AS world_pay_refunded,
                'n/a'                                                AS rate_pay_currency,
                0                                                    AS rate_pay_refunded,
                stripe_currency,
                COALESCE(stripe_refunded_amount, 0)                  AS stripe_refunded,
                COALESCE(cb_ctl_amount_lost, 0)                      AS chargeback_amount_lost,
                COALESCE(cb_ctl_amount_won, 0)                       AS chargeback_amount_won,
                COALESCE(cb_ctl_amount_pending, 0)                   AS chargeback_amount_pending,
                manual_bacs_customer_currency,
                COALESCE(manual_bacs_amount_in_customer_currency, 0) AS manual_bacs_refund,
                (world_pay_refunded + rate_pay_refunded + stripe_refunded + manual_bacs_refund
                    + chargeback_amount_lost
                    )                                                AS total_refunded_cash,
                coalesce(credit_active, 0)                           AS active_credit,
                coalesce(credit_deleted, 0)                          AS deleted_credit,
                coalesce(credit_used, 0)                             AS used_credit,
                coalesce(credit_used_tb, 0)                          AS used_tb_credit,
                coalesce(credit_refunded_cash, 0)                    AS refunded_cash_credit,
                total_refunded_cash + active_credit                  AS total_cash_and_active_credit,
                case_number,
                case_owner_full_name,
                transaction_id,
                sf_unique_transaction_id,
                subject,
                opportunity_sale_id,
                status,
                number_dup_cases_solved,
                case_origin,
                "VIEW",
                booking_lookup_check_in_date,
                booking_lookup_check_out_date,
                requested_rebooking_date,
                postponed_booking_request,
                booking_lookup_store_id,
                booking_lookup_supplier_territory,
                contact_reason,
                last_modified_by_full_name,
                overbooking_rebooking_stage,
                reason,
                case_id,
                date_time_opened,
                case_name,
                last_modified_date,
                last_modified_by_case_overview,
                priority_type,
                covid19_member_resolution_cs,
                case_overview_id,
                case_thread_id,
                priority,
                ''                                                   AS status_se_ho,
                status_se_pkg,
                NULL                                                 AS bk_cnx_refund_channel,
                NULL                                                 AS rate_pay_amount_gross,
                NULL                                                 AS world_pay_amount,
                NULL                                                 AS manual_bacs_amount_in_customer_currency,
                NULL                                                 AS cb_se_status,
                NULL                                                 AS cb_se_payment_amount
         FROM collab.covid_pii.covid_master_list_catalogue
     )
SELECT *
FROM ctl_master_list
UNION
SELECT *
FROM se_masterlist;


------------------------------------------------------------------------------------------------------------------------
--master_list_all backup
CREATE OR REPLACE VIEW collab.covid_pii.covid_master_list_all
    COPY GRANTS
AS
WITH se_masterlist
         AS
         (
             SELECT 'SE'                                                                           AS platform,
                    transactionid,
                    booking_id,
                    saleid,
                    salename,
                    datebooked,
                    checkin,
                    checkout,
                    currency,
                    territory,
                    customertotalprice,
                    customerpayment,
                    grossbookingvalue,
                    finance_net_amount_paid_fx,
                    finance_net_amount_paid_gbp,
                    finance_non_flight_spls_cash_held,
                    finance_non_flight_vcc_held,
                    finance_flight_refunds_received_gbp,
                    finance_total_held_gbp,
                    finance_perc_held,
                    finance_flight_and_non_flight_components_held,
                    finance_amount_inc_margin_adj,
                    type,
                    supplier,
                    country,
                    customerid,
                    saledimension,
                    dynamicflightbooked,
                    carrier,
                    grossprofit,
                    CASE WHEN refunded = TRUE OR cancelled THEN 'CANX' ELSE 'LIVE' END             AS booking_status,
                    world_pay_currency,
                    COALESCE(world_pay_amount, 0)                                                  AS world_pay_refunded,
                    rate_pay_currency,
                    COALESCE(rate_pay_amount_gross, 0)                                             AS rate_pay_refunded,
                    stripe_currency,
                    COALESCE(stripe_amount, 0)                                                     AS stripe_refunded,
                    CASE WHEN lower(cb_se_status) = 'lost' THEN cb_se_payment_amount ELSE 0 END    AS chargeback_amount_lost,
                    CASE WHEN lower(cb_se_status) = 'won' THEN cb_se_payment_amount ELSE 0 END     AS chargeback_amount_won,
                    CASE WHEN lower(cb_se_status) = 'pending' THEN cb_se_payment_amount ELSE 0 END AS chargeback_amount_pending,
                    manual_bacs_customer_currency,
                    COALESCE(manual_bacs_amount_in_customer_currency, 0)                           AS manual_bacs_refund,
                    (world_pay_refunded + rate_pay_refunded + stripe_refunded + manual_bacs_refund
                        + chargeback_amount_lost
                        )                                                                          AS total_refunded_cash,
                    coalesce(credit_active, 0)                                                     AS active_credit,
                    coalesce(credit_deleted, 0)                                                    AS deleted_credit,
                    coalesce(credit_used, 0)                                                       AS used_credit,
                    coalesce(credit_used_tb, 0)                                                    AS used_tb_credit,
                    coalesce(credit_refunded_cash, 0)                                              AS refunded_cash_credit,
                    total_refunded_cash + active_credit                                            AS total_cash_and_active_credit,
                    case_number,
                    case_owner_full_name,
                    transaction_id,
                    sf_unique_transaction_id,
                    subject,
                    opportunity_sale_id,
                    status,
                    number_dup_cases_solved,
                    case_origin,
                    "VIEW",
                    booking_lookup_check_in_date,
                    booking_lookup_check_out_date,
                    requested_rebooking_date,
                    postponed_booking_request,
                    booking_lookup_store_id,
                    booking_lookup_supplier_territory,
                    contact_reason,
                    last_modified_by_full_name,
                    overbooking_rebooking_stage,
                    reason,
                    case_id,
                    date_time_opened,
                    case_name,
                    last_modified_date,
                    last_modified_by_case_overview,
                    priority_type,
                    covid19_member_resolution_cs,
                    case_overview_id,
                    case_thread_id,
                    priority,
                    status_se_ho,
                    status_se_pkg
             FROM collab.covid_pii.covid_master_list_ho_packages
         ),
     ctl_master_list
         AS (
         SELECT 'TB'                                                 AS platform,
                booking_id::VARCHAR                                  AS transactionid,
                booking_id,
                sale_id,
                'n/a'                                                AS salename,
                booking_date,
                travel_date,
                return_date,
                payment_currency,
                territory_name,
                sold_price_in_currency                               AS customertotalprice,
                payment_amount,
                sold_price_in_currency,
                finance_net_amount_paid_fx,
                finance_net_amount_paid_gbp,
                finance_non_flight_spls_cash_held,
                finance_non_flight_vcc_held,
                finance_flight_refunds_received_gbp,
                finance_total_held_gbp,
                finance_perc_held,
                finance_flight_and_non_flight_components_held,
                finance_amount_inc_margin_adj,
                product_line,
                supplier,
                country,
                'n/a'                                                AS customerid,
                sale_type,
                CASE
                    WHEN lower(flight_transaction_state) IN ('complete', 'manual') THEN 'y'
                    ELSE 'n' END                                     AS dynamicflightbooked,
                airline,
                '0'                                                  AS grossprofit,
                booking_status,
                'n/a'                                                AS world_pay_currency,
                0                                                    AS world_pay_refunded,
                'n/a'                                                AS rate_pay_currency,
                0                                                    AS rate_pay_refunded,
                stripe_currency,
                COALESCE(stripe_refunded_amount, 0)                  AS stripe_refunded,
                COALESCE(cb_ctl_amount_lost, 0)                      AS chargeback_amount_lost,
                COALESCE(cb_ctl_amount_won, 0)                       AS chargeback_amount_won,
                COALESCE(cb_ctl_amount_pending, 0)                   AS chargeback_amount_pending,
                manual_bacs_customer_currency,
                COALESCE(manual_bacs_amount_in_customer_currency, 0) AS manual_bacs_refund,
                (world_pay_refunded + rate_pay_refunded + stripe_refunded + manual_bacs_refund
                    + chargeback_amount_lost
                    )                                                AS total_refunded_cash,
                coalesce(credit_active, 0)                           AS active_credit,
                coalesce(credit_deleted, 0)                          AS deleted_credit,
                coalesce(credit_used, 0)                             AS used_credit,
                coalesce(credit_used_tb, 0)                          AS used_tb_credit,
                coalesce(credit_refunded_cash, 0)                    AS refunded_cash_credit,
                total_refunded_cash + active_credit                  AS total_cash_and_active_credit,
                case_number,
                case_owner_full_name,
                transaction_id,
                sf_unique_transaction_id,
                subject,
                opportunity_sale_id,
                status,
                number_dup_cases_solved,
                case_origin,
                "VIEW",
                booking_lookup_check_in_date,
                booking_lookup_check_out_date,
                requested_rebooking_date,
                postponed_booking_request,
                booking_lookup_store_id,
                booking_lookup_supplier_territory,
                contact_reason,
                last_modified_by_full_name,
                overbooking_rebooking_stage,
                reason,
                case_id,
                date_time_opened,
                case_name,
                last_modified_date,
                last_modified_by_case_overview,
                priority_type,
                covid19_member_resolution_cs,
                case_overview_id,
                case_thread_id,
                priority,
                ''                                                   AS status_se_ho,
                status_se_pkg
         FROM collab.covid_pii.covid_master_list_catalogue
     )
SELECT *
FROM ctl_master_list
UNION
SELECT *
FROM se_masterlist;


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
                case_number::INT       AS case_number,
                case_origin,
                case_owner_full_name,
                contact_reason,
                opportunity_sale_id,
                LOWER(status)          AS status,
                CASE
                    WHEN LOWER(status) = 'hold' THEN 1
                    WHEN LOWER(status) = 'pending' THEN 1
                    WHEN LOWER(status) = 'open' THEN 1
                    WHEN LOWER(status) = 'new' THEN 2
                    WHEN LOWER(status) = 'solved' THEN 3
                    WHEN LOWER(status) = 'closed' THEN 4
                    ELSE 99
                    END                AS status_rank,
                LOWER(subject)         AS subject,
                CASE
                    WHEN LOWER(subject) LIKE '%amend%' AND requested_rebooking_date IS NOT NULL
                        THEN 'Member asked for rebooking with date'
                    WHEN LOWER(subject) LIKE '%amend%' AND requested_rebooking_date IS NULL
                        THEN 'Member asked for rebooking without date'
                    WHEN LOWER(subject) LIKE '%refund%' THEN 'Member asked for refund'
                    WHEN LOWER(subject) LIKE '%storn%' THEN 'Member asked for refund'
                    WHEN LOWER(subject) LIKE '%cxl%' THEN 'Member asked for refund'
                    WHEN LOWER(subject) LIKE '%cancel%' THEN 'Member asked for refund'
                    ELSE 'Unknown' END AS status_se,
        VIEW,
        TRY_CAST(postponed_booking_request AS BOOLEAN) AS postponed_booking_request,
        requested_rebooking_date,
        LOWER(last_modified_by_full_name) AS last_modified_by_full_name,
        LOWER(overbooking_rebooking_stage) AS overbooking_rebooking_stage,
        LOWER(reason) AS reason,
        case_id,
        date_time_opened,
        case_name::INT AS case_name,
        last_modified_date,
        LAST_MODIFIED_BY_CASE_OVERVIEW,
        PRIORITY_TYPE,
        covid19_member_resolution_cs,
        case_overview_id,
        case_thread_id,
        priority,
    -- CS sometimes put booking id in transaction id field
        COALESCE(transaction_id, booking_id) AS unique_transaction_id,
    -- hygiene flags
        CASE
        WHEN unique_transaction_id IS NULL
        THEN 1
        END AS fails_validation__unique_transaction_id__expected_nonnull,
        CASE
        WHEN fails_validation__unique_transaction_id__expected_nonnull = 1
        THEN 1
        END AS failed_some_validation,
        ROW_NUMBER() OVER (
        PARTITION BY unique_transaction_id
        ORDER BY
        status_rank ASC,
        case_number DESC,
        case_name DESC
        ) AS RANK
        FROM COMBINE_SALES_FORCE
        WHERE
        lower(last_modified_by_full_name) NOT IN ('dylan hone','kate donaghy','jessica ho')
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
        SALESFORCE_DATA_CLEAN AS (
        SELECT
        *
        FROM step01__filter_internal
        WHERE RANK = 1
        ),
        se_booking_adj AS (
        SELECT
    -- a booking can have multiple adjustments but we needs the dates associated with
    -- the most recent one
    -- TODO: if we need this elsewhere too we should define it in a `dv` module
    -- or change `cms_mysql_snapshot_bulk_wave2.booking_adjustment` to deduplicate on `booking_id` instead of `id`
        booking_id,
        check_in_date::DATE AS adjusted_check_in_date,
        check_out_date::DATE AS adjusted_check_out_date,
        stay_by_date AS voucher_stay_by_date
        FROM DATA_VAULT_MVP.CMS_MYSQL_SNAPSHOTS.BOOKING_ADJUSTMENT_SNAPSHOT
        QUALIFY ROW_NUMBER() OVER (PARTITION BY booking_id ORDER BY date_created DESC) = 1
        ),
        credit_data AS (
        SELECT original_se_booking_id,
        SUM(CASE WHEN upper(credit_status) = 'ACTIVE' THEN credit_amount ELSE 0 END) AS credit_active,
        SUM(CASE WHEN upper(credit_status) = 'DELETED' THEN credit_amount ELSE 0 END) AS credit_deleted,
        SUM(CASE WHEN upper(credit_status) = 'USED' THEN credit_amount ELSE 0 END) AS credit_used,
        SUM(CASE WHEN upper(credit_status) = 'USED_TB' THEN credit_amount ELSE 0 END) AS credit_used_tb,
        SUM(CASE WHEN upper(credit_status) = 'REFUNDED_CASH' THEN credit_amount ELSE 0 END) AS credit_refunded_cash,
        SUM(credit_amount) AS credit_amount_all
        FROM se.data.se_credit_model
        GROUP BY
        1
        ),
    --temporary fix to dedup so i can validate the rest of the SQL is not duplicating, I have asked Mike in tech support, and will replace witht the fixed version
        BOOKING_CANCELLATION_TEMP_FIX AS
        (
        SELECT * FROM DATA_VAULT_MVP.CMS_MYSQL_SNAPSHOTS.BOOKING_CANCELLATION_SNAPSHOT
        QUALIFY ROW_NUMBER() OVER (PARTITION BY COALESCE(BOOKING_ID,reservation_id) ORDER BY last_updated DESC) = 1
        ),
        WORLD_PAY_BY_ORDER_CODE_CUR
        AS
        (
        SELECT order_code,
        currency_code,
        MIN (event_date::DATE) AS MIN_EVENT_DATE,
        MAX(event_date::DATE) AS MAX_EVENT_DATE,
        COUNT(event_date) AS number_events,
        sum(CASE WHEN amount <0 THEN amount * -1 ELSE amount END) AS amount
        FROM
        raw_vault_mvp.worldpay.transaction_summary
        WHERE
        lower(STATUS) IN ('refunded','refunded_by_merchant')
        GROUP BY 1,2
        ),
        RATEPAY_BY_SHOPSORDER_CUR
        AS
        (
        SELECT shopsorder_id,
        currency,
        sum(amount_gross) AS amount_gross,
        sum(disagio_gross) AS disagio_gross,
        sum(transactionfee_gross) AS transactionfee_gross,
        sum(paymentchangefee_gross) AS paymentchangefee_gross
        FROM raw_vault_mvp.ratepay.clearing
        WHERE
        lower(entry_type) IN ('5','6','return','credit')
        GROUP BY 1,2
        ),
        chargeback_catalogue AS
        (
        SELECT SPLIT(reference, '-')[1] AS reference,
        ccy,
        SUM(CASE WHEN lower(RESULT) = 'lost' THEN amount ELSE 0 END) AS LOST_AMOUNT,
        SUM(CASE WHEN lower(RESULT) = 'won' THEN amount ELSE 0 END) AS WON_AMOUNT,
        SUM(CASE WHEN lower(RESULT) = 'pending' THEN amount ELSE 0 END) AS PENDING_AMOUNT,
        sum(amount) AS amount
        FROM RAW_VAULT_MVP.FINANCE_GSHEETS.CHARGEBACKS_CATALOGUE
        GROUP BY 1,2
        ),
        carrier_data_flight_report
        AS
        (SELECT external_reference,
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
        carrier_data_flight_report_PNR
        AS
        (SELECT external_reference,
        listagg(flight_pnr,'/') AS flight_pnr
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
                case_number::INT       AS case_number,
                case_origin,
                case_owner_full_name,
                contact_reason,
                opportunity_sale_id,
                LOWER(status)          AS status,
                CASE
                    WHEN LOWER(status) = 'hold' THEN 1
                    WHEN LOWER(status) = 'pending' THEN 1
                    WHEN LOWER(status) = 'open' THEN 1
                    WHEN LOWER(status) = 'new' THEN 2
                    WHEN LOWER(status) = 'solved' THEN 3
                    WHEN LOWER(status) = 'closed' THEN 4
                    ELSE 99
                    END                AS status_rank,
                LOWER(subject)         AS subject,
                CASE
                    WHEN LOWER(subject) LIKE '%amend%' AND requested_rebooking_date IS NOT NULL
                        THEN 'Member asked for rebooking with date'
                    WHEN LOWER(subject) LIKE '%amend%' AND requested_rebooking_date IS NULL
                        THEN 'Member asked for rebooking without date'
                    WHEN LOWER(subject) LIKE '%refund%' THEN 'Member asked for refund'
                    WHEN LOWER(subject) LIKE '%storn%' THEN 'Member asked for refund'
                    WHEN LOWER(subject) LIKE '%cxl%' THEN 'Member asked for refund'
                    WHEN LOWER(subject) LIKE '%cancel%' THEN 'Member asked for refund'
                    ELSE 'Unknown' END AS status_se,
        VIEW,
        TRY_CAST(postponed_booking_request AS BOOLEAN) AS postponed_booking_request,
        requested_rebooking_date,
        LOWER(last_modified_by_full_name) AS last_modified_by_full_name,
        LOWER(overbooking_rebooking_stage) AS overbooking_rebooking_stage,
        LOWER(reason) AS reason,
        case_id,
        date_time_opened,
        case_name::INT AS case_name,
        last_modified_date,
        LAST_MODIFIED_BY_CASE_OVERVIEW,
        PRIORITY_TYPE,
        covid19_member_resolution_cs,
        case_overview_id,
        case_thread_id,
        priority,
    -- CS sometimes put booking id in transaction id field
        COALESCE(transaction_id, booking_id) AS unique_transaction_id,
        REGEXP_SUBSTR(transaction_id, '-.*-(.*)', 1, 1, 'e') AS TRAVEL_BIRD_BOOKING_ID,
    -- hygiene flags
        CASE
        WHEN TRAVEL_BIRD_BOOKING_ID IS NULL
        THEN 1
        END AS fails_validation__unique_transaction_id__expected_nonnull,
        CASE
        WHEN fails_validation__unique_transaction_id__expected_nonnull = 1
        THEN 1
        END AS failed_some_validation,
        ROW_NUMBER() OVER (
        PARTITION BY TRAVEL_BIRD_BOOKING_ID
        ORDER BY
        status_rank ASC,
        case_number DESC,
        case_name DESC
        ) AS RANK
        FROM COMBINE_SALES_FORCE
        WHERE
        lower(last_modified_by_full_name) NOT IN ('dylan hone','kate donaghy','jessica ho')
        AND NOT (
        lower(last_modified_by_full_name) = 'marta lagut'
        AND case_name IS NULL
        AND lower(status) = 'solved'
        )
        AND lower(status) != 'closed'
        ),
        duplicated_solved_count AS
        (
        SELECT
        TRAVEL_BIRD_BOOKING_ID,
        SUM(CASE WHEN rank > 1 THEN 1 ELSE 0 END) AS number_dup_cases_solved
        FROM step01__filter_internal
        WHERE lower(status) = 'solved'
        GROUP BY 1
        ),
        SALESFORCE_DATA_CLEAN AS (
        SELECT
        *
        FROM step01__filter_internal
        WHERE RANK = 1
        ),
        credit_data AS (
        SELECT
        cm.original_external_id,
        eb.external_id,
        eb.reference_id,
        SUM(CASE WHEN upper(credit_status) = 'ACTIVE' THEN credit_amount ELSE 0 END) AS credit_active,
        SUM(CASE WHEN upper(credit_status) = 'DELETED' THEN credit_amount ELSE 0 END) AS credit_deleted,
        SUM(CASE WHEN upper(credit_status) = 'USED' THEN credit_amount ELSE 0 END) AS credit_used,
        SUM(CASE WHEN upper(credit_status) = 'USED_TB' THEN credit_amount ELSE 0 END) AS credit_used_tb,
        SUM(CASE WHEN upper(credit_status) = 'REFUNDED_CASH' THEN credit_amount ELSE 0 END) AS credit_refunded_cash,
        SUM(credit_amount) AS credit_amount_all
        FROM se.data.se_credit_model cm
        LEFT JOIN data_vault_mvp.cms_mysql_snapshots.external_booking_snapshot eb ON eb.ID = cm.original_external_id
        GROUP BY 1,2,3
        ),
        chargeback_catalogue AS
        (
        SELECT SPLIT(reference, '-')[1] AS reference,
        ccy,
        SUM(CASE WHEN lower(RESULT) = 'lost' THEN amount ELSE 0 END) AS LOST_AMOUNT,
        SUM(CASE WHEN lower(RESULT) = 'won' THEN amount ELSE 0 END) AS WON_AMOUNT,
        SUM(CASE WHEN lower(RESULT) = 'pending' THEN amount ELSE 0 END) AS PENDING_AMOUNT,
        sum(amount) AS amount
        FROM RAW_VAULT_MVP.FINANCE_GSHEETS.CHARGEBACKS_CATALOGUE
        GROUP BY 1,2
        ),
        stripe_data AS
        (
        SELECT
    --REGEXP_SUBSTR(description, '\\((.*)\\)', 1, 1, '', 1) AS REFERENCE_1,
    --REGEXP_SUBSTR(description, '\\(\\S+ (.*)\\)', 1, 1, '', 1) AS REFERENCE_2,
        COALESCE(order_id_metadata::VARCHAR,REGEXP_SUBSTR(description, '([0-9]+)\\)', 1, 1, '', 1)) AS ORDER_ID,
        CURRENCY,
        SUM(CASE WHEN lower(TYPE) IN ('refund','payment_refund') THEN amount ELSE 0 END) AS REFUNDED_AMOUNT,
        SUM(CASE WHEN lower(TYPE) IN ('charge','payment') THEN amount ELSE 0 END) AS PAYMENT_AMOUNT
        FROM
        raw_vault_mvp.finance_gsheets.cash_refunds_stripe
        GROUP BY
        1,2
        ),
        carrier_data_flight_report
        AS
        (SELECT booking_reference,
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
        carrier_data_flight_report_PNR
        AS
        (SELECT booking_reference,
        listagg(flight_pnr,'/') AS flight_pnr
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
         LEFT JOIN raw_vault_mvp.finance_gsheets.manual_refunds mancb
                   ON COALESCE(REGEXP_SUBSTR(mancb.full_cms_transaction_id, '-.*-(.*)', 1, 1, 'e'),
                               SPLIT(mancb.full_cms_transaction_id, '-')[1]) = bs.booking_id::VARCHAR
         LEFT JOIN raw_vault_mvp.finance_gsheets.cash_refunds_to_members fin ON fin.booking_id = bs.booking_id::VARCHAR
         LEFT JOIN stripe_data str ON str.order_id = bs.booking_id::VARCHAR
--LEFT JOIN raw_vault_mvp.finance_gsheets.CASH_REFUNDS_AIRLINE_REFUND_STATUS cmap ON cmap.flight_carrier = cr1.airline_name
         LEFT JOIN credit_data cr ON bs.booking_id = cr.external_id;


SELECT CHECK
FROM collab.covid_pii.covid_master_list_catalogue cmlc;
SELECT *
FROM collab.covid_pii.covid_master_list_all cmla cmlhp;


SET database_name = 'COLLAB';
SET schema_name = 'COVID_PII';
SET object_name = 'covid_master_list_ho_packages';


SELECT DISTINCT
       u.grantee_name AS user_name
FROM snowflake.account_usagegrants_to_users u
         INNER JOIN snowflake.account_usage.grants_to_roles r
                    ON u.role = r.grantee_name
WHERE u.granted_to = 'USER'
  AND r.table_catalog = $database_name
  AND r.table_schema = $schema_name
  AND r.name = $object_name
  AND u.deleted_on IS NULL
  AND r.deleted_on IS NULL;