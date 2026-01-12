CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.sfsc.rebooking_request_cases CLONE hygiene_vault_mvp.sfsc.rebooking_request_cases;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_credit CLONE data_vault_mvp.dwh.se_credit;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.booking_cancellation CLONE data_vault_mvp.dwh.booking_cancellation;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.worldpay.transaction_summary CLONE hygiene_snapshot_vault_mvp.worldpay.transaction_summary;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.ratepay.clearing CLONE hygiene_snapshot_vault_mvp.ratepay.clearing;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.finance_gsheets.manual_refunds CLONE hygiene_snapshot_vault_mvp.finance_gsheets.manual_refunds;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.finance_gsheets.cash_refunds_airline_refund_report CLONE hygiene_snapshot_vault_mvp.finance_gsheets.cash_refunds_airline_refund_report;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.finance_gsheets.cash_refunds_airline_refund_report CLONE hygiene_snapshot_vault_mvp.finance_gsheets.cash_refunds_airline_refund_report;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.finance_gsheets.cash_refunds_airline_refund_status CLONE hygiene_snapshot_vault_mvp.finance_gsheets.cash_refunds_airline_refund_status;


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_booking CLONE data_vault_mvp.dwh.se_booking;
CREATE
OR
REPLACE
TRANSIENT
VIEW se_dev_robin.data_pii.se_booking_summary_extended AS
SELECT *
FROM se.data.se_booking_summary_extended;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfsc.rebooking_request_cases CLONE hygiene_snapshot_vault_mvp.sfsc.rebooking_request_cases;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.finance_gsheets.chargebacks_se CLONE hygiene_snapshot_vault_mvp.finance_gsheets.chargebacks_se;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.finance_gsheets.cash_refunds_to_members CLONE hygiene_snapshot_vault_mvp.finance_gsheets.cash_refunds_to_members;

SELECT COUNT(*)
FROM data_vault_mvp.dwh.master_se_booking_list msbl;
SELECT COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.master_se_booking_list msbl;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.master_se_booking_list msbl;



SELECT MIN(msbl.date_booked)
FROM data_vault_mvp.dwh.master_se_booking_list msbl;

SELECT log_date            AS log_date,
       territory_id        AS territory_id,
       job_id              AS job_id,
       campaign_name       AS campaign_name,
       subscriber_key      AS subscriber_key,
       deal_id             AS deal_id,
       position_in_section AS position_in_section,
       COUNT(*)            AS cnt
FROM hygiene_vault_mvp.sfmc.athena_send_log
GROUP BY 1, 2, 3, 4, 5, 6, 7
HAVING cnt > 1;

USE WAREHOUSE pipe_xlarge;

SELECT *
FROM hygiene_vault_mvp.sfmc.athena_send_log asl
    QUALIFY COUNT(*) OVER (PARTITION BY asl.log_date, asl.territory_id, asl.job_id,
        asl.campaign_name, asl.subscriber_key, asl.deal_id, asl.position_in_section) > 1;