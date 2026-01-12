SELECT c.id                                                                 AS credit_id,
       c.billing_id                                                         AS billing_id,
       c.date_created                                                       AS credit_date_created,
       tlc.expires_on                                                       AS credit_expires_on,
       c.last_updated                                                       AS credit_last_updated,
       c.type                                                               AS credit_type,
       c.status                                                             AS credit_status,
       c.reason                                                             AS credit_reason,
       c.currency                                                           AS credit_currency,
       c.amount                                                             AS credit_amount,
       t.name                                                               AS credit_territory,

       su.id                                                                AS shiro_user_id,
       su.date_created                                                      AS user_join_date,

       COALESCE(c.from_refunded_booking_id::VARCHAR,
                CONCAT('A', c.from_refunded_reservation_id), tb.booking_id) AS original_booking_id,
       c.from_refunded_external_booking_id                                  AS original_external_id,
       eb.reference_id                                                      AS original_external_reference_id,

       c.voucher_id                                                         AS original_voucher_id,
       COALESCE(bc.booking_credits_used_id::VARCHAR,
                CONCAT('A', rc.reservation_credits_used_id))                AS redeemed_se_booking_id


FROM data_vault_mvp.cms_mysql_snapshots.credit_snapshot c
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.booking_credit_snapshot bc
                   ON bc.credit_id = c.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.reservation_credit_snapshot rc
                   ON rc.credit_id = c.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.billing_snapshot bi
                   ON bi.id = c.billing_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot su
                   ON su.billing_id = bi.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.affiliate_snapshot a
                   ON a.id = su.affiliate_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot t
                   ON t.id = a.territory_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.time_limited_credit_snapshot tlc
                   ON tlc.id = c.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.external_booking_snapshot eb
                   ON eb.id = c.from_refunded_external_booking_id
         LEFT JOIN data_vault_mvp.dwh.tb_booking tb ON eb.reference_id = tb.reference_id
WHERE original_external_reference_id IS NOT NULL;

SELECT *
FROM data_vault_mvp.dwh.user_attributes ua;

CREATE OR REPLACE TABLE scratch.robinpatel.credit_model AS
SELECT *
FROM se.data.se_credit_model scm;

SELECT GET_DDL('table', 'scratch.robinpatel.credit_model');

CREATE OR REPLACE TABLE credit_model
(
    credit_id                      NUMBER,
    billing_id                     NUMBER,
    credit_date_created            TIMESTAMP,
    credit_expires_on              TIMESTAMP,
    credit_last_updated            TIMESTAMP,
    credit_type                    VARCHAR,
    credit_status                  VARCHAR,
    credit_reason                  VARCHAR,
    credit_currency                VARCHAR,
    credit_amount                  FLOAT,
    credit_territory               VARCHAR,

    shiro_user_id                  NUMBER,
    user_join_date                 TIMESTAMP,

    original_se_booking_id         VARCHAR,
    original_external_id           NUMBER,
    original_external_reference_id VARCHAR,

    original_voucher_id            NUMBER,

    redeemed_se_booking_id         VARCHAR

    --redeemed_external_booking_id VARCHAR, --need to include this functionality


);


SELECT DISTINCT email_name
FROM hygiene_snapshot_vault_mvp.sfmc.jobs_list jl
WHERE LOWER(jl.email_name) LIKE '%password%';



CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.credit_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.credit_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.booking_credit_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.booking_credit_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.reservation_credit_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.reservation_credit_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.billing_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.billing_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.billing_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.billing_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.affiliate_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.affiliate_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.territory_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.territory_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.time_limited_credit_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.time_limited_credit_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.external_booking_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.external_booking_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes CLONE data_vault_mvp.dwh.user_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_booking CLONE data_vault_mvp.dwh.tb_booking;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_booking CLONE data_vault_mvp.dwh.se_booking;

self_describing_task --include 'dv/dwh/transactional/se_credit.py'  --method 'run' --start '2021-01-10 00:00:00' --end '2021-01-10 00:00:00'


SELECT sc.credit_id,
       sc.credit_date_created,
       sc.original_booking_id,
       sc.credit_status,
       sc.redeemed_se_booking_id,
       sb.booking_completed_date,
       sb.booking_status
FROM data_vault_mvp_dev_robin.dwh.se_credit sc
         LEFT JOIN se.data.se_booking sb ON sc.redeemed_se_booking_id = sb.booking_id
    QUALIFY COUNT(*) OVER (PARTITION BY credit_id) > 1;



SELECT c.id                                                  AS credit_id,
       c.billing_id                                          AS billing_id,
       c.date_created                                        AS credit_date_created,
       tlc.expires_on                                        AS credit_expires_on,
       c.last_updated                                        AS credit_last_updated,
       c.type                                                AS credit_type,
       c.status                                              AS credit_status,
       c.reason                                              AS credit_reason,
       c.currency                                            AS credit_currency,
       c.amount                                              AS credit_amount,

       ua.shiro_user_id,
       ua.signup_tstamp                                      AS user_signup_tstamp,
       ua.original_affiliate_territory                       AS user_original_territory,

       COALESCE(c.from_refunded_booking_id::VARCHAR,
                CONCAT('A', c.from_refunded_reservation_id),
                tb.booking_id)                               AS original_booking_id,
       c.from_refunded_external_booking_id                   AS original_external_id,
       eb.reference_id                                       AS original_external_reference_id,

       c.voucher_id                                          AS original_voucher_id,
       COALESCE(bc.booking_credits_used_id::VARCHAR,
                CONCAT('A', rc.reservation_credits_used_id)) AS redeemed_se_booking_id

FROM data_vault_mvp_dev_robin.cms_mysql_snapshots.credit_snapshot c
         LEFT JOIN data_vault_mvp_dev_robin.cms_mysql_snapshots.booking_credit_snapshot bc ON c.id = bc.credit_id
         LEFT JOIN data_vault_mvp_dev_robin.cms_mysql_snapshots.reservation_credit_snapshot rc ON c.id = rc.credit_id
         LEFT JOIN data_vault_mvp_dev_robin.cms_mysql_snapshots.billing_snapshot bi ON c.billing_id = bi.id
         LEFT JOIN data_vault_mvp_dev_robin.cms_mysql_snapshots.shiro_user_snapshot su ON bi.id = su.billing_id
         LEFT JOIN data_vault_mvp_dev_robin.dwh.user_attributes ua ON su.id = ua.shiro_user_id
         LEFT JOIN data_vault_mvp_dev_robin.cms_mysql_snapshots.time_limited_credit_snapshot tlc ON c.id = tlc.id
         LEFT JOIN data_vault_mvp_dev_robin.cms_mysql_snapshots.external_booking_snapshot eb
                   ON c.from_refunded_external_booking_id = eb.id
         LEFT JOIN data_vault_mvp_dev_robin.dwh.tb_booking tb ON eb.reference_id = tb.reference_id


SELECT scm.credit_id,
       redeemed_se_booking_id,
       fb.booking_status
FROM se.data.se_credit_model scm
         LEFT JOIN se.data.fact_booking fb
    QUALIFY COUNT(*) OVER (PARTITION BY scm.credit_id) > 1
ORDER BY 1;


SELECT rcs.credit_id,
       sb.booking_id
FROM data_vault_mvp.cms_mysql_snapshots.reservation_credit_snapshot rcs
         INNER JOIN data_vault_mvp.dwh.se_booking sb
                    ON 'A' || rcs.reservation_credits_used_id = sb.booking_id AND sb.booking_status = 'COMPLETE'
UNION

SELECT bcs.credit_id,
       sb.booking_id
FROM data_vault_mvp.cms_mysql_snapshots.booking_credit_snapshot bcs
         INNER JOIN data_vault_mvp.dwh.se_booking sb
                    ON bcs.booking_credits_used_id::VARCHAR = sb.booking_id AND sb.booking_status = 'COMPLETE'


SELECT *
FROM se.data.se_booking sb
WHERE sb.booking_id IN ('A1287627',
                        'A2936928'
    );

CREATE OR REPLACE TRANSIENT TABLE raw_vault_mvp_dev_robin.cms_mysql.booking_credit CLONE raw_vault_mvp.cms_mysql.booking_credit;

SELECT GET_DDL('table', 'raw_vault_mvp_dev_robin.cms_mysql.booking_credit');


CREATE OR REPLACE TRANSIENT TABLE booking_credit CLUSTER BY (TO_DATE(schedule_tstamp))
(
    dataset_name            VARCHAR(16777216) NOT NULL,
    dataset_source          VARCHAR(16777216) NOT NULL,
    schedule_interval       VARCHAR(16777216) NOT NULL,
    schedule_tstamp         TIMESTAMP_NTZ(9)  NOT NULL,
    run_tstamp              TIMESTAMP_NTZ(9)  NOT NULL,
    loaded_at               TIMESTAMP_NTZ(9)  NOT NULL,
    filename                VARCHAR(16777216) NOT NULL,
    file_row_number         NUMBER(38, 0)     NOT NULL,
    booking_credits_used_id NUMBER(38, 0),
    credit_id               NUMBER(38, 0),
    extract_metadata        VARIANT,
    PRIMARY KEY (dataset_name, dataset_source, schedule_interval, schedule_tstamp, run_tstamp, filename, file_row_number)
);
DROP TABLE hygi.cms_mysql.booking_credit;

self_describing_task --include 'hygiene/cms_mysql/reservation_credit.py'  --method 'run' --start '2020-03-24 00:00:00' --end '2020-03-24 00:00:00'
self_describing_task --include 'hygiene/cms_mysql/booking_credit.py'  --method 'run' --start '2020-03-09 00:00:00' --end '2020-03-09 00:00:00'
airflow backfill --start_date '2020-03-24 01:00:00' --end_date '2020-03-24 01:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__reservation_credit__daily_at_01h00
airflow backfill --start_date '2020-03-09 01:00:00' --end_date '2020-03-09 01:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__booking_credit__daily_at_01h00

SELECT COUNT(*)
FROM raw_vault_mvp.cms_mysql.booking_credit bc;

self_describing_task --include 'hygiene_snapshots/cms_mysql/reservation_credit.py'  --method 'run' --start '2021-01-11 00:00:00' --end '2021-01-11 00:00:00'
self_describing_task --include 'hygiene_snapshots/cms_mysql/booking_credit.py'  --method 'run' --start '2021-01-11 00:00:00' --end '2021-01-11 00:00:00'

SELECT *
FROM se.data.fact_booking fb;

SELECT *
FROM se.data_pii.se_user_attributes sua;

DROP TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.reservation_credit;

TRUNCATE hygiene_vault_mvp_dev_robin.cms_mysql.booking_credit;

SELECT MIN(loaded_at)
FROM raw_vault_mvp_dev_robin.cms_mysql.reservation_credit rc;

-- booking credit 2020-03-09 16:59:34.055284000
-- reservation credit 2020-03-24 18:13:18.276383000

SELECT rc.credit_id,
       sb.booking_id
FROM hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.reservation_credit rc
         INNER JOIN data_vault_mvp.dwh.se_booking sb
                    ON 'A' || rc.reservation_credits_used_id = sb.booking_id AND sb.booking_status IN ('COMPLETE', 'REFUNDED')
UNION

SELECT bc.credit_id,
       sb.booking_id
FROM hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking_credit bc
         INNER JOIN data_vault_mvp.dwh.se_booking sb
                    ON bc.booking_credits_used_id::VARCHAR = sb.booking_id AND sb.booking_status IN ('COMPLETE', 'REFUNDED');


SELECT sc.schedule_tstamp,
       sc.run_tstamp,
       sc.operation_id,
       sc.created_at,
       sc.updated_at,
       sc.credit_id,
       sc.billing_id,
       sc.credit_date_created,
       sc.credit_expires_on,
       sc.credit_last_updated,
       sc.credit_type,
       sc.credit_status,
       sc.credit_reason,
       sc.credit_currency,
       sc.credit_amount,
       sc.shiro_user_id,
       sc.user_signup_tstamp,
       sc.user_original_territory,
       sc.original_se_booking_id,
       sc.original_external_id,
       sc.original_external_reference_id,
       sc.original_voucher_id,
       sc.redeemed_se_booking_id
FROM data_vault_mvp_dev_robin.dwh.se_credit sc;

SELECT e.unstruct_event_com_branch_secretescapes_install_1
FROM snowplow.atomic.events e
WHERE e.unstruct_event_com_branch_secretescapes_install_1 IS NOT NULL
  AND e.etl_tstamp >= '2021-01-01'
  AND e.unstruct_event_com_branch_secretescapes_install_1:deep_linked::VARCHAR = 'true';


self_describing_task --include 'se/data/finance/se_credit.py'  --method 'run' --start '2021-01-11 00:00:00' --end '2021-01-11 00:00:00'


SELECT GET_DDL('table', 'data_vault_mvp_dev_robin.dwh.se_credit__step02_model_data');

SELECT md.credit_id,
       md.billing_id,
       md.credit_date_created,
       md.credit_expires_on,
       md.credit_last_updated,
       md.credit_type,
       md.credit_status,
       md.credit_reason,
       md.credit_currency,
       md.credit_amount,
       r.fx_rate                                                              AS cc_rate_to_gbp,
       IFF(r.fx_rate IS NULL, md.credit_amount, md.credit_amount * r.fx_rate) AS credit_amount_gbp,
       cc.multiplier                                                          AS cc_rate_to_gbp_constant_currency,
       IFF(cc.fx IS NULL, md.credit_amount, md.credit_amount * cc.multiplier) AS credit_amount_gbp_constant_currency,
       md.shiro_user_id,
       md.user_signup_tstamp,
       md.user_original_territory,
       md.original_se_booking_id,
       md.original_external_id,
       md.original_external_reference_id,
       md.original_voucher_id,
       md.redeemed_se_booking_id
FROM data_vault_mvp_dev_robin.dwh.se_credit__step02_model_data md
         LEFT JOIN data_vault_mvp.fx.rates r ON r.target_currency = 'GBP'
    AND r.fx_date = CURRENT_DATE
    AND r.source_currency = md.credit_currency
         LEFT JOIN hygiene_snapshot_vault_mvp.fpa_gsheets.constant_currency cc ON cc.currency = 'GBP'
    AND cc.start_date <= CURRENT_DATE
    AND cc.end_date >= CURRENT_DATE
    AND cc.base_currency = md.credit_currency;


SELECT *
FROM data_vault_mvp.fx.rates r;


CREATE OR REPLACE TRANSIENT TABLE se_credit__step02_model_data
(
    credit_id                      NUMBER(38, 0),
    billing_id                     NUMBER(38, 0),
    credit_date_created            TIMESTAMP_NTZ(9),
    credit_expires_on              TIMESTAMP_NTZ(9),
    credit_last_updated            TIMESTAMP_NTZ(9),
    credit_type                    VARCHAR(16777216),
    credit_status                  VARCHAR(16777216),
    credit_reason                  VARCHAR(16777216),
    credit_currency                VARCHAR(16777216),
    credit_amount                  FLOAT,
    shiro_user_id                  NUMBER(38, 0),
    user_signup_tstamp             TIMESTAMP_NTZ(9),
    user_original_territory        VARCHAR(16777216),
    original_se_booking_id         VARCHAR(16777216),
    original_external_id           NUMBER(38, 0),
    original_external_reference_id VARCHAR(16777216),
    original_voucher_id            NUMBER(38, 0),
    redeemed_se_booking_id         VARCHAR(16777216)
);


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.fx.rates CLONE data_vault_mvp.fx.rates;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.fpa_gsheets.constant_currency CLONE hygiene_snapshot_vault_mvp.fpa_gsheets.constant_currency;


--dev
SELECT credit_status,
       credit_type,
       SUM(credit_amount_gbp) AS credit_amount_gbp
FROM se_dev_robin.data.se_credit
GROUP BY 1, 2;

--prod
SELECT scm.credit_status,
       credit_type,
       SUM(IFF(scm.credit_currency= 'GBP', scm.credit_amount, scm.credit_amount * r.fx_rate)) as credit_amount_gbp
FROM se.data.se_credit_model scm
         LEFT JOIN data_vault_mvp.fx.rates r ON r.target_currency = 'GBP'
    AND r.fx_date = CURRENT_DATE
    AND r.source_currency = scm.credit_currency
GROUP BY 1, 2;

SELECT count(*) FROM se.data.se_credit;
SELECT count(*) FROM se.data.se_credit_model scm;