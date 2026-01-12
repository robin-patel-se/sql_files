SELECT *
FROM se.data.se_voucher_model svm

SELECT GET_DDL('table', 'se.data.se_voucher_model');


CREATE OR REPLACE VIEW se_voucher_model
    COPY GRANTS
AS
SELECT v.id                                                                        AS voucher_id,
       v.version                                                                   AS voucher_version,
       v.code                                                                      AS voucher_code,
       v.unique_transaction_reference                                              AS order_code,
       v.credit_id                                                                 AS credit_id,
       v.gifter_id                                                                 AS gifter_id,
       v.giftee_id                                                                 AS giftee_id,
       s.date_created                                                              AS gifter_join_date,
       su.date_created                                                             AS giftee_join_date,
       v.payment_id                                                                AS payment_id,
       v.date_created                                                              AS voucher_date_created,
       tc.expires_on                                                               AS voucher_expires_on,
       v.manual_expiry_date                                                        AS voucher_manual_expiry_date,
       v.last_updated                                                              AS voucher_last_updated,
       v.redeemed_date                                                             AS voucher_redeemed_date,
       v.type                                                                      AS voucher_type,
       v.status                                                                    AS voucher_status,
       CASE WHEN gc.delivery_address_postcode IS NOT NULL THEN TRUE ELSE FALSE END AS physical_voucher,
       t.currency                                                                  AS voucher_currency,
       t.name                                                                      AS voucher_territory,
       p.type                                                                      AS payment_type,
       p.status                                                                    AS payment_status,
       gc.delivery_charge                                                          AS delivery_charge,
       p.amount                                                                    AS payment_amount
FROM data_vault_mvp.cms_mysql_snapshots.voucher_snapshot v
         -- Get gift card information (for address check)
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.gift_card_snapshot gc ON gc.voucher_id = v.id
         LEFT JOIN (
    SELECT id, type, status, SUM(amount) AS amount
    FROM data_vault_mvp.cms_mysql_snapshots.payment_snapshot
    GROUP BY 1, 2, 3
) p ON p.id = v.payment_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.credit_snapshot c ON c.id = v.credit_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.time_limited_credit_snapshot tc ON tc.id = c.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot s ON s.id = v.gifter_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot su ON su.id = v.giftee_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.affiliate_snapshot a ON s.affiliate_id = a.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot t ON a.territory_id = t.id
;



SELECT v.id                                                                        AS voucher_id,
       v.version                                                                   AS voucher_version,
       v.code                                                                      AS voucher_code,
       v.unique_transaction_reference                                              AS order_code,
       v.credit_id                                                                 AS credit_id,
       v.gifter_id                                                                 AS gifter_id,
       v.giftee_id                                                                 AS giftee_id,
       v.payment_id                                                                AS payment_id,
       v.date_created                                                              AS voucher_date_created,
       tc.expires_on                                                               AS voucher_expires_on,
       v.manual_expiry_date                                                        AS voucher_manual_expiry_date,
       v.last_updated                                                              AS voucher_last_updated,
       v.redeemed_date                                                             AS voucher_redeemed_date,
       v.type                                                                      AS voucher_type,
       v.status                                                                    AS voucher_status,
       t.currency                                                                  AS voucher_currency,
       t.name                                                                      AS voucher_territory,
       gtr.signup_tstamp::DATE                                                     AS gifter_join_date,
       gte.signup_tstamp::DATE                                                     AS giftee_join_date,
       CASE WHEN gc.delivery_address_postcode IS NOT NULL THEN TRUE ELSE FALSE END AS physical_voucher,

       p.type                                                                      AS payment_type,
       p.status                                                                    AS payment_status,
       gc.delivery_charge                                                          AS delivery_charge,
       p.amount                                                                    AS payment_amount
FROM data_vault_mvp.cms_mysql_snapshots.voucher_snapshot v
         -- Get gift card information (for address check)
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.gift_card_snapshot gc ON gc.voucher_id = v.id
         LEFT JOIN (
    SELECT id,
           type,
           status,
           SUM(amount) AS amount
    FROM data_vault_mvp.cms_mysql_snapshots.payment_snapshot
    GROUP BY 1, 2, 3
) p ON p.id = v.payment_id
         LEFT JOIN data_vault_mvp_dev_robin.dwh.se_credit c ON c.credit_id = v.credit_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.time_limited_credit_snapshot tc ON tc.id = c.credit_id
         LEFT JOIN data_vault_mvp.dwh.user_attributes gtr ON v.gifter_id = gtr.shiro_user_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot t ON gtr.current_affiliate_territory_id = t.id
         LEFT JOIN data_vault_mvp.dwh.user_attributes gte ON v.giftee_id = gte.shiro_user_id
;

SELECT DISTINCT type
FROM data_vault_mvp.cms_mysql_snapshots.payment_snapshot

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.voucher_model AS
SELECT *
FROM se.data.se_voucher_model svm;

SELECT GET_DDL('table', 'scratch.robinpatel.voucher_model');



CREATE OR REPLACE TRANSIENT TABLE voucher_model
(
    voucher_id                 NUMBER,
    voucher_version            NUMBER,
    voucher_code               VARCHAR,
    order_code                 VARCHAR,
    credit_id                  NUMBER,
    gifter_id                  NUMBER,
    giftee_id                  NUMBER,
    gifter_join_date           TIMESTAMP,
    giftee_join_date           TIMESTAMP,
    payment_id                 NUMBER,
    voucher_date_created       TIMESTAMP,
    voucher_expires_on         TIMESTAMP,
    voucher_manual_expiry_date TIMESTAMP,
    voucher_last_updated       TIMESTAMP,
    voucher_redeemed_date      TIMESTAMP,
    voucher_type               VARCHAR,
    voucher_status             VARCHAR,
    physical_voucher           BOOLEAN,
    voucher_currency           VARCHAR,
    voucher_territory          VARCHAR,
    payment_type               VARCHAR,
    payment_status             VARCHAR,
    delivery_charge            FLOAT,
    payment_amount             FLOAT
);

SELECT *
FROM hygiene_snapshot_vault_mvp.fpa_gsheets.constant_currency cc;

SELECT sb.currency,
       sb.margin_gross_of_toms_cc,
       sb.margin_gross_of_toms_gbp_constant_currency,
       sb.margin_gross_of_toms_cc / NULLIF(sb.margin_gross_of_toms_gbp_constant_currency, 0)
FROM se.data.se_booking sb
WHERE sb.booking_status = 'COMPLETE';


SELECT *
FROM se.data.se_voucher_model svm;
SELECT *
FROM data_vault_mvp.cms_mysql_snapshots.voucher_snapshot vs;



SELECT rate_date,
       se_room_type_rooms_and_rates.room_type_id,
       room_type_name,
       lead_rate_plan_name,
       rt_no_available_rooms
FROM se.data.se_room_type_rooms_and_rates
WHERE hotel_code = '001w000001fICUg'
ORDER BY 1, 2

SELECT GET_DDL('table', 'se.data.tb_booking');


SELECT *
FROM data_vault_mvp.cms_mysql_snapshots.payment_snapshot ps;



CREATE OR REPLACE TRANSIENT TABLE raw_vault_mvp_dev_robin.cms_mysql.payment CLONE raw_vault_mvp.cms_mysql.payment;
ALTER TABLE raw_vault_mvp_dev_robin.cms_mysql.payment
    ADD COLUMN currency VARCHAR;
ALTER TABLE raw_vault_mvp_dev_robin.cms_mysql.payment
    ADD COLUMN merchant_code VARCHAR;

--1970-01-01 00:00:00 --min last updated from production table

DROP TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.payment_snapshot;

CREATE TABLE IF NOT EXISTS data_vault_mvp_dev_robin.cms_mysql_snapshots.payment_snapshot
(
    dataset_name      TEXT,
    dataset_source    TEXT,
    schedule_interval TEXT,
    schedule_tstamp   TIMESTAMP_NTZ,
    run_tstamp        TIMESTAMP_NTZ,
    loaded_at         TIMESTAMP_NTZ,
    filename          TEXT,
    file_row_number   NUMBER,
    id                NUMBER,
    version           NUMBER,
    amount            FLOAT,
    status            TEXT,
    transaction_id    TEXT,
    surcharge         FLOAT,
    type              TEXT,
    date_created      TIMESTAMP_NTZ,
    last_updated      TIMESTAMP_NTZ,
    extract_metadata  VARIANT,
    currency          TEXT,
    merchant_code     TEXT
);

MERGE INTO data_vault_mvp_dev_robin.cms_mysql_snapshots.payment_snapshot dest USING (
    SELECT id,
           dataset_name,
           dataset_source,
           schedule_interval,
           schedule_tstamp,
           run_tstamp,
           loaded_at,
           filename,
           file_row_number,
           version,
           amount,
           status,
           transaction_id,
           surcharge,
           type,
           date_created,
           last_updated,
           extract_metadata,
           currency,
           merchant_code
    FROM raw_vault_mvp_dev_robin.cms_mysql.payment
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY last_updated DESC, loaded_at DESC
            ) = 1
) src ON (src.id = dest.id)
    WHEN
        MATCHED
        AND (
                src.last_updated > dest.last_updated
                OR src.loaded_at > dest.loaded_at
            )
        THEN UPDATE
        SET dest.dataset_name = src.dataset_name,
            dest.dataset_source = src.dataset_source,
            dest.schedule_interval = src.schedule_interval,
            dest.schedule_tstamp = src.schedule_tstamp,
            dest.run_tstamp = src.run_tstamp,
            dest.loaded_at = src.loaded_at,
            dest.filename = src.filename,
            dest.file_row_number = src.file_row_number,
            dest.version = src.version,
            dest.amount = src.amount,
            dest.status = src.status,
            dest.transaction_id = src.transaction_id,
            dest.surcharge = src.surcharge,
            dest.type = src.type,
            dest.date_created = src.date_created,
            dest.last_updated = src.last_updated,
            dest.extract_metadata = src.extract_metadata,
            dest.currency = src.currency,
            dest.merchant_code = src.merchant_code
    WHEN NOT MATCHED THEN INSERT (
                                  dataset_name,
                                  dataset_source,
                                  schedule_interval,
                                  schedule_tstamp,
                                  run_tstamp,
                                  loaded_at,
                                  filename,
                                  file_row_number,
                                  id,
                                  version,
                                  amount,
                                  status,
                                  transaction_id,
                                  surcharge,
                                  type,
                                  date_created,
                                  last_updated,
                                  extract_metadata,
                                  currency,
                                  merchant_code
        ) VALUES (src.dataset_name,
                  src.dataset_source,
                  src.schedule_interval,
                  src.schedule_tstamp,
                  src.run_tstamp,
                  src.loaded_at,
                  src.filename,
                  src.file_row_number,
                  src.id,
                  src.version,
                  src.amount,
                  src.status,
                  src.transaction_id,
                  src.surcharge,
                  src.type,
                  src.date_created,
                  src.last_updated,
                  src.extract_metadata,
                  src.currency,
                  src.merchant_code);

self_describing_task --include 'dv/cms_snapshots/cms_mysql_snapshot_bulk_wave2.py'  --method 'run' --start '2021-01-18 00:00:00' --end '2021-01-18 00:00:00'

dataset_task --include 'cms_mysql.payment' --operation ProductionIngestOperation --method 'run' --upstream --start '2021-01-18 00:30:00' --end '2021-01-18 00:30:00'



self_describing_task --include '/dv/dwh/transactional/se_voucher.py'  --method 'run' --start '2021-01-18 00:00:00' --end '2021-01-18 00:00:00'

airflow backfill --start_date '2020-09-02 00:30:00' --end_date '2020-09-02 00:30:00' --task_regex '.*' incoming__cms_mysql__payment__daily_at_00h30
airflow clear --start_date '2020-09-02 00:30:00' --end_date '2020-09-02 00:30:00' --task_regex '.*' incoming__cms_mysql__payment__daily_at_00h30

2020-09-02 12:35:19

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.voucher_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.voucher_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.payment_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.payment_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.gift_card_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.gift_card_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_credit CLONE data_vault_mvp.dwh.se_credit;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.time_limited_credit_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.time_limited_credit_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes CLONE data_vault_mvp.dwh.user_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.territory_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.territory_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.fx.rates CLONE data_vault_mvp.fx.rates;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.fpa_gsheets.constant_currency CLONE hygiene_snapshot_vault_mvp.fpa_gsheets.constant_currency;


SELECT GET_DDL('table', 'data_vault_mvp_dev_robin.dwh.se_voucher__step02_model_data');


CREATE OR REPLACE TRANSIENT TABLE se_voucher__step02_model_data
(
    voucher_id,
    voucher_version,
    voucher_code,
    order_code,
    credit_id,
    gifter_id,
    giftee_id,
    payment_id,
    voucher_date_created,
    voucher_expires_on,
    voucher_manual_expiry_date,
    voucher_last_updated,
    voucher_redeemed_date,
    voucher_type,
    voucher_status,
    gifter_join_date,
    gifter_currency,
    gifter_territory,
    giftee_join_date,
    giftee_territory,
    is_physical_voucher,
    delivery_charge,
    total_payment_amount,
    payment_type,
    payment_status,
    payment_currency,
    verified_payment_amount,
    pending_payment_amount,
    failed_payment_amount,
    confirmed_payment_amount,
);

SELECT GET_DDL('table', 'data_vault_mvp_dev_robin.dwh.se_voucher__step03_gbp_financials');


CREATE OR REPLACE TRANSIENT TABLE se_voucher__step03_gbp_financials
(
    voucher_id                   NUMBER,
    voucher_version              NUMBER,
    voucher_code                 VARCHAR,
    order_code                   VARCHAR,
    credit_id                    NUMBER,
    gifter_id                    NUMBER,
    giftee_id                    NUMBER,
    payment_id                   NUMBER,
    voucher_date_created         TIMESTAMP,
    voucher_expires_on           TIMESTAMP,
    voucher_manual_expiry_date   TIMESTAMP,
    voucher_last_updated         TIMESTAMP,
    voucher_redeemed_date        TIMESTAMP,
    voucher_type                 VARCHAR,
    voucher_status               VARCHAR,
    voucher_currency             VARCHAR,
    gifter_join_date             DATE,
    gifter_currency              VARCHAR,
    gifter_territory             VARCHAR,
    giftee_join_date             DATE,
    giftee_territory             VARCHAR,
    is_physical_voucher          BOOLEAN,
    delivery_charge              FLOAT,
    payment_type                 VARCHAR,
    payment_status               VARCHAR,
    total_payment_amount_gbp     FLOAT,
    verified_payment_amount_gbp  FLOAT,
    pending_payment_amount_gbp   FLOAT,
    failed_payment_amount_gbp    FLOAT,
    confirmed_payment_amount_gbp FLOAT,
    total_payment_amount         FLOAT,
    verified_payment_amount      FLOAT,
    pending_payment_amount       FLOAT,
    failed_payment_amount        FLOAT,
    confirmed_payment_amount     FLOAT
);


SELECT v.id                                                                        AS voucher_id,
       v.version                                                                   AS voucher_version,
       v.code                                                                      AS voucher_code,
       v.unique_transaction_reference                                              AS order_code,
       v.credit_id                                                                 AS credit_id,
       c.credit_status,
       v.gifter_id                                                                 AS gifter_id,
       v.giftee_id                                                                 AS giftee_id,
       v.payment_id                                                                AS payment_id,
       v.date_created                                                              AS voucher_date_created,
       tc.expires_on                                                               AS voucher_expires_on,
       v.manual_expiry_date                                                        AS voucher_manual_expiry_date,
       v.last_updated                                                              AS voucher_last_updated,
       v.redeemed_date                                                             AS voucher_redeemed_date,
       v.type                                                                      AS voucher_type,
       v.status                                                                    AS voucher_status,
       p.currency                                                                  AS voucher_currency, -- the payment currency is considered the currency of the voucher

       gtr.shiro_user_id                                                           AS gifter_shiro_user_id,
       gtr.signup_tstamp::DATE                                                     AS gifter_join_date,
       t.currency                                                                  AS gifter_currency,
       gtr.current_affiliate_territory                                             AS gifter_territory,
       gte.shiro_user_id                                                           AS giftee_shiro_user_id,
       gte.signup_tstamp::DATE                                                     AS giftee_join_date,
       gte.current_affiliate_territory                                             AS giftee_territory,
       CASE WHEN gc.delivery_address_postcode IS NOT NULL THEN TRUE ELSE FALSE END AS is_physical_voucher,

       gc.delivery_charge                                                          AS delivery_charge,

       p.type                                                                      AS payment_type,
       p.status                                                                    AS payment_status,

       p.total_amount                                                              AS total_payment_amount,
       p.verified_amount                                                           AS verified_payment_amount,
       p.pending_amount                                                            AS pending_payment_amount,
       p.failed_amount                                                             AS failed_payment_amount,
       p.confirmed_amount                                                          AS confirmed_payment_amount

FROM data_vault_mvp_dev_robin.cms_mysql_snapshots.voucher_snapshot v
         LEFT JOIN data_vault_mvp_dev_robin.cms_mysql_snapshots.gift_card_snapshot gc
                   ON v.id = gc.voucher_id -- Get gift card information (for address/virtual card check)
         LEFT JOIN data_vault_mvp_dev_robin.dwh.se_voucher__step01_model_payment p ON v.payment_id = p.payment_id
         LEFT JOIN data_vault_mvp_dev_robin.dwh.se_credit c ON v.credit_id = c.credit_id
         LEFT JOIN data_vault_mvp_dev_robin.cms_mysql_snapshots.time_limited_credit_snapshot tc ON c.credit_id = tc.id
         LEFT JOIN data_vault_mvp_dev_robin.dwh.user_attributes gtr ON gtr.shiro_user_id = v.gifter_id
         LEFT JOIN data_vault_mvp_dev_robin.cms_mysql_snapshots.territory_snapshot t ON gtr.current_affiliate_territory_id = t.id
         LEFT JOIN data_vault_mvp_dev_robin.dwh.user_attributes gte ON v.giftee_id = gte.shiro_user_id;


SELECT * FROM data_vault_mvp_dev_robin.dwh.se_voucher;

SELECT COUNT(*) FROM data_vault_mvp.cms_mysql_snapshots.tag_links_snapshot tls;

self_describing_task --include 'se/data/finance/se_voucher.py'  --method 'run' --start '2021-01-18 00:00:00' --end '2021-01-18 00:00:00';

airflow backfill --start_date '2021-01-19 07:00:00' --end_date '2021-01-19 07:00:00' --task_regex '.*' se_data_object_creation__daily_at_07h00

