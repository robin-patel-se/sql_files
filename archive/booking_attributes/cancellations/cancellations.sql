SELECT bcs.dataset_name,
       bcs.dataset_source,
       bcs.schedule_interval,
       bcs.schedule_tstamp,
       bcs.run_tstamp,
       bcs.loaded_at,
       bcs.filename,
       bcs.file_row_number,
       bcs.id,
       bcs.version,
       bcs.booking_id,
       bcs.date_created,
       bcs.last_updated,
       bcs.fault,
       bcs.reason,
       bcs.booking_fee,
       bcs.cc_fee,
       bcs.hotel_good_will,
       bcs.refund_channel,
       bcs.refund_type,
       bcs.se_good_will,
       bcs.who_pays,
       bcs.reservation_id,
       bcs.cancel_with_provider,
       bcs.extract_metadata
FROM data_vault_mvp.cms_mysql_snapshots.booking_cancellation_snapshot bcs;


SELECT get_ddl('table', 'data_vault_mvp.cms_mysql_snapshots.booking_cancellation_snapshot');

CREATE OR REPLACE TABLE booking_cancellation_snapshot
(

    id                   NUMBER,
    version              NUMBER,
    booking_id           NUMBER,
    date_created         TIMESTAMP,
    last_updated         TIMESTAMP,
    fault                VARCHAR,
    reason               VARCHAR,
    booking_fee          FLOAT,
    cc_fee               FLOAT,
    hotel_good_will      FLOAT,
    refund_channel       VARCHAR,
    refund_type          VARCHAR,
    se_good_will         FLOAT,
    who_pays             VARCHAR,
    reservation_id       NUMBER,
    cancel_with_provider BOOLEAN
);

self_describing_task --include 'hygiene/cms_mysql/booking_cancellation.py'  --method 'run' --start '2020-02-25 00:00:00' --end '2020-02-25 00:00:00'
airflow backfill --start_date '2020-02-25 01:00:00' --end_date '2020-02-25 01:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__booking_cancellation__daily_at_01h00
airflow backfill --start_date '2020-10-12 01:00:00' --end_date '2020-10-12 01:00:00' --task_regex '.*' -m hygiene_snapshots__cms_mysql__booking_cancellation__daily_at_01h00
airflow backfill --start_date '2020-10-13 03:00:00' --end_date '2020-10-13 03:00:00' --task_regex '.*' dwh__transactional__booking_cancellation__daily_at_03h00

SELECT MIN(loaded_at)
FROM raw_vault_mvp.cms_mysql.booking_cancellation bc; --2020-02-25 16:51:09.935855000
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.cms_mysql.booking_cancellation CLONE raw_vault_mvp.cms_mysql.booking_cancellation;

SELECT *
FROM raw_vault_mvp.cms_mysql.booking_cancellation bc;


self_describing_task --include 'hygiene_snapshots/cms_mysql/booking_cancellation.py'  --method 'run' --start '2020-02-25 00:00:00' --end '2020-02-25 00:00:00'

SELECT booking_id,
       count(*)
FROM hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking_cancellation
GROUP BY 1
HAVING COUNT(*) > 1
ORDER BY 2 DESC;
;

SELECT booking_id,
       count(*)
FROM hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking_cancellation
GROUP BY 1
HAVING COUNT(*) = 1
ORDER BY 2 DESC;
;

SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking_cancellation
WHERE booking_id IN ()
ORDER BY booking_id, last_updated;

SELECT *
FROM se.data_pii.se_user_attributes sua
WHERE sua.email = 'gianni.raftis@gmail.com';

SELECT *
FROM se.data.se_booking sb
         LEFT JOIN hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking_cancellation bc ON sb.booking_id = bc.booking_id
WHERE sb.shiro_user_id = 72868430
  AND sb.booking_status = 'COMPLETE'

SELECT bc.booking_id,
       bc.date_created,
       bc.last_updated,
       bc.fault,
       bc.reason,
       bc.booking_fee,
       bc.cc_fee,
       bc.hotel_good_will,
       bc.se_good_will,
       bc.refund_channel,
       bc.refund_type,
       bc.who_pays,
       bc.cancel_with_provider
FROM hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking_cancellation bc
WHERE booking_id IN (
    SELECT booking_id
    FROM hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking_cancellation
    GROUP BY 1
    HAVING COUNT(*) > 1

)
ORDER BY booking_id;

SELECT bc.booking_id,
       bc.date_created,
       bc.last_updated,
       bc.fault,
       bc.reason,
       bc.booking_fee,
       bc.cc_fee,
       bc.hotel_good_will,
       bc.se_good_will,
       bc.refund_channel,
       bc.refund_type,
       bc.who_pays,
       bc.cancel_with_provider
FROM hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking_cancellation bc
WHERE booking_id IN (

    SELECT bc.booking_id
    FROM hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking_cancellation bc
    WHERE booking_id IN (
        SELECT booking_id
        FROM hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking_cancellation
        GROUP BY 1
        HAVING COUNT(*) > 1

    )
      AND bc.booking_fee + bc.cc_fee + bc.hotel_good_will + bc.se_good_will = 0
);


--partition table to get list of full cancellations
--do an except for cancellations that don't include a full cancellation
--decide rule as to what we do with the cancellcations that only have partials (take last?first? sum?)


self_describing_task --include 'dv/dwh/transactional/se_booking_cancellation.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.dwh.booking_cancellation__step01__full_canx_bookings;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.booking_cancellation__step02__model_full_canx bc;


SELECT *
FROM data_vault_mvp_dev_robin.dwh.booking_cancellation__step04__model_partial_canx;



SELECT refund_type, count(*)
FROM data_vault_mvp_dev_robin.dwh.booking_cancellation
GROUP BY 1;

'booking': DBObjectRef(
            db_name=VAULTS['hygiene_snapshot_vault'],
            schema_name='cms_mysql',
            object_name='booking',
        ),
        'reservation': DBObjectRef(
            db_name=VAULTS['hygiene_snapshot_vault'],
            schema_name='cms_mysql',
            object_name='reservation',
        ),
        'booking_summary': DBObjectRef(
            db_name=VAULTS['hygiene_snapshot_vault'],
            schema_name='cms_mongodb',
            object_name='booking_summary',
        ),
        'amendment': DBObjectRef(
            db_name=VAULTS['dv'],
            schema_name='cms_mysql_snapshots',
            object_name='amendment_snapshot',
        ),
        'product_reservation': DBObjectRef(
            db_name=VAULTS['dv'],
            schema_name='cms_mysql_snapshots',
            object_name='product_reservation_snapshot',
        ),
        'constant_currency': DBObjectRef(
            db_name=VAULTS['hygiene_snapshot_vault'],
            schema_name='fpa_gsheets',
            object_name='constant_currency',
        ),
        'booking_cancellation': DBObjectRef(
            db_name=VAULTS['dv'],
            schema_name='dwh',
            object_name='booking_cancellation',
        ),



CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking CLONE hygiene_snapshot_vault_mvp.cms_mysql.booking;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.reservation CLONE hygiene_snapshot_vault_mvp.cms_mysql.reservation;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary CLONE hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.amendment_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.amendment_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.product_reservation_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.product_reservation_snapshot;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.fpa_gsheets.constant_currency CLONE hygiene_snapshot_vault_mvp.fpa_gsheets.constant_currency;

self_describing_task --include 'dv/dwh/transactional/se_booking.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_booking__step04__model_cancellations AS (
    SELECT
        -- (lineage) metadata for the current job
        '2020-04-12 03:00:00'                                                                                                                   AS schedule_tstamp,
        '2020-10-13 11:39:43'                                                                                                                   AS run_tstamp,
        'ScriptOperator__/usr/local/one-data-pipeline/biapp/task_catalogue/dv/dwh/transactional/se_booking.py__20200412T030000__daily_at_03h00' AS operation_id,
        CURRENT_TIMESTAMP()::TIMESTAMP                                                                                                          AS created_at,
        CURRENT_TIMESTAMP()::TIMESTAMP                                                                                                          AS updated_at,

        cf.*,

        bc.refund_type,
        --full refunds don't have a refund amount.
        IFF(bc.refund_type = 'FULL', cf.customer_total_price_cc,
            bc.total_refunded)                                                                                                                  AS total_refunded_cc,
        IFF(bc.refund_type = 'FULL', cf.customer_total_price_gbp,
            bc.total_refunded * cf.cc_rate_to_gbp)                                                                                              AS total_refunded_gbp,
        IFF(bc.refund_type = 'FULL', cf.customer_total_price_sc,
            bc.total_refunded * cf.cc_rate_to_sc)                                                                                               AS total_refunded_sc,
        bc.cancellation_date,
        bc.cancellation_tstamp,
        bc.fault                                                                                                                                AS cancellation_fault,
        bc.reason                                                                                                                               AS cancellation_reason,
        bc.refund_channel                                                                                                                       AS cancellation_refund_channel
    FROM data_vault_mvp_dev_robin.dwh.se_booking__step03__collate_fields cf
             LEFT JOIN data_vault_mvp_dev_robin.dwh.booking_cancellation bc ON cf.booking_id = bc.booking_id
)
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_booking sb
WHERE refund_type = 'PARTIAL';

SELECT *
FROM hygiene_snapshot_vault_mvp.sfsc.rebooking_request_cases rrc
INNER JOIN data_vault_mvp.dwh.se_booking sb ON rrc.transaction_id = sb.transaction_id
WHERE LOWER(VIEW) LIKE '%rebook%'
  AND LOWER(rrc.status) NOT IN ('solved', 'closed', 'resolved')
;


self_describing_task --include 'se/data/dwh/se_booking.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT COUNT(*) FROM data_vault_mvp.dwh.se_booking sb;
SELECT COUNT(*) FROM data_vault_mvp_dev_robin.dwh.se_booking sb;


SELECT sb.booking_id,
       sb.refund_type,
       sb.customer_total_price_cc,
       sb.total_refunded_cc,
       sb.customer_total_price_gbp,
       sb.total_refunded_gbp,
       sb.customer_total_price_sc,
       sb.total_refunded_sc,
       sb.cancellation_date,
       sb.cancellation_tstamp,
       sb.cancellation_fault,
       sb.cancellation_reason,
       sb.cancellation_refund_channel
FROM data_vault_mvp_dev_robin.dwh.se_booking sb
WHERE sb.refund_type = 'PARTIAL';

DROP TABLE hygiene_vault_mvp_dev_robin.cms_mysql.booking_cancellation;
DROP TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking_cancellation;

SELECT sb.customer_total_price_cc,
       sb.customer_total_price_gbp,
       1/sb.cc_rate_to_gbp as gbp_rate_to_cc,
       sb.cc_rate_to_gbp,
       sb.customer_total_price_gbp * gbp_rate_to_cc
       FROM se.data.se_booking sb
WHERE sb.booking_status = 'COMPLETE'
AND sb.cc_rate_to_gbp != 1