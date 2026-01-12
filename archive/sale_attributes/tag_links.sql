SELECT COUNT(*)
FROM raw_vault_mvp.cms_mysql.tag_links tl;

SELECT COUNT(*)
FROM data_vault_mvp.cms_mysql_snapshots.tag_links_snapshot tls;

SELECT GET_DDL('table', 'raw_vault_mvp.cms_mysql.tag_links');


CREATE OR REPLACE TABLE tag_links
(
    dataset_name      VARCHAR,
    dataset_source    VARCHAR,
    schedule_interval VARCHAR,
    schedule_tstamp   TIMESTAMP,
    run_tstamp        TIMESTAMP,
    loaded_at         TIMESTAMP,
    filename          VARCHAR,
    file_row_number   NUMBER,
    id                NUMBER,
    version           NUMBER,
    tag_id            NUMBER,
    tag_ref           NUMBER,
    type              VARCHAR,
    extract_metadata  VARIANT
);

SELECT type, COUNT(*)
FROM raw_vault_mvp.cms_mysql.tag_links tl
GROUP BY 1;

CREATE OR REPLACE TRANSIENT TABLE raw_vault_mvp_dev_robin.cms_mysql.tag_links CLONE raw_vault_mvp.cms_mysql.tag_links;

SELECT MIN(loaded_at)
FROM raw_vault_mvp.cms_mysql.booking_note bn tl;

self_describing_task --include 'staging/hygiene/cms_mysql/tag_links.py'  --method 'run' --start '2021-01-18 00:00:00' --end '2021-01-18 00:00:00'

self_describing_task --include 'staging/hygiene_snapshots/cms_mysql/tag_links.py'  --method 'run' --start '2021-01-18 00:00:00' --end '2021-01-18 00:00:00'

self_describing_task --include 'dv/cms_snapshots/dv_snapshots_create_views.py'  --method 'run' --start '2021-01-18 00:00:00' --end '2021-01-18 00:00:00'

DROP TABLE data_vault_mvp.cms_mysql_snapshots.booking_note_snapshot;
DROP TABLE data_vault_mvp.cms_mysql_snapshots.tag_links_snapshot;

CREATE OR REPLACE TRANSIENT TABLE raw_vault_mvp_dev_robin.cms_mysql.hotel_rate_plan CLONE raw_vault_mvp.cms_mysql.hotel_rate_plan;

SELECT *
FROM se.data.se_sale_attributes ssa;

airflow backfill --start_date '2021-01-18 01:00:00' --end_date '2021-01-18 01:00:00' --task_regex '.*' -m hygiene_snapshots__cms_mysql__booking_note__daily_at_01h00

airflow backfill --start_date '2021-01-05 01:00:00' --end_date '2021-01-05 01:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__booking_note__daily_at_01h00

airflow backfill --start_date '2021-01-18 07:00:00' --end_date '2021-01-18 07:00:00' --task_regex '.*' dv_snapshots_create_views__daily_at_07h00


SELECT *
FROM se.data.se_sale_tags sst;


CREATE OR REPLACE TABLE scratch.robinpatel.testincrement
(
    id       INT PRIMARY KEY NOT NULL AUTOINCREMENT,
    testname varchar
);

INSERT INTO scratch.robinpatel.testincrement (testname)
VALUES ('testing2');

SELECT *
FROM scratch.robinpatel.testincrement t;


SELECT ds.sale_active, COUNT(*)
FROM se.data.dim_sale ds
GROUP BY 1;

SELECT booking_status, COUNT(*)
FROM se.data.se_booking sb
GROUP BY 1;



SELECT sf.sfid, sf.transactionid, bk.transaction_id, bk.currency
FROM collab.salesforce_pii.transaction_data sf
         LEFT JOIN se.data.se_booking bk ON sf.transactionid = bk.transaction_id
WHERE bk.transaction_id IS NULL;


SELECT sf.sfid,
       sf.transactionid,
       bk.transaction_id,
       bk.currency
FROM collab.salesforce_pii.transaction_data sf
         INNER JOIN se.data.se_booking bk ON sf.transactionid = bk.transaction_id

UNION

SELECT sf.sfid,
       sf.transactionid,
       tb.reference_id,
       tb.sold_price_currency
FROM collab.salesforce_pii.transaction_data sf
         INNER JOIN se.data.tb_booking tb ON sf.transactionid = tb.reference_id;


SELECT * FROM se.data.tb_booking tb

;

SELECT COUNT(*) FROM se.data.scv_touch_basic_attributes stba; --522,240,610


--inserted 1,631,387
--updated 120,925,753
