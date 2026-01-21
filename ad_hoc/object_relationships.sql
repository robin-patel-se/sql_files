CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.object_relationships
(
    relationship_id          INT AUTOINCREMENT NOT NULL,
    shiro_user_id            INT,
    relationship_date        TIMESTAMP,
    parent_id                VARCHAR,
    parent_type              VARCHAR,
    parent_status            VARCHAR,
    child_id                 VARCHAR,
    child_type               VARCHAR,
    child_status             VARCHAR,
    relationship_category    VARCHAR,
    relationship_subcategory VARCHAR
) CLUSTER BY (shiro_user_id);

DROP TABLE scratch.robinpatel.object_relationships;

--credits awarded from bookings
INSERT INTO scratch.robinpatel.object_relationships
(shiro_user_id,
 relationship_date,
 parent_id,
 parent_type,
 parent_status,
 child_id,
 child_type,
 child_status,
 relationship_category,
 relationship_subcategory)
SELECT sc.shiro_user_id,
       sc.credit_date_created    AS relationship_date,
       sc.original_se_booking_id AS parent_id,
       'booking'                 AS parent_type,
       fb.booking_status         AS parent_status,
       sc.credit_id              AS child_id,
       'credit'                  AS child_type,
       sc.credit_status          AS child_status,
       'booking to credit'       AS relationship_category,
       CASE
           WHEN sc.credit_type IN ('REFUND',
                                   'CANCELLATION_CREDIT',
                                   'EXTRA_REFUND_CREDIT'
               ) THEN 'REFUND_CREDIT'
           WHEN sc.credit_type = 'HOLD' THEN 'HOLD_CREDIT'
           END                   AS relationship_subcategory -- TODO discovery on types of credits awarded from bookings (look into compensation)
FROM se.data.se_credit sc
         INNER JOIN se.data.fact_booking fb ON sc.original_se_booking_id = fb.booking_id
WHERE sc.shiro_user_id IS NOT NULL
  AND sc.shiro_user_id = 50893148 --TODO REMOVE, TESTING ONLY
;

SELECT DISTINCT sc.credit_type
FROM se.data.se_credit sc
WHERE sc.original_se_booking_id IS NOT NULL;

--credits not awarded from bookings, needed for anchor points
INSERT INTO scratch.robinpatel.object_relationships
(shiro_user_id,
 relationship_date,
 parent_id,
 parent_type,
 parent_status,
 child_id,
 child_type,
 child_status,
 relationship_category,
 relationship_subcategory)
SELECT sc.shiro_user_id,
       sc.credit_date_created AS relationship_date,
       NULL                   AS parent_id,
       NULL                   AS parent_type,
       NULL                   AS parent_status,
       sc.credit_id           AS child_id,
       'credit'               AS child_type,
       sc.credit_status       AS child_status,
       'awarded credit'       AS relationship_category,
       'AWARDED_CREDIT'       AS relationship_subcategory
FROM se.data.se_credit sc
         INNER JOIN se.data.fact_booking fb ON sc.original_se_booking_id = fb.booking_id
WHERE sc.shiro_user_id IS NOT NULL
  AND sc.shiro_user_id = 50893148 --TODO REMOVE, TESTING ONLY
  AND sc.original_se_booking_id IS NULL --used as anchor
;


--credits used on bookings
INSERT INTO scratch.robinpatel.object_relationships
(shiro_user_id,
 relationship_date,
 parent_id,
 parent_type,
 parent_status,
 child_id,
 child_type,
 child_status,
 relationship_category,
 relationship_subcategory)
SELECT sc.shiro_user_id,
       fb.booking_completed_date AS relationship_date,
       sc.credit_id              AS parent_id,
       'credit'                  AS parent_type,
       sc.credit_status          AS parent_status,
       sc.redeemed_se_booking_id AS child_id,
       'booking'                 AS child_type,
       fb.booking_status         AS child_status,
       'credit to booking'       AS relationship_category,
       'CREDIT_REDEMPTION'       AS relationship_subcategory
FROM se.data.se_credit sc
         INNER JOIN se.data.fact_booking fb ON sc.redeemed_se_booking_id = fb.booking_id
WHERE sc.shiro_user_id IS NOT NULL
  AND sc.shiro_user_id = 50893148 --TODO REMOVE, TESTING ONLY
;

--bookings that aren't associated to any credits, used as an anchor point
INSERT INTO scratch.robinpatel.object_relationships
(shiro_user_id,
 relationship_date,
 parent_id,
 parent_type,
 parent_status,
 child_id,
 child_type,
 child_status,
 relationship_category,
 relationship_subcategory)
SELECT DISTINCT
       fb.shiro_user_id,
       fb.booking_completed_date AS relationship_date,
       NULL                      AS parent_id,
       NULL                      AS parent_type,
       NULL                      AS parent_status,
       fb.booking_id             AS child_id,
       'booking'                 AS child_type,
       fb.booking_status         AS child_status,
       'booking'                 AS relationship_category,
       'ORIGINAL_BOOKING'        AS relationship_subcategory
FROM se.data.fact_booking fb
         LEFT JOIN se.data.se_credit sc ON fb.booking_id = sc.redeemed_se_booking_id
WHERE sc.redeemed_se_booking_id IS NULL --used as achor point
  AND fb.booking_status_type IN ('live', 'cancelled')
  AND sc.shiro_user_id = 50893148 --TODO REMOVE, TESTING ONLY
;


--voucher to credit
INSERT INTO scratch.robinpatel.object_relationships
(shiro_user_id,
 relationship_date,
 parent_id,
 parent_type,
 parent_status,
 child_id,
 child_type,
 child_status,
 relationship_category,
 relationship_subcategory)
SELECT sv.giftee_shiro_user_id,
       sc.credit_date_created AS relationship_date,
       sv.voucher_id          AS parent_id,
       'voucher'              AS parent_type,
       sv.voucher_status      AS parent_status,
       sc.credit_id           AS child_id,
       'credit'               AS child_type,
       sc.credit_status       AS child_status,
       'voucher to credit'    AS relationship_category,
       'VOUCHER_CONVERSION'   AS relationship_subcategory
FROM se.data.se_voucher sv
         INNER JOIN se.data.se_credit sc ON sv.credit_id = sc.credit_id
WHERE sv.giftee_shiro_user_id IS NOT NULL
  AND sv.giftee_shiro_user_id = 50893148 --TODO REMOVE, TESTING ONLY
;

--voucher to credit, needed for anchor points
INSERT INTO scratch.robinpatel.object_relationships
(shiro_user_id,
 relationship_date,
 parent_id,
 parent_type,
 parent_status,
 child_id,
 child_type,
 child_status,
 relationship_category,
 relationship_subcategory)
SELECT sv.giftee_shiro_user_id,
       sv.voucher_date_created AS relationship_date,
       NULL                    AS parent_id,
       NULL                    AS parent_type,
       NULL                    AS parent_status,
       sv.voucher_id           AS child_id,
       'voucher'               AS child_type,
       sc.credit_status        AS child_status,
       'voucher to giftee'     AS relationship_category,
       'VOUCHER_AWARDED'       AS relationship_subcategory
FROM se.data.se_voucher sv
         INNER JOIN se.data.se_credit sc ON sv.credit_id = sc.credit_id
WHERE sv.giftee_shiro_user_id IS NOT NULL
  AND sv.giftee_shiro_user_id = 50893148
--TODO REMOVE, TESTING ONLY
--no achor point filter because these are the start of the flow.
;

-- ADD gifter to giftee link


SELECT *
FROM scratch.robinpatel.object_relationships;


-- WITH RECURSIVE cte_name (X, Y) AS
-- (
--   SELECT related_to_X, related_to_Y FROM table1
--   UNION ALL
--   SELECT also_related_to_X, also_related_to_Y
--     FROM table1 JOIN cte_name ON <join_condition>
-- )
-- SELECT ... FROM ...

USE WAREHOUSE pipe_xlarge;

WITH RECURSIVE cte_name (parent_id, child_id) AS (
    SELECT obr1.parent_id,
           obr1.child_id
    FROM scratch.robinpatel.object_relationships obr1
    WHERE obr1.parent_id IS NULL
    UNION ALL
    SELECT obr2.parent_id,
           obr2.child_id
    FROM scratch.robinpatel.object_relationships obr2
             INNER JOIN cte_name ON obr2.parent_id = cte_name.child_id
)
SELECT *
FROM cte_name;


SELECT shiro_user_id, COUNT(*)
FROM scratch.robinpatel.object_relationships
GROUP BY object_relationships.shiro_user_id
ORDER BY 2 DESC;


SELECT *
FROM scratch.robinpatel.object_relationships;


------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE scratch.robinpatel.employees
(
    title       varchar,
    employee_id integer,
    manager_id  integer
);

INSERT INTO scratch.robinpatel.employees (title, employee_id, manager_id)
VALUES ('President', 1, NULL), -- The President has no manager.
       ('Vice President Engineering', 10, 1),
       ('Programmer', 100, 10),
       ('QA Engineer', 101, 10),
       ('Vice President HR', 20, 1),
       ('Health Insurance Analyst', 200, 20);

--standard
SELECT *
FROM scratch.robinpatel.employees;

--recursive
SELECT employee_id, manager_id, title
FROM scratch.robinpatel.employees
START WITH title = 'President'
    CONNECT BY
    manager_id = prior employee_id
    ORDER BY employee_id;

------------------------------------------------------------------------------------------------------------------------

-- https://docs.snowflake.com/en/sql-reference/constructs/connect-by.html

--commenting to avoid formatting breaking it
-- SELECT
--     obr1.shiro_user_id,
--     obr1.relationship_date,
--     obr1.parent_id,
--     obr1.parent_type,
--     obr1.child_id,
--     obr1.child_type,
--     obr1.relationship_category,
--     obr1.relationship_date,
--     sys_connect_by_path(obr1.relationship_category, ' -> ') path,
--     CONNECT_BY_ROOT relationship_category as root_category,
--     CONNECT_BY_ROOT child_type as root_type,
--     CONNECT_BY_ROOT child_id as root_id
--
-- FROM scratch.robinpatel.object_relationships obr1
-- START WITH parent_id IS NULL
--     CONNECT BY
--     parent_id = prior child_id
--     AND shiro_user_id = shiro_user_id;


--code doesn't work in datagrip, have to paste into snowflake ui
SELECT obr1.shiro_user_id,
       obr1.relationship_date,
       obr1.parent_id,
       obr1.parent_type,
       obr1.child_id,
       obr1.child_type,
       obr1.relationship_category,
       obr1.relationship_date,
       sys_connect_by_path(obr1.relationship_category, ' -> ') AS path,
       connect_by_root                                         AS relationship_category AS root_category, connect_by_root AS child_type AS root_type, connect_by_root AS child_id AS root_id

FROM data_vault_mvp_dev_robin.dwh.object_relationships obr1
START WITH parent_id IS NULL
    CONNECT BY
    parent_id = prior child_id
    AND shiro_user_id = shiro_user_id;


SELECT target_account_list__c, *
FROM raw_vault_mvp.sfsc.account__m_z
WHERE id = '001w000001DVHXQAA5';
SELECT target_account_list__c, *
FROM hygiene_vault_mvp.sfsc.account
WHERE id = '001w000001DVHXQAA5';
SELECT target_account_list__c, *
FROM hygiene_snapshot_vault_mvp.sfsc.account
WHERE id = '001w000001DVHXQAA5';


SELECT *
FROM snowplow.atomic.events e
WHERE e.etl_tstamp::DATE = CURRENT_DATE
ORDER BY etl_tstamp DESC airflow backfill --start_date '2021-01-31 03:00:00' --end_date '2021-01-31 03:00:00' --task_regex '.*' dwh__salesforce__sale_opportunity__daily_at_03h00

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_credit CLONE data_vault_mvp.dwh.se_credit;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_voucher CLONE data_vault_mvp.dwh.se_voucher;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_booking CLONE data_vault_mvp.dwh.se_booking;

self_describing_task --include 'dv/dwh/transactional/object_relationship.py'  --method 'run' --start '2021-02-01 00:00:00' --end '2021-02-01 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.dwh.object_relationships__step07__union_tables;

SELECT *
FROM raw_vault_mvp.cms_mysql.booking
WHERE id = 51400211;

SELECT booking_status,
       fb.tech_platform,
       COUNT(*)
FROM se.data.fact_booking fb
GROUP BY 1, 2;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.object_relationships
WHERE object_relationships.relationship_date IS NULL;

-- 5m before deletion,
-- 683K from 2020
-- 019a26e0-32b4-ff3d-0000-02ddb293879a query id of recursive on 683K
DELETE
FROM data_vault_mvp_dev_robin.dwh.object_relationships o
WHERE o.relationship_date < '2020-01-01';

self_describing_task --include 'dv/dwh/transactional/object_relationship.py'  --method 'run' --start '2021-02-07 00:00:00' --end '2021-02-07 00:00:00'



WITH recursive_relationships (indent, relationship_date, parent_id, parent_type, child_id, child_type, relationship_category) AS (
    SELECT '' AS indent,
           obr1.relationship_date,
           obr1.parent_id,
           obr1.parent_type,
           obr1.child_id,
           obr1.child_type,
           obr1.relationship_category
    FROM scratch.robinpatel.object_relationships obr1
    WHERE obr1.parent_id IS NULL

    UNION ALL

    SELECT obr2.indent || '--- ',
           obr2.relationship_date,
           obr2.parent_id,
           obr2.parent_type,
           obr2.child_id,
           obr2.child_type,
           obr2.relationship_category
    FROM scratch.robinpatel.object_relationships obr2
             JOIN scratch.robinpatel.object_relationships obr3 ON obr2.parent_id = obr3.child_id
)
SELECT *
FROM recursive_relationships
;

SELECT *
FROM data_vault_mvp.dwh.object_relationships o
WHERE o.shiro_user_id = 50893148;

CREATE OR REPLACE TABLE scratch.robinpatel.object_relationships__full CLONE data_vault_mvp.dwh.object_relationships;

SELECT msbl.booking_id,
       msbl.shiro_user_id,
       msbl.transaction_id,
       msbl.credits_used,
       msbl.cr_credit_active,
       msbl.cr_credit_used,
       msbl.bk_cnx_reason
FROM se.data.master_se_booking_list msbl
WHERE msbl.booking_id = '52894147';

SELECT *
FROM se.data.se_credit sc
WHERE sc.shiro_user_id = 29962297;


--mihaela request
-- WITH input_data AS (
-- SELECT *
--     FROM data_vault_mvp.dwh.object_relationships orr
--     WHERE orr.shiro_user_id = 47231968
--   )
--
-- SELECT
--     obr1.shiro_user_id,
--     obr1.relationship_date,
--     obr1.parent_id,
--     obr1.parent_type,
--     obr1.child_id,
--     obr1.child_type,
--     obr1.relationship_category,
--     sys_connect_by_path(obr1.relationship_category, ' -> ') path,
--     CONNECT_BY_ROOT relationship_category as root_category,
--     CONNECT_BY_ROOT child_type as root_type,
--     CONNECT_BY_ROOT child_id as root_id
--
-- FROM input_data obr1
-- START WITH parent_id IS NULL
--     CONNECT BY
--     parent_id = prior child_id
--     AND shiro_user_id = shiro_user_id;
-- ;

--
-- WITH input_data AS (
-- SELECT *
--     FROM data_vault_mvp.dwh.object_relationships orr
--     WHERE orr.shiro_user_id = 47231968
--   )
--
-- SELECT
--     obr1.shiro_user_id,
--     obr1.relationship_date,
--     obr1.parent_id,
--     obr1.parent_type,
--     obr1.child_id,
--     obr1.child_type,
--     obr1.relationship_category,
--     sys_connect_by_path(obr1.relationship_category, ' -> ') path,
--     CONNECT_BY_ROOT relationship_category as root_category,
--     CONNECT_BY_ROOT child_type as root_type,
--     CONNECT_BY_ROOT child_id as root_id
--
-- FROM input_data obr1
-- START WITH parent_id = 'NA'
--     CONNECT BY
--     parent_id = prior child_id
--     AND shiro_user_id = shiro_user_id;
-- ;

SELECT * FROM data_vault_mvp.dwh.object_relationships o