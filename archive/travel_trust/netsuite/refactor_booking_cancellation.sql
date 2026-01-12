SELECT refund_type, COUNT(*)
FROM hygiene_snapshot_vault_mvp.cms_mysql.booking_cancellation bc
GROUP BY 1;

SELECT *
FROM hygiene_snapshot_vault_mvp.cms_mysql.booking_cancellation bc
    QUALIFY COUNT(*) OVER (PARTITION BY bc.booking_id) > 1
ORDER BY booking_id, date_created;

WITH list_full_canx AS (
    SELECT *
    FROM hygiene_snapshot_vault_mvp.cms_mysql.booking_cancellation
    WHERE refund_type IN ('CANCELLATION', 'FULL')
      AND booking_cancellation.date_created >= CURRENT_DATE - 30
        QUALIFY COUNT(*) OVER (PARTITION BY booking_id) > 1
    ORDER BY booking_id, date_created
)
SELECT list_full_canx.booking_id,
       COUNT(DISTINCT refund_type) AS refund_types
FROM list_full_canx
GROUP BY 1
HAVING refund_types > 1;

SELECT *
FROM hygiene_snapshot_vault_mvp.cms_mysql.booking_cancellation bc
WHERE bc.booking_id = 'A3138702';


------------------------------------------------------------------------------------------------------------------------
WITH flags AS (
    SELECT bc.booking_id,
           bc.refund_type,
           bc.refund_type = 'FULL'         AS full_canx,
           bc.refund_type = 'CANCELLATION' AS cancellation_canx,
           bc.refund_type = 'PARTIAL'      AS partial_canx,
           bc.refund_type = 'WP_REFUND'    AS wp_partial_canx
    FROM hygiene_snapshot_vault_mvp.cms_mysql.booking_cancellation bc
)
SELECT f.booking_id,
       CASE
           WHEN MAX(f.full_canx) THEN 'FULL'
           WHEN MAX(f.cancellation_canx) THEN 'CANCELLATION'
           ELSE 'PARTIAL'
           END AS refund_type
FROM flags f
GROUP BY 1;

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking_cancellation CLONE hygiene_snapshot_vault_mvp.cms_mysql.booking_cancellation;

self_describing_task --include 'dv/dwh/transactional/se_booking_cancellation.py'  --method 'run' --start '2021-08-16 00:00:00' --end '2021-08-16 00:00:00'

SELECT dev.booking_id,
       dev.refund_type
FROM data_vault_mvp_dev_robin.dwh.booking_cancellation dev
EXCEPT
SELECT prod.booking_id,
       prod.refund_type
FROM data_vault_mvp.dwh.booking_cancellation prod;

self_describing_task --include 'dv/dwh/transactional/se_booking.py'  --method 'run' --start '2021-08-17 00:00:00' --end '2021-08-17 00:00:00'