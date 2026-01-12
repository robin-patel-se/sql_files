SELECT *
FROM hygiene_snapshot_vault_mvp.cms_mysql.booking b;

SELECT *
FROM hygiene_snapshot_vault_mvp.cms_mysql.reservation r;

SELECT bs.booking_id, count(*)
FROM hygiene_vault_mvp.cms_mongodb.booking_summary bs
GROUP BY 1
HAVING count(*) > 1

SELECT *
FROM hygiene_vault_mvp.cms_mongodb.booking_summary bs
WHERE bs.booking_id = '53552529'
ORDER BY last_updated;
SELECT *
FROM hygiene_vault_mvp.cms_mongodb.booking_summary bs
WHERE bs.booking_id = '52894147'
ORDER BY last_updated;

SELECT *
FROM se.data.se_sale_attributes ssa;
SELECT *
FROM se.data.tb_offer t;

SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
         self_describing_task --include 'se/data_pii/scv_touch_basic_attributes.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT *
FROM se.data.scv_touch_basic_attributes stba
         INNER JOIN se.data.scv_touch_attribution sta
                    ON stba.touch_id = sta.touch_id AND sta.attribution_model = 'last paid' --'last non direct'
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id;

SELECT *
FROM se.data_pii.scv_touch_basic_attributes;

------------------------------------------------------------------------------------------------------------------------

SELECT DISTINCT
       te.additional_info         AS 'Booking id',
       s.id                       AS 'Sale id',
       te.type                    AS 'Email type',
       te.to_address              AS 'Recipient',
       te.status                  AS 'Status',
       te.date_created            AS 'Date created',
       te.date_queued_at_provider AS 'When queued at provider',
       te.date_delivered          AS 'When sent to recipient',
       te.retries                 AS 'Retries number',
       te.fail_reason             AS 'Fail reason'
FROM triggered_email te
         LEFT JOIN booking_allocations ba ON te.additional_info = ba.booking_allocations_id
         LEFT JOIN allocation al ON ba.allocation_id = al.id
         LEFT JOIN offer o ON o.id = al.offer_id
         LEFT JOIN sale s ON o.sale_id = s.id
WHERE te.date_created > :startDate
  AND te.date_created <= :endDate

SELECT * FROM data_vault_mvp.cms_mysql_snapshots.booking_cancellation_snapshot bcs;