SELECT sb.booking_id,
       sb.booking_lead_time_days,
       sb.booking_completed_date,
       sb.check_in_date,
       sb.check_out_date

FROM data_vault_mvp.dwh.se_booking sb
WHERE sb.booking_lead_time_days < 0;


SELECT sb.booking_id,
       sb.booking_lead_time_days,
       sb.booking_completed_date,
       sb.check_in_date,
       sb.check_out_date

FROM data_vault_mvp.dwh.se_booking sb
WHERE sb.booking_lead_time_days < 0
  AND sb.booking_completed_date > sb.check_in_date;


SELECT *
FROM hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary bs
WHERE bs.booking_id IN ('A2689966',
                        'A2140768',
                        '53553246',
                        '54019627',
                        '51892107',
                        '52294862',
                        '38974699',
                        'A774516',
                        '51586032',
                        'A457257',
                        'A330111',
                        '50943929',
                        '50943039',
                        'A487536',
                        '50161652',
                        '34455865',
                        '23605823',
                        '30295384'
    );


SELECT *
FROM se.data.se_booking sb
WHERE sb.booking_id IN
      ('A2689966',
       'A2140768',
       '53553246',
       '54019627',
       '51892107',
       '52294862',
       '38974699',
       'A774516',
       '51586032',
       'A457257',
       'A330111',
       '50943929',
       '50943039',
       'A487536',
       '50161652',
       '34455865',
       '23605823',
       '30295384'
          );

-- not rebookings
-- not affiliate
-- not partial cancellation or any cancellation
-- not product type
-- not platform
-- not new/old data model
-- not sale type
-- not cs agent booking

SELECT *
FROM raw_vault_mvp.cms_mongodb.booking_summary bs
WHERE bs.record:transaction_id = 'A15158-15691-2689966';

SELECT *
FROM hygiene_snapshot_vault_mvp.cms_mysql.reservation r
WHERE id = 2689966;



SELECT DATE_TRUNC(YEAR, sb.booking_created_date),
       COUNT(*)
FROM data_vault_mvp.dwh.se_booking sb
WHERE sb.booking_lead_time_days < 0
GROUP BY 1;


SELECT DATE_TRUNC(YEAR, sb.check_in_date),
       COUNT(*)
FROM data_vault_mvp.dwh.se_booking sb
WHERE sb.booking_lead_time_days < 0
GROUP BY 1

self_describing_task --include 'dv/dwh/transactional/tb_booking.py'  --method 'run' --start '2021-02-18 00:00:00' --end '2021-02-18 00:00:00'

SELECT * FROM data_vault_mvp_dev_robin.dwh.tb_booking tb WHERE tb.is_staff_booking = FALSE;

SELECT * FROM se.data.se_credit sc WHERE sc.redeemed_se_booking_id LIKE 'TB-%'

self_describing_task --include 'se/data_pii/dwh/se_booking.py'  --method 'run' --start '2021-02-18 00:00:00' --end '2021-02-18 00:00:00'
self_describing_task --include 'se/data/dwh/se_booking.py'  --method 'run' --start '2021-02-18 00:00:00' --end '2021-02-18 00:00:00'